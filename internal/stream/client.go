package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/mit/fraudguard/internal/payment"
)

type Config struct {
	Region     string
	StreamName string
	Endpoint   string
}

func NewClient(cfg Config) *kinesis.Client {
	opts := []func(*kinesis.Options){}
	var awsCfg aws.Config

	if cfg.Endpoint != "" {
		awsCfg = aws.Config{
			Region:      cfg.Region,
			Credentials: credentials.NewStaticCredentialsProvider("local", "local", ""),
		}
		opts = append(opts, func(o *kinesis.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	} else {
		loaded, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion(cfg.Region))
		if err != nil {
			slog.Error("load aws config", "err", err)
			awsCfg = aws.Config{Region: cfg.Region}
		} else {
			awsCfg = loaded
		}
	}
	return kinesis.NewFromConfig(awsCfg, opts...)
}

type Producer struct {
	client     *kinesis.Client
	streamName string
}

func NewProducer(client *kinesis.Client, streamName string) *Producer {
	return &Producer{client: client, streamName: streamName}
}

func (p *Producer) Put(ctx context.Context, pay payment.Payment) error {
	body, err := json.Marshal(pay)
	if err != nil {
		return err
	}
	_, err = p.client.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:   aws.String(p.streamName),
		Data:         body,
		PartitionKey: aws.String(pay.CardID),
	})
	return err
}

func EnsureStream(ctx context.Context, client *kinesis.Client, name string, shardCount int32) error {
	_, err := client.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{
		StreamName: aws.String(name),
	})
	if err == nil {
		return nil
	}
	_, err = client.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String(name),
		ShardCount: aws.Int32(shardCount),
	})
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		sum, err := client.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{
			StreamName: aws.String(name),
		})
		if err == nil && sum.StreamDescriptionSummary != nil &&
			sum.StreamDescriptionSummary.StreamStatus == types.StreamStatusActive {
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("stream %s not active in time", name)
}
