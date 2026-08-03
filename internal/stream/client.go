package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/google/uuid"
	"github.com/mit/fraudguard/internal/payment"
	"github.com/mit/fraudguard/internal/scorer"
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

// Worker reads shards and fans payments across a goroutine pool.
type Worker struct {
	client     *kinesis.Client
	streamName string
	name       string
	workers    int
	scorer     *scorer.Service
	scored     atomic.Int64
	errors     atomic.Int64
	latSumUs   atomic.Int64
}

func NewWorker(client *kinesis.Client, streamName, name string, workers int, s *scorer.Service) *Worker {
	if workers < 1 {
		workers = 8
	}
	return &Worker{
		client:     client,
		streamName: streamName,
		name:       name,
		workers:    workers,
		scorer:     s,
	}
}

func (w *Worker) Stats() (scored, errs int64, avgMs float64) {
	n := w.scored.Load()
	if n == 0 {
		return 0, w.errors.Load(), 0
	}
	return n, w.errors.Load(), float64(w.latSumUs.Load()) / float64(n) / 1000.0
}

func (w *Worker) Run(ctx context.Context) error {
	shards, err := w.listShards(ctx)
	if err != nil {
		return err
	}
	slog.Info("worker starting", "name", w.name, "shards", len(shards), "pool", w.workers)

	jobs := make(chan payment.Payment, w.workers*4)
	var wg sync.WaitGroup
	for i := 0; i < w.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				start := time.Now()
				if _, err := w.scorer.Score(ctx, p); err != nil {
					w.errors.Add(1)
					slog.Error("score failed", "txn_id", p.TxnID, "err", err)
					continue
				}
				w.scored.Add(1)
				w.latSumUs.Add(time.Since(start).Microseconds())
			}
		}()
	}

	var shardWG sync.WaitGroup
	for _, shardID := range shards {
		shardWG.Add(1)
		go func(shardID string) {
			defer shardWG.Done()
			w.pollShard(ctx, shardID, jobs)
		}(shardID)
	}

	shardWG.Wait()
	close(jobs)
	wg.Wait()
	return nil
}

func (w *Worker) listShards(ctx context.Context) ([]string, error) {
	out, err := w.client.ListShards(ctx, &kinesis.ListShardsInput{
		StreamName: aws.String(w.streamName),
	})
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(out.Shards))
	for _, s := range out.Shards {
		ids = append(ids, aws.ToString(s.ShardId))
	}
	return ids, nil
}

func (w *Worker) pollShard(ctx context.Context, shardID string, jobs chan<- payment.Payment) {
	iterOut, err := w.client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(w.streamName),
		ShardId:           aws.String(shardID),
		ShardIteratorType: types.ShardIteratorTypeLatest,
	})
	if err != nil {
		slog.Error("get shard iterator", "shard", shardID, "err", err)
		return
	}
	iterator := iterOut.ShardIterator

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if iterator == nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		recOut, err := w.client.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: iterator,
			Limit:         aws.Int32(100),
		})
		if err != nil {
			slog.Error("get records", "shard", shardID, "err", err)
			time.Sleep(time.Second)
			continue
		}
		for _, rec := range recOut.Records {
			var p payment.Payment
			if err := json.Unmarshal(rec.Data, &p); err != nil {
				w.errors.Add(1)
				continue
			}
			if p.TxnID == "" {
				p.TxnID = uuid.NewString()
			}
			if p.Timestamp.IsZero() {
				p.Timestamp = time.Now().UTC()
			}
			select {
			case jobs <- p:
			case <-ctx.Done():
				return
			}
		}
		iterator = recOut.NextShardIterator
		if len(recOut.Records) == 0 {
			time.Sleep(200 * time.Millisecond)
		}
	}
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
