package cloudwatchx

import (
	"context"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

// Publisher sends worker heartbeats to CloudWatch (for failover alarms).
type Publisher struct {
	client *cloudwatch.Client
	name   string
}

func New(ctx context.Context, region, workerName string) *Publisher {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		slog.Warn("cloudwatch config unavailable", "err", err)
		return &Publisher{name: workerName}
	}
	return &Publisher{
		client: cloudwatch.NewFromConfig(cfg),
		name:   workerName,
	}
}

func (p *Publisher) Beat(ctx context.Context) {
	if p == nil || p.client == nil {
		return
	}
	_, err := p.client.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace: aws.String("FraudGuard"),
		MetricData: []types.MetricDatum{{
			MetricName: aws.String("WorkerHeartbeat"),
			Dimensions: []types.Dimension{{
				Name:  aws.String("WorkerName"),
				Value: aws.String(p.name),
			}},
			Value: aws.Float64(1),
			Unit:  types.StandardUnitCount,
		}},
	})
	if err != nil {
		slog.Warn("cloudwatch put metric", "err", err)
	}
}
