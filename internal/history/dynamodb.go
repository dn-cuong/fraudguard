package history

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/mit/fraudguard/internal/payment"
)

type Config struct {
	Region          string
	TableName       string
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
}

// Store persists scored transactions in DynamoDB.
type Store struct {
	client    *dynamodb.Client
	tableName string
}

func New(cfg Config) *Store {
	awsCfg := aws.Config{Region: cfg.Region}
	opts := []func(*dynamodb.Options){}
	if cfg.Endpoint != "" {
		awsCfg.Credentials = credentials.NewStaticCredentialsProvider(
			firstNonEmpty(cfg.AccessKeyID, "local"),
			firstNonEmpty(cfg.SecretAccessKey, "local"),
			"",
		)
		opts = append(opts, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}
	return &Store{
		client:    dynamodb.NewFromConfig(awsCfg, opts...),
		tableName: cfg.TableName,
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func (s *Store) EnsureTable(ctx context.Context) error {
	_, err := s.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(s.tableName),
	})
	if err == nil {
		return nil
	}

	_, err = s.client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(s.tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("card_id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("txn_id"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("card_id"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("txn_id"), KeyType: types.KeyTypeRange},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	waiter := dynamodb.NewTableExistsWaiter(s.client)
	return waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(s.tableName),
	}, 30*time.Second)
}

func (s *Store) Put(ctx context.Context, rec payment.Record) error {
	item, err := attributevalue.MarshalMap(rec)
	if err != nil {
		return err
	}
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.tableName),
		Item:      item,
	})
	return err
}

func (s *Store) HasDispute(ctx context.Context, cardID string) (bool, error) {
	out, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(s.tableName),
		KeyConditionExpression: aws.String("card_id = :c"),
		FilterExpression:       aws.String("disputed = :d"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":c": &types.AttributeValueMemberS{Value: cardID},
			":d": &types.AttributeValueMemberBOOL{Value: true},
		},
		Limit: aws.Int32(1),
	})
	if err != nil {
		return false, err
	}
	return len(out.Items) > 0, nil
}
