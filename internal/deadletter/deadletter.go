// Package deadletter keeps the Kinesis records the worker could not score: the
// raw bytes and why, so they can be inspected or replayed by hand instead of
// vanishing into a log line.
package deadletter

import (
	"context"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dn-cuong/fraudguard/internal/history"
)

// retention is how long a dead letter is kept before DynamoDB TTL drops it.
const retention = 14 * 24 * time.Hour

type Store struct {
	client *dynamodb.Client
	table  string
}

func New(client *dynamodb.Client, table string) *Store {
	return &Store{client: client, table: table}
}

func (s *Store) EnsureTable(ctx context.Context) error {
	if _, err := s.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(s.table)}); err == nil {
		return nil
	}
	_, err := history.CreateTable(ctx, s.client, &dynamodb.CreateTableInput{
		TableName: aws.String(s.table),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("shard_id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("seq"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("shard_id"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("seq"), KeyType: types.KeyTypeRange},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	return err
}

// Put stores one record. Keyed by shard and sequence number, so a replayed
// batch overwrites its own dead letter instead of adding another.
func (s *Store) Put(ctx context.Context, shardID, seq string, data []byte, cause error) error {
	now := time.Now().UTC()
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item: map[string]types.AttributeValue{
			"shard_id":   &types.AttributeValueMemberS{Value: shardID},
			"seq":        &types.AttributeValueMemberS{Value: seq},
			"data":       &types.AttributeValueMemberB{Value: data},
			"error":      &types.AttributeValueMemberS{Value: cause.Error()},
			"at":         &types.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
			"expires_at": &types.AttributeValueMemberN{Value: strconv.FormatInt(now.Add(retention).Unix(), 10)},
		},
	})
	return err
}
