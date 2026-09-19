// Package lease keeps shard ownership and checkpoints in DynamoDB, so workers
// can share a Kinesis stream without a fixed shard split: a shard belongs to
// whoever holds an unexpired lease, and a dead worker's lease just runs out.
//
// Expiry is compared against each worker's own clock, so keep the TTL well
// above any clock skew between hosts.
package lease

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ErrHeld means the lease is owned by someone else (or the shard is finished).
var ErrHeld = errors.New("lease held by another worker")

const workerPrefix = "worker#"

// Lease is one row: a shard's owner, expiry and checkpoint.
type Lease struct {
	ShardID    string `dynamodbav:"shard_id"`
	Owner      string `dynamodbav:"owner"`
	ExpiresMs  int64  `dynamodbav:"expires_ms"`
	Checkpoint string `dynamodbav:"checkpoint,omitempty"`
	Finished   bool   `dynamodbav:"finished"`
}

// Expired reports whether nobody holds the lease at time now.
func (l Lease) Expired(now time.Time) bool { return l.ExpiresMs < now.UnixMilli() }

// Store is the lease table.
type Store struct {
	client *dynamodb.Client
	table  string
	ttl    time.Duration
	now    func() time.Time
}

func New(client *dynamodb.Client, table string, ttl time.Duration) *Store {
	return &Store{client: client, table: table, ttl: ttl, now: time.Now}
}

func (s *Store) EnsureTable(ctx context.Context) error {
	if _, err := s.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(s.table)}); err == nil {
		return nil
	}
	_, err := s.client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(s.table),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("shard_id"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("shard_id"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		return fmt.Errorf("create lease table: %w", err)
	}
	return dynamodb.NewTableExistsWaiter(s.client).Wait(ctx,
		&dynamodb.DescribeTableInput{TableName: aws.String(s.table)}, 30*time.Second)
}

func (s *Store) expiry() types.AttributeValue {
	return &types.AttributeValueMemberN{Value: fmt.Sprint(s.now().Add(s.ttl).UnixMilli())}
}

func key(shardID string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"shard_id": &types.AttributeValueMemberS{Value: shardID}}
}

func str(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }

func isConditionFailed(err error) bool {
	var c *types.ConditionalCheckFailedException
	return errors.As(err, &c)
}

// Acquire takes the lease if it is free, expired or already ours, and extends
// it. It is also how a holder renews. The returned lease carries the checkpoint
// to resume from. A finished shard can't be acquired.
func (s *Store) Acquire(ctx context.Context, shardID, owner string) (Lease, error) {
	out, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(s.table),
		Key:              key(shardID),
		UpdateExpression: aws.String("SET #o = :o, expires_ms = :e"),
		ConditionExpression: aws.String(
			"(attribute_not_exists(shard_id) OR #o = :o OR expires_ms < :now) AND " +
				"(attribute_not_exists(finished) OR finished = :no)"),
		ExpressionAttributeNames: map[string]string{"#o": "owner"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":o":   str(owner),
			":e":   s.expiry(),
			":now": &types.AttributeValueMemberN{Value: fmt.Sprint(s.now().UnixMilli())},
			":no":  &types.AttributeValueMemberBOOL{Value: false},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	if isConditionFailed(err) {
		return Lease{}, ErrHeld
	}
	if err != nil {
		return Lease{}, err
	}
	var l Lease
	if err := attributevalue.UnmarshalMap(out.Attributes, &l); err != nil {
		return Lease{}, err
	}
	return l, nil
}

// Checkpoint records that everything up to seq is done, and extends the lease.
// ErrHeld means we lost the shard and must stop reading it.
func (s *Store) Checkpoint(ctx context.Context, shardID, owner, seq string) error {
	return s.ownerUpdate(ctx, shardID, owner, "SET checkpoint = :c, expires_ms = :e",
		map[string]types.AttributeValue{":c": str(seq), ":e": s.expiry()})
}

// Finish marks a closed shard as fully consumed so its children can start.
func (s *Store) Finish(ctx context.Context, shardID, owner string) error {
	return s.ownerUpdate(ctx, shardID, owner, "SET finished = :t",
		map[string]types.AttributeValue{":t": &types.AttributeValueMemberBOOL{Value: true}})
}

// Release hands the shard back immediately instead of waiting for expiry.
func (s *Store) Release(ctx context.Context, shardID, owner string) error {
	return s.ownerUpdate(ctx, shardID, owner, "SET expires_ms = :z",
		map[string]types.AttributeValue{":z": &types.AttributeValueMemberN{Value: "0"}})
}

func (s *Store) ownerUpdate(ctx context.Context, shardID, owner, update string, vals map[string]types.AttributeValue) error {
	vals[":o"] = str(owner)
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(s.table),
		Key:                       key(shardID),
		UpdateExpression:          aws.String(update),
		ConditionExpression:       aws.String("#o = :o"),
		ExpressionAttributeNames:  map[string]string{"#o": "owner"},
		ExpressionAttributeValues: vals,
	})
	if isConditionFailed(err) {
		return ErrHeld
	}
	return err
}

// Heartbeat registers this worker so others can count how many are alive.
func (s *Store) Heartbeat(ctx context.Context, owner string) error {
	item, err := attributevalue.MarshalMap(Lease{
		ShardID:   workerPrefix + owner,
		Owner:     owner,
		ExpiresMs: s.now().Add(s.ttl).UnixMilli(),
	})
	if err != nil {
		return err
	}
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(s.table), Item: item})
	return err
}

// Unregister removes this worker's heartbeat row on clean shutdown.
func (s *Store) Unregister(ctx context.Context, owner string) error {
	_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(s.table), Key: key(workerPrefix + owner)})
	return err
}

// State is everything in the table at one moment.
type State struct {
	Shards  map[string]Lease // by shard id
	Workers int              // workers with an unexpired heartbeat
}

// Snapshot reads the whole table (a handful of rows) and drops heartbeat rows
// of workers that have been gone for a while.
func (s *Store) Snapshot(ctx context.Context) (State, error) {
	st := State{Shards: map[string]Lease{}}
	now := s.now()
	var start map[string]types.AttributeValue
	for {
		out, err := s.client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(s.table), ExclusiveStartKey: start})
		if err != nil {
			return State{}, err
		}
		for _, it := range out.Items {
			var l Lease
			if err := attributevalue.UnmarshalMap(it, &l); err != nil {
				return State{}, err
			}
			if !strings.HasPrefix(l.ShardID, workerPrefix) {
				st.Shards[l.ShardID] = l
				continue
			}
			switch {
			case !l.Expired(now):
				st.Workers++
			case now.UnixMilli()-l.ExpiresMs > (10 * time.Minute).Milliseconds():
				_, _ = s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(s.table), Key: key(l.ShardID)})
			}
		}
		if len(out.LastEvaluatedKey) == 0 {
			return st, nil
		}
		start = out.LastEvaluatedKey
	}
}
