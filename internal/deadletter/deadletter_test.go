package deadletter

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dn-cuong/fraudguard/internal/history"
	"github.com/google/uuid"
)

// Needs DynamoDB Local, see internal/lease/lease_test.go.
func TestPutStoresRecordAndOverwritesOnReplay(t *testing.T) {
	ep := os.Getenv("DYNAMO_TEST_ENDPOINT")
	if ep == "" {
		t.Skip("DYNAMO_TEST_ENDPOINT not set")
	}
	client := history.NewClient(history.Config{Region: "us-east-1", Endpoint: ep})
	s := New(client, "dead-"+uuid.NewString())
	ctx := context.Background()
	if err := s.EnsureTable(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ { // the second Put is a replayed batch
		if err := s.Put(ctx, "shard-0", "42", []byte("{not json"), errors.New("bad record")); err != nil {
			t.Fatal(err)
		}
	}
	out, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(s.table)})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Items) != 1 {
		t.Fatalf("want 1 dead letter, got %d", len(out.Items))
	}
	if data := out.Items[0]["data"].(*types.AttributeValueMemberB).Value; string(data) != "{not json" {
		t.Fatalf("raw record not kept: %q", data)
	}
}
