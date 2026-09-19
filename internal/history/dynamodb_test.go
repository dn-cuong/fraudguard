package history

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/mit/fraudguard/internal/payment"
)

// Needs DynamoDB Local, see internal/lease/lease_test.go.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	ep := os.Getenv("DYNAMO_TEST_ENDPOINT")
	if ep == "" {
		t.Skip("DYNAMO_TEST_ENDPOINT not set")
	}
	s := New(Config{Region: "us-east-1", Endpoint: ep, TableName: "txns-" + uuid.NewString()})
	if err := s.EnsureTable(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPutKeepsFirstResultAndDisputeFlag(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	rec := payment.Record{TxnID: "t1", CardID: "c1", Decision: payment.DecisionAllow, Score: 0}
	if err := s.Put(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkDisputed(ctx, "c1", "t1"); err != nil {
		t.Fatal(err)
	}

	// the same txn is delivered and scored again
	again := rec
	again.Decision, again.Score = payment.DecisionDecline, 90
	if err := s.Put(ctx, again); err != nil {
		t.Fatalf("replay must not error: %v", err)
	}

	got, err := s.GetByTxnID(ctx, "t1")
	if err != nil || got == nil {
		t.Fatalf("get: %v %v", got, err)
	}
	if got.Decision != payment.DecisionAllow || !got.Disputed {
		t.Fatalf("replay overwrote the record: %+v", got)
	}
	if ok, err := s.HasDispute(ctx, "c1"); err != nil || !ok {
		t.Fatalf("dispute lost: %v %v", ok, err)
	}
	if ok, _ := s.HasDispute(ctx, "other-card"); ok {
		t.Fatal("dispute leaked to another card")
	}
}
