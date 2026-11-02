package history

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/dn-cuong/fraudguard/internal/payment"
	"github.com/google/uuid"
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

func TestPutSetsTTLAndDisputeClearsIt(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for _, txn := range []string{"keep", "drop"} {
		if err := s.Put(ctx, payment.Record{TxnID: txn, CardID: "c1"}); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := s.GetByTxnID(ctx, "drop")
	if want := time.Now().Add(retention).Unix(); got == nil || got.ExpiresAt < want-60 || got.ExpiresAt > want {
		t.Fatalf("expires_at not ~90 days out: %+v", got)
	}
	if err := s.MarkDisputed(ctx, "c1", "keep"); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.GetByTxnID(ctx, "keep"); got == nil || got.ExpiresAt != 0 {
		t.Fatalf("disputed txn would still expire: %+v", got)
	}
}

// Ingest and worker start together and both create the table. The one that
// loses the race used to exit on ResourceInUseException.
func TestEnsureTableConcurrently(t *testing.T) {
	ep := os.Getenv("DYNAMO_TEST_ENDPOINT")
	if ep == "" {
		t.Skip("DYNAMO_TEST_ENDPOINT not set")
	}
	name := "txns-" + uuid.NewString()
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = New(Config{Region: "us-east-1", Endpoint: ep, TableName: name}).EnsureTable(context.Background())
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d: %v", i, err)
		}
	}
}
