package stream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/dn-cuong/fraudguard/internal/lease"
	"github.com/dn-cuong/fraudguard/internal/payment"
)

type fakeScorer struct {
	scoreErrs  []error // returned in order, then nil
	calls      int
	failed     []error
	failureErr error
}

func (f *fakeScorer) Score(context.Context, payment.Payment) (payment.ScoreResult, error) {
	var err error
	if f.calls < len(f.scoreErrs) {
		err = f.scoreErrs[f.calls]
	}
	f.calls++
	return payment.ScoreResult{}, err
}

func (f *fakeScorer) RecordFailure(_ context.Context, _ payment.Payment, cause error) error {
	f.failed = append(f.failed, cause)
	return f.failureErr
}

var errBoom = errors.New("redis down")

func TestProcessRetriesThenSucceeds(t *testing.T) {
	f := &fakeScorer{scoreErrs: []error{errBoom, errBoom}}
	c := &Consumer{scorer: f}
	if !c.process(context.Background(), payment.Payment{TxnID: "t"}) {
		t.Fatal("want resolved")
	}
	if f.calls != 3 || len(f.failed) != 0 {
		t.Fatalf("calls=%d failed=%v", f.calls, f.failed)
	}
}

func TestProcessGivesUpWithAnErrorRecord(t *testing.T) {
	f := &fakeScorer{scoreErrs: []error{errBoom, errBoom, errBoom, errBoom}}
	c := &Consumer{scorer: f}
	if !c.process(context.Background(), payment.Payment{TxnID: "t"}) {
		t.Fatal("an ERROR record counts as a result")
	}
	if f.calls != maxAttempts || len(f.failed) != 1 {
		t.Fatalf("calls=%d failed=%v", f.calls, f.failed)
	}
}

func TestProcessDoesNotRetryPermanentErrors(t *testing.T) {
	f := &fakeScorer{scoreErrs: []error{payment.ErrInvalid}}
	c := &Consumer{scorer: f}
	c.process(context.Background(), payment.Payment{TxnID: "t"})
	if f.calls != 1 || len(f.failed) != 1 {
		t.Fatalf("calls=%d failed=%v", f.calls, f.failed)
	}
}

// If even the ERROR record can't be stored, the batch must not be
// checkpointed, so the txn is read again later.
func TestProcessUnresolvedWhenFailureCannotBeStored(t *testing.T) {
	f := &fakeScorer{scoreErrs: []error{errBoom, errBoom, errBoom}, failureErr: errBoom}
	c := &Consumer{scorer: f}
	if c.process(context.Background(), payment.Payment{TxnID: "t"}) {
		t.Fatal("want unresolved")
	}
}

func TestProcessStopsOnShutdown(t *testing.T) {
	f := &fakeScorer{}
	c := &Consumer{scorer: f}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if c.process(ctx, payment.Payment{TxnID: "t"}) || f.calls != 0 {
		t.Fatal("cancelled ctx must not score or report a result")
	}
}

func TestProcessBatchIgnoresClientTimestamp(t *testing.T) {
	c := &Consumer{jobs: make(chan job, 1)}
	arrival := time.Unix(1_800_000_000, 0)
	go func() {
		j := <-c.jobs
		if !j.p.Timestamp.Equal(arrival) {
			t.Errorf("got %v, want arrival %v", j.p.Timestamp, arrival)
		}
		j.batch.wg.Done()
	}()
	c.processBatch(context.Background(), "s", []types.Record{{
		Data:                        []byte(`{"txn_id":"t","card_id":"c","timestamp":"2001-01-01T00:00:00Z"}`),
		ApproximateArrivalTimestamp: &arrival,
	}})
}

// A reader rebalance gave up is no longer in c.readers but can still be sending
// on c.jobs, which Run closes right after stopAll.
func TestStopAllWaitsForDroppedReaders(t *testing.T) {
	c := &Consumer{readers: map[string]*reader{}}
	var mu sync.Mutex
	exited := false
	c.running.Add(1)
	go func() {
		defer c.running.Done()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		exited = true
		mu.Unlock()
	}()
	c.stopAll()
	mu.Lock()
	defer mu.Unlock()
	if !exited {
		t.Fatal("stopAll returned while a reader was still running")
	}
}

func TestParentsDone(t *testing.T) {
	parent := types.Shard{ShardId: aws.String("p")}
	child := types.Shard{ShardId: aws.String("c"), ParentShardId: aws.String("p")}
	merged := types.Shard{ShardId: aws.String("m"), ParentShardId: aws.String("p"), AdjacentParentShardId: aws.String("q")}
	byID := map[string]types.Shard{"p": parent, "c": child, "m": merged, "q": {ShardId: aws.String("q")}}

	st := lease.State{Shards: map[string]lease.Lease{}}
	if !parentsDone(parent, byID, st) {
		t.Fatal("a root shard has no parents to wait for")
	}
	if parentsDone(child, byID, st) {
		t.Fatal("child must wait for its parent")
	}
	st.Shards["p"] = lease.Lease{Finished: true}
	if !parentsDone(child, byID, st) {
		t.Fatal("child can start once the parent is finished")
	}
	if parentsDone(merged, byID, st) {
		t.Fatal("merged shard must wait for both parents")
	}
	delete(byID, "q") // q aged out of the stream
	if !parentsDone(merged, byID, st) {
		t.Fatal("a parent that left the stream has nothing left to read")
	}
}
