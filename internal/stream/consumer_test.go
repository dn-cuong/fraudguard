package stream

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/mit/fraudguard/internal/lease"
	"github.com/mit/fraudguard/internal/payment"
	"github.com/mit/fraudguard/internal/scorer"
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

var boom = errors.New("redis down")

func TestProcessRetriesThenSucceeds(t *testing.T) {
	f := &fakeScorer{scoreErrs: []error{boom, boom}}
	c := &Consumer{scorer: f}
	if !c.process(context.Background(), payment.Payment{TxnID: "t"}) {
		t.Fatal("want resolved")
	}
	if f.calls != 3 || len(f.failed) != 0 {
		t.Fatalf("calls=%d failed=%v", f.calls, f.failed)
	}
}

func TestProcessGivesUpWithAnErrorRecord(t *testing.T) {
	f := &fakeScorer{scoreErrs: []error{boom, boom, boom, boom}}
	c := &Consumer{scorer: f}
	if !c.process(context.Background(), payment.Payment{TxnID: "t"}) {
		t.Fatal("an ERROR record counts as a result")
	}
	if f.calls != maxAttempts || len(f.failed) != 1 {
		t.Fatalf("calls=%d failed=%v", f.calls, f.failed)
	}
}

func TestProcessDoesNotRetryPermanentErrors(t *testing.T) {
	f := &fakeScorer{scoreErrs: []error{scorer.ErrUnsupportedCurrency}}
	c := &Consumer{scorer: f}
	c.process(context.Background(), payment.Payment{TxnID: "t"})
	if f.calls != 1 || len(f.failed) != 1 {
		t.Fatalf("calls=%d failed=%v", f.calls, f.failed)
	}
}

// If even the ERROR record can't be stored, the batch must not be
// checkpointed, so the txn is read again later.
func TestProcessUnresolvedWhenFailureCannotBeStored(t *testing.T) {
	f := &fakeScorer{scoreErrs: []error{boom, boom, boom}, failureErr: boom}
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
