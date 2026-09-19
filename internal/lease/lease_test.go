package lease

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mit/fraudguard/internal/history"
)

// These run against DynamoDB Local. Set DYNAMO_TEST_ENDPOINT (CI does);
// locally: make up && DYNAMO_TEST_ENDPOINT=http://localhost:8000 go test ./...
func newStore(t *testing.T) (*Store, *time.Time) {
	t.Helper()
	ep := os.Getenv("DYNAMO_TEST_ENDPOINT")
	if ep == "" {
		t.Skip("DYNAMO_TEST_ENDPOINT not set")
	}
	client := history.NewClient(history.Config{Region: "us-east-1", Endpoint: ep})
	s := New(client, "leases-"+uuid.NewString(), 30*time.Second)
	clock := time.Now()
	s.now = func() time.Time { return clock }
	if err := s.EnsureTable(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s, &clock
}

func TestAcquireIsExclusive(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if _, err := s.Acquire(ctx, "shard-0", "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Acquire(ctx, "shard-0", "b"); !errors.Is(err, ErrHeld) {
		t.Fatalf("b should be refused, got %v", err)
	}
	if _, err := s.Acquire(ctx, "shard-0", "a"); err != nil {
		t.Fatalf("owner should be able to renew, got %v", err)
	}
}

func TestExpiredLeaseCanBeTakenOver(t *testing.T) {
	s, clock := newStore(t)
	ctx := context.Background()
	s.Acquire(ctx, "shard-0", "a") //nolint:errcheck
	if err := s.Checkpoint(ctx, "shard-0", "a", "seq-42"); err != nil {
		t.Fatal(err)
	}

	*clock = clock.Add(31 * time.Second) // a stopped renewing
	l, err := s.Acquire(ctx, "shard-0", "b")
	if err != nil {
		t.Fatalf("b should take over: %v", err)
	}
	if l.Checkpoint != "seq-42" || l.Owner != "b" {
		t.Fatalf("takeover should resume from a's checkpoint, got %+v", l)
	}
	// the old owner can no longer checkpoint or finish
	if err := s.Checkpoint(ctx, "shard-0", "a", "seq-99"); !errors.Is(err, ErrHeld) {
		t.Fatalf("stale owner must be refused, got %v", err)
	}
}

func TestReleaseFreesTheShardImmediately(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	s.Acquire(ctx, "shard-0", "a") //nolint:errcheck
	if err := s.Release(ctx, "shard-0", "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Acquire(ctx, "shard-0", "b"); err != nil {
		t.Fatalf("released shard should be free: %v", err)
	}
}

func TestFinishedShardCannotBeAcquired(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	s.Acquire(ctx, "shard-0", "a") //nolint:errcheck
	if err := s.Finish(ctx, "shard-0", "a"); err != nil {
		t.Fatal(err)
	}
	s.Release(ctx, "shard-0", "a") //nolint:errcheck
	if _, err := s.Acquire(ctx, "shard-0", "b"); !errors.Is(err, ErrHeld) {
		t.Fatalf("finished shard must stay closed, got %v", err)
	}
	st, err := s.Snapshot(ctx)
	if err != nil || !st.Shards["shard-0"].Finished {
		t.Fatalf("snapshot: %+v %v", st, err)
	}
}

func TestSnapshotCountsLiveWorkers(t *testing.T) {
	s, clock := newStore(t)
	ctx := context.Background()
	s.Heartbeat(ctx, "a") //nolint:errcheck
	s.Heartbeat(ctx, "b") //nolint:errcheck
	st, _ := s.Snapshot(ctx)
	if st.Workers != 2 {
		t.Fatalf("want 2 workers, got %d", st.Workers)
	}
	s.Unregister(ctx, "b") //nolint:errcheck
	st, _ = s.Snapshot(ctx)
	if st.Workers != 1 {
		t.Fatalf("want 1 worker after unregister, got %d", st.Workers)
	}
	*clock = clock.Add(time.Minute) // a's heartbeat expires
	st, _ = s.Snapshot(ctx)
	if st.Workers != 0 {
		t.Fatalf("want 0 live workers, got %d", st.Workers)
	}
	if len(st.Shards) != 0 {
		t.Fatalf("worker rows must not show up as shards: %+v", st.Shards)
	}
}
