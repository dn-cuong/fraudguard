package velocity

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newStore(t *testing.T) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	return NewWithClient(redis.NewClient(&redis.Options{Addr: mr.Addr()})), mr
}

func TestHitCountsIncludeCurrentTxn(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		n, err := s.HitCard(ctx, "c1", fmt.Sprintf("t%d", i), time.Minute)
		if err != nil || n != int64(i) {
			t.Fatalf("hit %d: n=%d err=%v", i, n, err)
		}
	}
}

// The old read-then-increment let concurrent txns all see the same count.
func TestHitConcurrentCountsAreDistinct(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	const n = 50
	got := make([]int64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := s.HitCard(ctx, "c1", fmt.Sprintf("t%d", i), time.Minute)
			if err != nil {
				t.Error(err)
			}
			got[i] = c
		}()
	}
	wg.Wait()
	sort.Slice(got, func(a, b int) bool { return got[a] < got[b] })
	for i, c := range got {
		if c != int64(i+1) {
			t.Fatalf("counts should be 1..%d with no repeats, got %v", n, got)
		}
	}
}

func TestHitSameTxnCountsOnce(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	a, _ := s.HitIP(ctx, "1.1.1.1", "t1", time.Minute)
	b, _ := s.HitIP(ctx, "1.1.1.1", "t1", time.Minute) // redelivery
	c, _ := s.HitIP(ctx, "1.1.1.1", "t2", time.Minute)
	if a != 1 || b != 1 || c != 2 {
		t.Fatalf("want 1,1,2 got %d,%d,%d", a, b, c)
	}
}

func TestHitAlwaysSetsTTL(t *testing.T) {
	s, mr := newStore(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := s.HitCard(ctx, "c1", fmt.Sprintf("t%d", i), time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	if ttl := mr.TTL(cardKey("c1")); ttl <= 0 || ttl > time.Minute {
		t.Fatalf("counter ttl = %v", ttl)
	}
}

func TestHitOldEntriesLeaveTheWindow(t *testing.T) {
	s, mr := newStore(t)
	ctx := context.Background()
	t0 := time.Unix(1_800_000_000, 0)
	var got []int64
	for i, at := range []time.Duration{0, 40 * time.Second, 70 * time.Second} {
		mr.SetTime(t0.Add(at))
		n, err := s.HitCard(ctx, "c1", fmt.Sprintf("t%d", i), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, n)
	}
	// at 70s the hit from 0s is out of the 60s window, the one from 40s is not
	if got[0] != 1 || got[1] != 2 || got[2] != 2 {
		t.Fatalf("want 1,2,2 got %v", got)
	}
}

// A fixed window resets at the boundary, so 5 txns at 55s and 5 at 65s never
// counted more than 5. A sliding window sees all 10 within 60s.
func TestHitBurstAcrossWindowBoundary(t *testing.T) {
	s, mr := newStore(t)
	ctx := context.Background()
	t0 := time.Unix(1_800_000_000, 0)
	var last int64
	for i := 0; i < 10; i++ {
		at := 55 * time.Second
		if i >= 5 {
			at = 65 * time.Second
		}
		mr.SetTime(t0.Add(at))
		n, err := s.HitCard(ctx, "c1", fmt.Sprintf("t%d", i), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		last = n
	}
	if last != 10 {
		t.Fatalf("want 10 in the sliding window, got %d", last)
	}
}

func TestHitRedeliveryKeepsOriginalTime(t *testing.T) {
	s, mr := newStore(t)
	ctx := context.Background()
	t0 := time.Unix(1_800_000_000, 0)
	mr.SetTime(t0)
	s.HitCard(ctx, "c1", "t1", time.Minute) //nolint:errcheck
	mr.SetTime(t0.Add(50 * time.Second))
	if n, _ := s.HitCard(ctx, "c1", "t1", time.Minute); n != 1 {
		t.Fatalf("redelivery counted again: %d", n)
	}
	// t1 keeps its original timestamp, so it expires 60s after the first hit
	mr.SetTime(t0.Add(65 * time.Second))
	if n, _ := s.HitCard(ctx, "c1", "t2", time.Minute); n != 1 {
		t.Fatalf("want only t2 left in window, got %d", n)
	}
}
