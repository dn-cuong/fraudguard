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

func TestHitWindowResets(t *testing.T) {
	s, mr := newStore(t)
	ctx := context.Background()
	for _, id := range []string{"t1", "t2"} {
		if _, err := s.HitCard(ctx, "c1", id, time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	mr.FastForward(time.Minute + time.Second)
	n, _ := s.HitCard(ctx, "c1", "t3", time.Minute)
	if n != 1 {
		t.Fatalf("want fresh window, got %d", n)
	}
}
