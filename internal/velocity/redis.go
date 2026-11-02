package velocity

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Store tracks hot counters in Redis / ElastiCache.
type Store struct {
	rdb *redis.Client
}

func New(addr string) *Store {
	return &Store{
		rdb: redis.NewClient(&redis.Options{
			Addr:         addr,
			DialTimeout:  2 * time.Second,
			ReadTimeout:  500 * time.Millisecond,
			WriteTimeout: 500 * time.Millisecond,
		}),
	}
}

// NewWithClient builds a Store around an existing Redis client (tests).
func NewWithClient(rdb *redis.Client) *Store {
	return &Store{rdb: rdb}
}

func (s *Store) Ping(ctx context.Context) error { return s.rdb.Ping(ctx).Err() }
func (s *Store) Close() error                   { return s.rdb.Close() }

func cardKey(cardID string) string { return fmt.Sprintf("win:card:%s", cardID) }
func ipKey(ip string) string       { return fmt.Sprintf("win:ip:%s", ip) }

// reorderSlack is how far apart two txns can be scored out of arrival order and
// still count each other. The worker pool scores a batch concurrently, so a
// later txn can reach Redis first.
const reorderSlack = time.Second

// hitScript is a sliding window over a sorted set: member = txn_id, score =
// arrival time in ms. It drops entries too old to matter, adds this txn and
// counts the window ending at the txn's arrival.
//
//	KEYS[1] window set, ARGV[1] arrival ms, ARGV[2] txn_id, ARGV[3] drop-up-to,
//	ARGV[4] count-from, ARGV[5] count-to, ARGV[6] TTL ms
//
// Time is the txn's arrival, not the moment it's scored, so a backlog drained
// after an outage isn't mistaken for a burst. ZADD NX makes it idempotent: a
// redelivered txn keeps its original timestamp and isn't counted twice. All of
// it is one script, so it's atomic and the TTL can't get lost.
var hitScript = redis.NewScript(`
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[3])
redis.call('ZADD', KEYS[1], 'NX', ARGV[1], ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[6])
return redis.call('ZCOUNT', KEYS[1], ARGV[4], ARGV[5])
`)

// HitCard counts txnID, which arrived at `at`, in the card's sliding window and
// returns how many txns (including this one) the card made in the `window`
// before it. `at` must come from the server, never from the client.
func (s *Store) HitCard(ctx context.Context, cardID, txnID string, at time.Time, window time.Duration) (int64, error) {
	return s.hit(ctx, cardKey(cardID), txnID, at, window)
}

// HitIP is HitCard for an IP address.
func (s *Store) HitIP(ctx context.Context, ip, txnID string, at time.Time, window time.Duration) (int64, error) {
	return s.hit(ctx, ipKey(ip), txnID, at, window)
}

func (s *Store) hit(ctx context.Context, key, txnID string, at time.Time, window time.Duration) (int64, error) {
	ms, win, slack := at.UnixMilli(), window.Milliseconds(), reorderSlack.Milliseconds()
	return hitScript.Run(ctx, s.rdb, []string{key}, ms, txnID,
		ms-win-slack,               // drop up to
		fmt.Sprintf("(%d", ms-win), // count from (exclusive)
		ms+slack,                   // count to
		win+slack,                  // TTL
	).Int64()
}
