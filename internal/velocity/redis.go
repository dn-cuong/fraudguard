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

// hitScript is a sliding window over a sorted set: member = txn_id, score =
// time in ms. It drops entries older than the window, adds this txn and
// returns the size.
//
//	KEYS[1] window set, ARGV[1] window in ms, ARGV[2] txn_id
//
// Time comes from Redis, so every worker uses the same clock. ZADD NX makes it
// idempotent: a redelivered txn keeps its original timestamp and isn't counted
// twice. All of it is one script, so it's atomic and the TTL can't get lost.
var hitScript = redis.NewScript(`
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - ARGV[1])
redis.call('ZADD', KEYS[1], 'NX', now, ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[1])
return redis.call('ZCARD', KEYS[1])
`)

// HitCard counts txnID in the card's sliding window and returns how many txns
// (including this one) the card has made in the last `window`.
func (s *Store) HitCard(ctx context.Context, cardID, txnID string, window time.Duration) (int64, error) {
	return s.hit(ctx, cardKey(cardID), txnID, window)
}

// HitIP is HitCard for an IP address.
func (s *Store) HitIP(ctx context.Context, ip, txnID string, window time.Duration) (int64, error) {
	return s.hit(ctx, ipKey(ip), txnID, window)
}

func (s *Store) hit(ctx context.Context, key, txnID string, window time.Duration) (int64, error) {
	return hitScript.Run(ctx, s.rdb, []string{key}, window.Milliseconds(), txnID).Int64()
}
