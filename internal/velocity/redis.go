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

func cardKey(cardID string) string { return fmt.Sprintf("vel:card:%s", cardID) }
func ipKey(ip string) string       { return fmt.Sprintf("vel:ip:%s", ip) }

// hitScript counts a txn against a window in one atomic step.
//
//	KEYS[1] counter, KEYS[2] per-txn marker, ARGV[1] window in ms.
//
// The marker makes the hit idempotent: Kinesis can redeliver a record, and
// the same txn must not be counted twice. The counter and its TTL are set in
// the same script so a crash can't leave a counter that never expires.
var hitScript = redis.NewScript(`
if redis.call('SET', KEYS[2], 1, 'NX', 'PX', ARGV[1]) then
  local n = redis.call('INCR', KEYS[1])
  if n == 1 then
    redis.call('PEXPIRE', KEYS[1], ARGV[1])
  end
  return n
end
return tonumber(redis.call('GET', KEYS[1]) or '0')
`)

// HitCard counts txnID against the card's window and returns the count
// including this txn.
func (s *Store) HitCard(ctx context.Context, cardID, txnID string, window time.Duration) (int64, error) {
	return s.hit(ctx, cardKey(cardID), seenKey("card", cardID, txnID), window)
}

// HitIP is HitCard for an IP address.
func (s *Store) HitIP(ctx context.Context, ip, txnID string, window time.Duration) (int64, error) {
	return s.hit(ctx, ipKey(ip), seenKey("ip", ip, txnID), window)
}

func seenKey(kind, subject, txnID string) string {
	return fmt.Sprintf("vel:seen:%s:%s:%s", kind, subject, txnID)
}

func (s *Store) hit(ctx context.Context, key, seen string, window time.Duration) (int64, error) {
	return hitScript.Run(ctx, s.rdb, []string{key, seen}, window.Milliseconds()).Int64()
}
