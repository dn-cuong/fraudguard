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

func (s *Store) Ping(ctx context.Context) error { return s.rdb.Ping(ctx).Err() }
func (s *Store) Close() error                   { return s.rdb.Close() }

func cardKey(cardID string) string { return fmt.Sprintf("vel:card:%s", cardID) }
func ipKey(ip string) string       { return fmt.Sprintf("vel:ip:%s", ip) }

func (s *Store) GetCard(ctx context.Context, cardID string) (int64, error) {
	n, err := s.rdb.Get(ctx, cardKey(cardID)).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return n, err
}

func (s *Store) GetIP(ctx context.Context, ip string) (int64, error) {
	n, err := s.rdb.Get(ctx, ipKey(ip)).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return n, err
}

func (s *Store) IncrCard(ctx context.Context, cardID string, window time.Duration) (int64, error) {
	return s.incr(ctx, cardKey(cardID), window)
}

func (s *Store) IncrIP(ctx context.Context, ip string, window time.Duration) (int64, error) {
	return s.incr(ctx, ipKey(ip), window)
}

func (s *Store) incr(ctx context.Context, key string, window time.Duration) (int64, error) {
	pipe := s.rdb.TxPipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, window)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return incr.Val(), nil
}
