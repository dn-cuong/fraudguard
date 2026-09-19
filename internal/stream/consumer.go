package stream

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/mit/fraudguard/internal/lease"
	"github.com/mit/fraudguard/internal/payment"
	"github.com/mit/fraudguard/internal/scorer"
)

// Scorer is what the consumer needs from the scoring service.
type Scorer interface {
	Score(ctx context.Context, p payment.Payment) (payment.ScoreResult, error)
	RecordFailure(ctx context.Context, p payment.Payment, cause error) error
}

const (
	tickEvery   = 5 * time.Second
	maxAttempts = 3
	retryBase   = 50 * time.Millisecond
)

// Consumer reads a Kinesis stream as one member of a group of workers.
//
// Shards are shared through leases in DynamoDB (see internal/lease): each
// worker takes its fair share, renews it, and picks up shards from workers that
// stop renewing. A batch is only checkpointed once every record in it has a
// result, so a crash replays the batch instead of losing it; scoring is
// idempotent, so the replay is safe. A child shard (after a split or merge)
// starts only when its parents are fully consumed, so per-card order holds.
type Consumer struct {
	client     *kinesis.Client
	streamName string
	owner      string
	pool       int
	leases     *lease.Store
	scorer     Scorer

	jobs     chan job
	mu       sync.Mutex
	readers  map[string]*reader
	scored   atomic.Int64
	errors   atomic.Int64
	latSumUs atomic.Int64
}

type reader struct {
	cancel context.CancelFunc
	done   chan struct{}
}

type job struct {
	ctx   context.Context
	p     payment.Payment
	batch *batch
}

// batch tracks one GetRecords result while the pool works on it.
type batch struct {
	wg         sync.WaitGroup
	unresolved atomic.Int64
}

// NewConsumer builds a consumer. owner must be unique per process.
func NewConsumer(client *kinesis.Client, streamName, owner string, pool int, leases *lease.Store, s Scorer) *Consumer {
	if pool < 1 {
		pool = 8
	}
	return &Consumer{
		client:     client,
		streamName: streamName,
		owner:      owner,
		pool:       pool,
		leases:     leases,
		scorer:     s,
		jobs:       make(chan job, pool*4),
		readers:    map[string]*reader{},
	}
}

func (c *Consumer) Stats() (scored, errs int64, avgMs float64) {
	n := c.scored.Load()
	if n == 0 {
		return 0, c.errors.Load(), 0
	}
	return n, c.errors.Load(), float64(c.latSumUs.Load()) / float64(n) / 1000.0
}

// Owned is how many shards this worker is reading right now.
func (c *Consumer) Owned() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.readers)
}

// Run blocks until ctx is cancelled, then releases its shards.
func (c *Consumer) Run(ctx context.Context) error {
	var pool sync.WaitGroup
	for i := 0; i < c.pool; i++ {
		pool.Add(1)
		go func() {
			defer pool.Done()
			for j := range c.jobs {
				if !c.process(j.ctx, j.p) {
					j.batch.unresolved.Add(1)
				}
				j.batch.wg.Done()
			}
		}()
	}

	slog.Info("consumer starting", "owner", c.owner, "pool", c.pool)
	t := time.NewTicker(tickEvery)
	defer t.Stop()
	for {
		if err := c.rebalance(ctx); err != nil && ctx.Err() == nil {
			slog.Error("rebalance", "err", err)
		}
		select {
		case <-ctx.Done():
			c.stopAll()
			// readers are gone, so nothing sends on jobs any more
			close(c.jobs)
			pool.Wait()
			// ctx is cancelled, so use a fresh one to hand the shards back
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = c.leases.Unregister(cleanup, c.owner)
			return nil
		case <-t.C:
		}
	}
}

// rebalance is the once-per-tick lease bookkeeping: renew what we hold, give
// back any excess, and take free shards up to our fair share.
func (c *Consumer) rebalance(ctx context.Context) error {
	shards, err := c.listShards(ctx)
	if err != nil {
		return err
	}
	if err := c.leases.Heartbeat(ctx, c.owner); err != nil {
		return err
	}
	st, err := c.leases.Snapshot(ctx)
	if err != nil {
		return err
	}
	c.pruneAndRenew(ctx)

	byID := map[string]types.Shard{}
	open := 0 // shards that still have records to read
	for _, sh := range shards {
		byID[aws.ToString(sh.ShardId)] = sh
		if !st.Shards[aws.ToString(sh.ShardId)].Finished {
			open++
		}
	}
	workers := max(st.Workers, 1)
	want := (open + workers - 1) / workers

	c.mu.Lock()
	var excess []*reader
	for id, r := range c.readers {
		if len(c.readers) > want {
			excess = append(excess, r)
			delete(c.readers, id)
		}
	}
	held := len(c.readers)
	c.mu.Unlock()
	for _, r := range excess {
		r.cancel() // the reader releases its lease on the way out
	}

	for _, sh := range shards {
		if held >= want {
			break
		}
		id := aws.ToString(sh.ShardId)
		if c.isRunning(id) || st.Shards[id].Finished || !parentsDone(sh, byID, st) {
			continue
		}
		if l, ok := st.Shards[id]; ok && !l.Expired(time.Now()) && l.Owner != c.owner {
			continue
		}
		l, err := c.leases.Acquire(ctx, id, c.owner)
		if errors.Is(err, lease.ErrHeld) {
			continue
		}
		if err != nil {
			return err
		}
		c.start(ctx, id, l.Checkpoint)
		held++
	}
	return nil
}

// parentsDone is true when every parent still in the stream has been fully
// consumed. A parent that has aged out of the stream no longer has records.
func parentsDone(sh types.Shard, byID map[string]types.Shard, st lease.State) bool {
	for _, p := range []*string{sh.ParentShardId, sh.AdjacentParentShardId} {
		if p == nil {
			continue
		}
		if _, still := byID[*p]; still && !st.Shards[*p].Finished {
			return false
		}
	}
	return true
}

func (c *Consumer) isRunning(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.readers[id]
	return ok
}

// pruneAndRenew forgets readers that ended and extends the lease on the rest.
func (c *Consumer) pruneAndRenew(ctx context.Context) {
	c.mu.Lock()
	ids := make([]string, 0, len(c.readers))
	for id, r := range c.readers {
		select {
		case <-r.done:
			delete(c.readers, id)
		default:
			ids = append(ids, id)
		}
	}
	c.mu.Unlock()

	for _, id := range ids {
		if _, err := c.leases.Acquire(ctx, id, c.owner); errors.Is(err, lease.ErrHeld) {
			slog.Warn("lost lease", "shard", id)
			c.mu.Lock()
			if r, ok := c.readers[id]; ok {
				r.cancel()
				delete(c.readers, id)
			}
			c.mu.Unlock()
		}
	}
}

func (c *Consumer) start(ctx context.Context, shardID, checkpoint string) {
	rctx, cancel := context.WithCancel(ctx)
	r := &reader{cancel: cancel, done: make(chan struct{})}
	c.mu.Lock()
	c.readers[shardID] = r
	c.mu.Unlock()
	slog.Info("shard acquired", "shard", shardID, "checkpoint", checkpoint)
	go func() {
		defer close(r.done)
		c.readShard(rctx, shardID, checkpoint)
		// give the shard back right away, even when ctx is cancelled
		bg, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.leases.Release(bg, shardID, c.owner)
	}()
}

func (c *Consumer) stopAll() {
	c.mu.Lock()
	rs := make([]*reader, 0, len(c.readers))
	for _, r := range c.readers {
		rs = append(rs, r)
	}
	c.mu.Unlock()
	for _, r := range rs {
		r.cancel()
	}
	for _, r := range rs {
		<-r.done
	}
}

func (c *Consumer) listShards(ctx context.Context) ([]types.Shard, error) {
	var out []types.Shard
	var token *string
	for {
		in := &kinesis.ListShardsInput{}
		if token != nil {
			in.NextToken = token // NextToken can't be combined with StreamName
		} else {
			in.StreamName = aws.String(c.streamName)
		}
		res, err := c.client.ListShards(ctx, in)
		if err != nil {
			return nil, err
		}
		out = append(out, res.Shards...)
		if res.NextToken == nil {
			return out, nil
		}
		token = res.NextToken
	}
}

func (c *Consumer) iterator(ctx context.Context, shardID, checkpoint string) (*string, error) {
	in := &kinesis.GetShardIteratorInput{
		StreamName: aws.String(c.streamName),
		ShardId:    aws.String(shardID),
	}
	if checkpoint == "" {
		// Nothing consumed yet: start at the oldest record, so payments that
		// arrived before any worker was up still get scored.
		in.ShardIteratorType = types.ShardIteratorTypeTrimHorizon
	} else {
		in.ShardIteratorType = types.ShardIteratorTypeAfterSequenceNumber
		in.StartingSequenceNumber = aws.String(checkpoint)
	}
	out, err := c.client.GetShardIterator(ctx, in)
	if err != nil {
		return nil, err
	}
	return out.ShardIterator, nil
}

// readShard consumes one shard until it is finished, the lease is lost or ctx
// ends. Anything that goes wrong restarts from the last checkpoint.
func (c *Consumer) readShard(ctx context.Context, shardID, checkpoint string) {
	for ctx.Err() == nil {
		iter, err := c.iterator(ctx, shardID, checkpoint)
		if err != nil {
			slog.Error("get shard iterator", "shard", shardID, "err", err)
			sleep(ctx, time.Second)
			continue
		}
		if c.readFrom(ctx, shardID, iter, &checkpoint) {
			return
		}
		sleep(ctx, time.Second)
	}
}

// readFrom returns true when the shard is finished or the lease is gone.
// Otherwise the caller re-reads from the checkpoint.
func (c *Consumer) readFrom(ctx context.Context, shardID string, iter *string, checkpoint *string) bool {
	for iter != nil {
		if ctx.Err() != nil {
			return true
		}
		out, err := c.client.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: iter, Limit: aws.Int32(100)})
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("get records", "shard", shardID, "err", err)
			}
			return false
		}
		if n := len(out.Records); n > 0 {
			if !c.processBatch(ctx, shardID, out.Records) {
				return false // replay from the checkpoint
			}
			seq := aws.ToString(out.Records[n-1].SequenceNumber)
			if err := c.leases.Checkpoint(ctx, shardID, c.owner, seq); err != nil {
				if errors.Is(err, lease.ErrHeld) {
					slog.Warn("lost lease while checkpointing", "shard", shardID)
					return true
				}
				slog.Error("checkpoint", "shard", shardID, "err", err)
				return false
			}
			*checkpoint = seq
		} else {
			sleep(ctx, 200*time.Millisecond)
		}
		iter = out.NextShardIterator
	}
	// a closed shard has been read to its end
	if err := c.leases.Finish(ctx, shardID, c.owner); err != nil && !errors.Is(err, lease.ErrHeld) {
		slog.Error("finish shard", "shard", shardID, "err", err)
		return false
	}
	slog.Info("shard finished", "shard", shardID)
	return true
}

// processBatch scores a batch on the pool and reports whether every record got
// a result. Unparseable records can't be recorded (no txn_id/card_id), so they
// are counted and skipped.
func (c *Consumer) processBatch(ctx context.Context, shardID string, recs []types.Record) bool {
	b := &batch{}
	sent := true
	for _, rec := range recs {
		var p payment.Payment
		if err := json.Unmarshal(rec.Data, &p); err != nil || p.TxnID == "" || p.CardID == "" {
			// Ingest always sets these, so this is malformed input.
			c.errors.Add(1)
			slog.Error("bad record skipped", "shard", shardID, "seq", aws.ToString(rec.SequenceNumber))
			continue
		}
		if p.Timestamp.IsZero() {
			p.Timestamp = rec.ApproximateArrivalTimestamp.UTC()
		}
		b.wg.Add(1)
		select {
		case c.jobs <- job{ctx: ctx, p: p, batch: b}:
		case <-ctx.Done():
			b.wg.Done()
			sent = false
		}
		if !sent {
			break
		}
	}
	b.wg.Wait()
	return sent && b.unresolved.Load() == 0
}

// process scores one payment with a few retries. A payment that still can't be
// scored gets an ERROR record instead. It returns false only when there is no
// result at all (shutdown, or even the ERROR record could not be written), and
// then the batch is not checkpointed.
func (c *Consumer) process(ctx context.Context, p payment.Payment) bool {
	start := time.Now()
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			sleep(ctx, retryBase<<attempt)
		}
		if ctx.Err() != nil {
			return false
		}
		if _, err = c.scorer.Score(ctx, p); err == nil {
			c.scored.Add(1)
			c.latSumUs.Add(time.Since(start).Microseconds())
			return true
		}
		if errors.Is(err, scorer.ErrUnsupportedCurrency) {
			break // retrying won't change the answer
		}
		slog.Warn("score failed", "txn_id", p.TxnID, "attempt", attempt+1, "err", err)
	}
	if ctx.Err() != nil {
		return false
	}
	c.errors.Add(1)
	slog.Error("giving up on txn", "txn_id", p.TxnID, "err", err)
	if ferr := c.scorer.RecordFailure(ctx, p, err); ferr != nil {
		slog.Error("could not record failure", "txn_id", p.TxnID, "err", ferr)
		return false
	}
	return true
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
