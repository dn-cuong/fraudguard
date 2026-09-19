package scorer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mit/fraudguard/internal/engine"
	"github.com/mit/fraudguard/internal/payment"
)

type velocityStore interface {
	HitCard(ctx context.Context, cardID, txnID string, window time.Duration) (int64, error)
	HitIP(ctx context.Context, ip, txnID string, window time.Duration) (int64, error)
}

type historyStore interface {
	HasDispute(ctx context.Context, cardID string) (bool, error)
	Put(ctx context.Context, rec payment.Record) error
}

// ErrUnsupportedCurrency is returned for anything but USD. The amount rule
// compares against a USD threshold and there is no FX conversion.
var ErrUnsupportedCurrency = errors.New("only USD is supported")

// Service scores one payment. Counters are hit first (atomic, per-txn
// idempotent) and the returned counts feed the rules, so concurrent payments
// on the same card each see a distinct count.
type Service struct {
	engine  *engine.Engine
	vel     velocityStore
	history historyStore
	cfg     engine.Config
}

func New(eng *engine.Engine, vel velocityStore, hist historyStore, cfg engine.Config) *Service {
	return &Service{engine: eng, vel: vel, history: hist, cfg: cfg}
}

func (s *Service) Score(ctx context.Context, p payment.Payment) (payment.ScoreResult, error) {
	start := time.Now()
	if p.Timestamp.IsZero() {
		p.Timestamp = time.Now().UTC()
	}
	if p.Currency == "" {
		p.Currency = "USD"
	}

	if p.Currency != "USD" {
		return payment.ScoreResult{}, ErrUnsupportedCurrency
	}

	cardCount, err := s.vel.HitCard(ctx, p.CardID, p.TxnID, s.cfg.VelocityCardWindow)
	if err != nil {
		return payment.ScoreResult{}, fmt.Errorf("redis card velocity: %w", err)
	}
	ipCount, err := s.vel.HitIP(ctx, p.IP, p.TxnID, s.cfg.VelocityIPWindow)
	if err != nil {
		return payment.ScoreResult{}, fmt.Errorf("redis ip velocity: %w", err)
	}
	hadDispute, err := s.history.HasDispute(ctx, p.CardID)
	if err != nil {
		return payment.ScoreResult{}, fmt.Errorf("dynamodb dispute lookup: %w", err)
	}

	res := s.engine.Evaluate(p, engine.Snapshot{
		CardCount:  cardCount,
		IPCount:    ipCount,
		HadDispute: hadDispute,
	})

	rec := payment.Record{
		TxnID:     p.TxnID,
		CardID:    p.CardID,
		UserID:    p.UserID,
		Amount:    p.Amount,
		Currency:  p.Currency,
		Merchant:  p.Merchant,
		Country:   p.Country,
		IP:        p.IP,
		Decision:  res.Decision,
		Score:     res.Score,
		Timestamp: p.Timestamp,
	}
	if err := s.history.Put(ctx, rec); err != nil {
		return payment.ScoreResult{}, fmt.Errorf("dynamodb put: %w", err)
	}

	res.LatencyMs = float64(time.Since(start).Microseconds()) / 1000.0
	return res, nil
}
