package scorer

import (
	"context"
	"fmt"
	"time"

	"github.com/mit/fraudguard/internal/engine"
	"github.com/mit/fraudguard/internal/history"
	"github.com/mit/fraudguard/internal/payment"
	"github.com/mit/fraudguard/internal/velocity"
)

// Service reads shared state, evaluates rules, then writes counters + history.
type Service struct {
	engine  *engine.Engine
	vel     *velocity.Store
	history *history.Store
	cfg     engine.Config
}

func New(eng *engine.Engine, vel *velocity.Store, hist *history.Store, cfg engine.Config) *Service {
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

	cardVel, err := s.vel.GetCard(ctx, p.CardID)
	if err != nil {
		return payment.ScoreResult{}, fmt.Errorf("redis card velocity: %w", err)
	}
	ipVel, err := s.vel.GetIP(ctx, p.IP)
	if err != nil {
		return payment.ScoreResult{}, fmt.Errorf("redis ip velocity: %w", err)
	}
	hadDispute, err := s.history.HasDispute(ctx, p.CardID)
	if err != nil {
		return payment.ScoreResult{}, fmt.Errorf("dynamodb dispute lookup: %w", err)
	}

	res := s.engine.Evaluate(p, engine.Snapshot{
		CardVelocity: cardVel,
		IPVelocity:   ipVel,
		HadDispute:   hadDispute,
	})

	if _, err := s.vel.IncrCard(ctx, p.CardID, s.cfg.VelocityCardWindow); err != nil {
		return payment.ScoreResult{}, fmt.Errorf("redis incr card: %w", err)
	}
	if _, err := s.vel.IncrIP(ctx, p.IP, s.cfg.VelocityIPWindow); err != nil {
		return payment.ScoreResult{}, fmt.Errorf("redis incr ip: %w", err)
	}

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
