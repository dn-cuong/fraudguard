package engine

import (
	"fmt"
	"time"

	"github.com/mit/fraudguard/internal/payment"
)

type Config struct {
	AmountHighUSD      float64
	VelocityCardLimit  int64
	VelocityCardWindow time.Duration
	VelocityIPLimit    int64
	VelocityIPWindow   time.Duration
	DeclineScore       int
	ReviewScore        int
}

func DefaultConfig() Config {
	return Config{
		AmountHighUSD:      2500,
		VelocityCardLimit:  5,
		VelocityCardWindow: time.Minute,
		VelocityIPLimit:    20,
		VelocityIPWindow:   time.Minute,
		DeclineScore:       80,
		ReviewScore:        40,
	}
}

// Snapshot is the Redis + DynamoDB view at score time.
type Snapshot struct {
	CardVelocity int64
	IPVelocity   int64
	HadDispute   bool
}

type Engine struct {
	cfg Config
}

func New(cfg Config) *Engine {
	return &Engine{cfg: cfg}
}

func (e *Engine) Evaluate(p payment.Payment, snap Snapshot) payment.ScoreResult {
	start := time.Now()
	var triggered []payment.TriggeredRule
	score := 0

	if p.Amount >= e.cfg.AmountHighUSD {
		score += 40
		triggered = append(triggered, payment.TriggeredRule{
			ID:      "AMOUNT_HIGH",
			Message: fmt.Sprintf("amount %.2f >= %.2f", p.Amount, e.cfg.AmountHighUSD),
		})
	}

	cardCount := snap.CardVelocity + 1
	if cardCount > e.cfg.VelocityCardLimit {
		score += 50
		triggered = append(triggered, payment.TriggeredRule{
			ID:      "VELOCITY_CARD",
			Message: fmt.Sprintf("card %s has %d txns in window (limit %d)", p.CardID, cardCount, e.cfg.VelocityCardLimit),
		})
	}

	ipCount := snap.IPVelocity + 1
	if ipCount > e.cfg.VelocityIPLimit {
		score += 30
		triggered = append(triggered, payment.TriggeredRule{
			ID:      "VELOCITY_IP",
			Message: fmt.Sprintf("ip %s has %d txns in window (limit %d)", p.IP, ipCount, e.cfg.VelocityIPLimit),
		})
	}

	if snap.HadDispute {
		score += 60
		triggered = append(triggered, payment.TriggeredRule{
			ID:      "HISTORY_DISPUTE",
			Message: fmt.Sprintf("card %s has prior dispute/fraud history", p.CardID),
		})
	}

	decision := payment.DecisionAllow
	switch {
	case score >= e.cfg.DeclineScore:
		decision = payment.DecisionDecline
	case score >= e.cfg.ReviewScore:
		decision = payment.DecisionReview
	}

	if triggered == nil {
		triggered = []payment.TriggeredRule{}
	}

	return payment.ScoreResult{
		TxnID:          p.TxnID,
		Decision:       decision,
		Score:          score,
		TriggeredRules: triggered,
		LatencyMs:      float64(time.Since(start).Microseconds()) / 1000.0,
		ScoredAt:       time.Now().UTC(),
	}
}
