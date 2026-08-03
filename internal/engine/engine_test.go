package engine

import (
	"testing"
	"time"

	"github.com/mit/fraudguard/internal/payment"
)

func TestEvaluateAllow(t *testing.T) {
	e := New(DefaultConfig())
	p := payment.Payment{TxnID: "t1", CardID: "card-1", Amount: 50, IP: "1.1.1.1"}
	res := e.Evaluate(p, Snapshot{})
	if res.Decision != payment.DecisionAllow {
		t.Fatalf("want ALLOW, got %s score=%d", res.Decision, res.Score)
	}
}

func TestEvaluateAmountAndVelocityDecline(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AmountHighUSD = 100
	cfg.VelocityCardLimit = 2
	e := New(cfg)

	p := payment.Payment{TxnID: "t2", CardID: "card-2", Amount: 500, IP: "2.2.2.2"}
	res := e.Evaluate(p, Snapshot{CardVelocity: 2})
	if res.Decision != payment.DecisionDecline {
		t.Fatalf("want DECLINE, got %s score=%d", res.Decision, res.Score)
	}
}

func TestEvaluateDisputeHistory(t *testing.T) {
	e := New(DefaultConfig())
	p := payment.Payment{TxnID: "t3", CardID: "card-3", Amount: 10, IP: "3.3.3.3"}
	res := e.Evaluate(p, Snapshot{HadDispute: true})
	if res.Decision != payment.DecisionReview && res.Decision != payment.DecisionDecline {
		t.Fatalf("want REVIEW or DECLINE, got %s", res.Decision)
	}
	found := false
	for _, r := range res.TriggeredRules {
		if r.ID == "HISTORY_DISPUTE" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected HISTORY_DISPUTE")
	}
}

func TestEvaluateDeterministic(t *testing.T) {
	e := New(DefaultConfig())
	p := payment.Payment{
		TxnID: "t4", CardID: "card-4", Amount: 3000, IP: "4.4.4.4",
		Timestamp: time.Now().UTC(),
	}
	snap := Snapshot{CardVelocity: 6, IPVelocity: 25, HadDispute: true}
	a := e.Evaluate(p, snap)
	b := e.Evaluate(p, snap)
	if a.Decision != b.Decision || a.Score != b.Score || len(a.TriggeredRules) != len(b.TriggeredRules) {
		t.Fatalf("non-deterministic: %+v vs %+v", a, b)
	}
}
