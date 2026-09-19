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
	if res.Score != 0 {
		t.Fatalf("want score 0, got %d", res.Score)
	}
	if len(res.TriggeredRules) != 0 {
		t.Fatalf("want no rules, got %+v", res.TriggeredRules)
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

func TestEvaluateReviewThreshold(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AmountHighUSD = 100
	cfg.ReviewScore = 40
	cfg.DeclineScore = 80
	e := New(cfg)

	// AMOUNT_HIGH alone = 40 → REVIEW
	res := e.Evaluate(
		payment.Payment{TxnID: "t5", CardID: "c", Amount: 100, IP: "1.1.1.1"},
		Snapshot{},
	)
	if res.Decision != payment.DecisionReview || res.Score != 40 {
		t.Fatalf("want REVIEW/40, got %s/%d", res.Decision, res.Score)
	}
}

func TestEvaluateDeclineThreshold(t *testing.T) {
	e := New(DefaultConfig())
	// HISTORY_DISPUTE (60) + AMOUNT_HIGH (40) = 100 → DECLINE
	res := e.Evaluate(
		payment.Payment{TxnID: "t6", CardID: "c", Amount: 2500, IP: "1.1.1.1"},
		Snapshot{HadDispute: true},
	)
	if res.Decision != payment.DecisionDecline || res.Score < 80 {
		t.Fatalf("want DECLINE >=80, got %s/%d", res.Decision, res.Score)
	}
}

func TestEvaluateIPVelocity(t *testing.T) {
	cfg := DefaultConfig()
	cfg.VelocityIPLimit = 2
	e := New(cfg)
	res := e.Evaluate(
		payment.Payment{TxnID: "t7", CardID: "c", Amount: 10, IP: "9.9.9.9"},
		Snapshot{IPVelocity: 2},
	)
	found := false
	for _, r := range res.TriggeredRules {
		if r.ID == "VELOCITY_IP" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected VELOCITY_IP")
	}
	if res.Decision != payment.DecisionAllow {
		// score 30 < review 40
		t.Fatalf("want ALLOW for IP-only, got %s score=%d", res.Decision, res.Score)
	}
}

func TestEvaluateBoundaries(t *testing.T) {
	cfg := DefaultConfig() // amount 2500, card limit 5, ip limit 20
	e := New(cfg)
	has := func(res payment.ScoreResult, id string) bool {
		for _, r := range res.TriggeredRules {
			if r.ID == id {
				return true
			}
		}
		return false
	}
	cases := []struct {
		name   string
		amount float64
		snap   Snapshot
		rule   string
		want   bool
	}{
		{"amount just under", 2499.99, Snapshot{}, "AMOUNT_HIGH", false},
		{"amount at threshold", 2500, Snapshot{}, "AMOUNT_HIGH", true},
		{"card: this txn is the 5th", 10, Snapshot{CardVelocity: 4}, "VELOCITY_CARD", false},
		{"card: this txn is the 6th", 10, Snapshot{CardVelocity: 5}, "VELOCITY_CARD", true},
		{"ip: this txn is the 20th", 10, Snapshot{IPVelocity: 19}, "VELOCITY_IP", false},
		{"ip: this txn is the 21st", 10, Snapshot{IPVelocity: 20}, "VELOCITY_IP", true},
	}
	for _, tc := range cases {
		res := e.Evaluate(payment.Payment{CardID: "c", IP: "i", Amount: tc.amount}, tc.snap)
		if got := has(res, tc.rule); got != tc.want {
			t.Errorf("%s: %s triggered=%v, want %v", tc.name, tc.rule, got, tc.want)
		}
	}
}

func TestEvaluateScoreCutoffs(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DeclineScore, cfg.ReviewScore = 50, 40 // card velocity alone = 50
	e := New(cfg)
	res := e.Evaluate(payment.Payment{CardID: "c"}, Snapshot{CardVelocity: 5})
	if res.Decision != payment.DecisionDecline || res.Score != 50 {
		t.Fatalf("score == DeclineScore must decline, got %s/%d", res.Decision, res.Score)
	}
}
