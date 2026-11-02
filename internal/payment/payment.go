package payment

import (
	"errors"
	"fmt"
	"net/netip"
	"time"
)

// ErrInvalid marks a payment that can never be scored, so retrying is pointless.
var ErrInvalid = errors.New("invalid payment")

const maxFieldLen = 128

// Payment is one authorization attempt entering the pipeline.
type Payment struct {
	TxnID     string    `json:"txn_id" dynamodbav:"txn_id"`
	CardID    string    `json:"card_id" dynamodbav:"card_id"`
	UserID    string    `json:"user_id" dynamodbav:"user_id"`
	Amount    float64   `json:"amount" dynamodbav:"amount"`
	Currency  string    `json:"currency" dynamodbav:"currency"`
	Merchant  string    `json:"merchant" dynamodbav:"merchant"`
	Country   string    `json:"country" dynamodbav:"country"`
	IP        string    `json:"ip" dynamodbav:"ip"`
	Timestamp time.Time `json:"timestamp" dynamodbav:"timestamp"`
}

// Validate checks a payment at the trust boundary. Currency is USD only: the
// amount rule compares against a USD threshold and there is no FX conversion.
// An empty currency means USD.
func (p Payment) Validate() error {
	if p.CardID == "" || p.Amount <= 0 {
		return fmt.Errorf("%w: card_id and amount required", ErrInvalid)
	}
	if p.Currency != "" && p.Currency != "USD" {
		return fmt.Errorf("%w: only USD is supported", ErrInvalid)
	}
	if p.IP != "" {
		if _, err := netip.ParseAddr(p.IP); err != nil {
			return fmt.Errorf("%w: ip is not an IP address", ErrInvalid)
		}
	}
	for name, v := range map[string]string{
		"txn_id": p.TxnID, "card_id": p.CardID, "user_id": p.UserID, "merchant": p.Merchant, "country": p.Country,
	} {
		if len(v) > maxFieldLen {
			return fmt.Errorf("%w: %s is longer than %d", ErrInvalid, name, maxFieldLen)
		}
	}
	return nil
}

type Decision string

const (
	DecisionAllow   Decision = "ALLOW"
	DecisionReview  Decision = "REVIEW"
	DecisionDecline Decision = "DECLINE"
	// DecisionError means scoring failed for good; Record.Error says why.
	DecisionError Decision = "ERROR"
)

type TriggeredRule struct {
	ID      string `json:"id"`
	Message string `json:"message"`
}

type ScoreResult struct {
	TxnID          string          `json:"txn_id"`
	Decision       Decision        `json:"decision"`
	Score          int             `json:"score"`
	TriggeredRules []TriggeredRule `json:"triggered_rules"`
	LatencyMs      float64         `json:"latency_ms"`
	ScoredAt       time.Time       `json:"scored_at"`
}

// Record is durable txn history in DynamoDB.
type Record struct {
	TxnID     string    `json:"txn_id" dynamodbav:"txn_id"`
	CardID    string    `json:"card_id" dynamodbav:"card_id"`
	UserID    string    `json:"user_id" dynamodbav:"user_id"`
	Amount    float64   `json:"amount" dynamodbav:"amount"`
	Currency  string    `json:"currency" dynamodbav:"currency"`
	Merchant  string    `json:"merchant" dynamodbav:"merchant"`
	Country   string    `json:"country" dynamodbav:"country"`
	IP        string    `json:"ip" dynamodbav:"ip"`
	Decision  Decision  `json:"decision" dynamodbav:"decision"`
	Score     int       `json:"score" dynamodbav:"score"`
	Timestamp time.Time `json:"timestamp" dynamodbav:"timestamp"`
	Disputed  bool      `json:"disputed" dynamodbav:"disputed"`
	Error     string    `json:"error,omitempty" dynamodbav:"error,omitempty"`
	// DisputeCardID is only set on disputed txns; it keys the sparse dispute-index.
	DisputeCardID string `json:"-" dynamodbav:"dispute_card_id,omitempty"`
}
