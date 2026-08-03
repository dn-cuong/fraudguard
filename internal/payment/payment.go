package payment

import "time"

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

type Decision string

const (
	DecisionAllow   Decision = "ALLOW"
	DecisionReview  Decision = "REVIEW"
	DecisionDecline Decision = "DECLINE"
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
}
