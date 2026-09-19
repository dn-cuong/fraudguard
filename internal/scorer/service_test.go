package scorer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mit/fraudguard/internal/engine"
	"github.com/mit/fraudguard/internal/payment"
)

type fakeVel struct {
	card, ip int64 // counts to report, including the txn being scored
	hits     int
	err      error
}

func (f *fakeVel) HitCard(context.Context, string, string, time.Duration) (int64, error) {
	f.hits++
	return f.card, f.err
}
func (f *fakeVel) HitIP(context.Context, string, string, time.Duration) (int64, error) {
	f.hits++
	return f.ip, f.err
}

type fakeHist struct {
	dispute bool
	putErr  error
	puts    []payment.Record
}

func (f *fakeHist) HasDispute(context.Context, string) (bool, error) { return f.dispute, nil }
func (f *fakeHist) Put(_ context.Context, r payment.Record) error {
	if f.putErr != nil {
		return f.putErr
	}
	f.puts = append(f.puts, r)
	return nil
}

func newSvc(v *fakeVel, h *fakeHist) *Service {
	cfg := engine.DefaultConfig()
	return New(engine.New(cfg), v, h, cfg)
}

func TestScoreWritesRecord(t *testing.T) {
	v, h := &fakeVel{card: 1, ip: 1}, &fakeHist{}
	res, err := newSvc(v, h).Score(context.Background(), payment.Payment{TxnID: "t1", CardID: "c1", Amount: 10, IP: "1.1.1.1"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != payment.DecisionAllow {
		t.Fatalf("want ALLOW, got %s", res.Decision)
	}
	if len(h.puts) != 1 || h.puts[0].Currency != "USD" || h.puts[0].Timestamp.IsZero() {
		t.Fatalf("unexpected record: %+v", h.puts)
	}
}

func TestScoreUsesReturnedCounts(t *testing.T) {
	// 6th txn on the card (limit 5) plus a prior dispute
	v, h := &fakeVel{card: 6, ip: 1}, &fakeHist{dispute: true}
	res, err := newSvc(v, h).Score(context.Background(), payment.Payment{TxnID: "t2", CardID: "c1", Amount: 10, IP: "1.1.1.1"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != payment.DecisionDecline {
		t.Fatalf("want DECLINE (velocity + dispute), got %s/%d", res.Decision, res.Score)
	}
}

func TestScoreVelocityErrorWritesNothing(t *testing.T) {
	v, h := &fakeVel{err: errors.New("boom")}, &fakeHist{}
	if _, err := newSvc(v, h).Score(context.Background(), payment.Payment{CardID: "c"}); err == nil {
		t.Fatal("want error")
	}
	if len(h.puts) != 0 {
		t.Fatal("no record expected when velocity fails")
	}
}

func TestScoreRejectsNonUSD(t *testing.T) {
	v, h := &fakeVel{}, &fakeHist{}
	_, err := newSvc(v, h).Score(context.Background(), payment.Payment{CardID: "c", Currency: "JPY", Amount: 3000})
	if !errors.Is(err, ErrUnsupportedCurrency) {
		t.Fatalf("want ErrUnsupportedCurrency, got %v", err)
	}
	if v.hits != 0 {
		t.Fatal("rejected payment must not touch counters")
	}
}

// A failed Put no longer double counts: a retry of the same txn_id hits the
// per-txn marker in Redis and gets the same count back.
func TestScorePutFailureReturnsError(t *testing.T) {
	v, h := &fakeVel{card: 1, ip: 1}, &fakeHist{putErr: errors.New("throttled")}
	if _, err := newSvc(v, h).Score(context.Background(), payment.Payment{CardID: "c", IP: "i"}); err == nil {
		t.Fatal("want error")
	}
}

func TestRecordFailureStoresErrorDecision(t *testing.T) {
	h := &fakeHist{}
	err := newSvc(&fakeVel{}, h).RecordFailure(context.Background(), payment.Payment{TxnID: "t9", CardID: "c"}, errors.New("redis down"))
	if err != nil {
		t.Fatal(err)
	}
	if len(h.puts) != 1 || h.puts[0].Decision != payment.DecisionError || h.puts[0].Error != "redis down" {
		t.Fatalf("unexpected record: %+v", h.puts)
	}
}
