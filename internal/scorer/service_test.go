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
	card, ip       int64
	incrCard, incr int
	getErr         error
}

func (f *fakeVel) GetCard(context.Context, string) (int64, error) { return f.card, f.getErr }
func (f *fakeVel) GetIP(context.Context, string) (int64, error)   { return f.ip, nil }
func (f *fakeVel) IncrCard(context.Context, string, time.Duration) (int64, error) {
	f.incrCard++
	return f.card + 1, nil
}
func (f *fakeVel) IncrIP(context.Context, string, time.Duration) (int64, error) {
	f.incr++
	return f.ip + 1, nil
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

func TestScoreWritesRecordAndBumpsCounters(t *testing.T) {
	v, h := &fakeVel{}, &fakeHist{}
	res, err := newSvc(v, h).Score(context.Background(), payment.Payment{TxnID: "t1", CardID: "c1", Amount: 10, IP: "1.1.1.1"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != payment.DecisionAllow {
		t.Fatalf("want ALLOW, got %s", res.Decision)
	}
	if v.incrCard != 1 || v.incr != 1 {
		t.Fatalf("counters: card=%d ip=%d", v.incrCard, v.incr)
	}
	if len(h.puts) != 1 || h.puts[0].Currency != "USD" || h.puts[0].Timestamp.IsZero() {
		t.Fatalf("unexpected record: %+v", h.puts)
	}
}

func TestScoreUsesSharedSnapshot(t *testing.T) {
	v, h := &fakeVel{card: 5}, &fakeHist{dispute: true}
	res, err := newSvc(v, h).Score(context.Background(), payment.Payment{TxnID: "t2", CardID: "c1", Amount: 10, IP: "1.1.1.1"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != payment.DecisionDecline {
		t.Fatalf("want DECLINE (velocity + dispute), got %s/%d", res.Decision, res.Score)
	}
}

func TestScoreReadErrorWritesNothing(t *testing.T) {
	v, h := &fakeVel{getErr: errors.New("boom")}, &fakeHist{}
	if _, err := newSvc(v, h).Score(context.Background(), payment.Payment{CardID: "c"}); err == nil {
		t.Fatal("want error")
	}
	if v.incrCard != 0 || len(h.puts) != 0 {
		t.Fatal("no writes expected when reads fail")
	}
}

// counters go up before the Put, so a failed Put still leaves them bumped
func TestScorePutFailureLeavesCountersBumped(t *testing.T) {
	v, h := &fakeVel{}, &fakeHist{putErr: errors.New("throttled")}
	if _, err := newSvc(v, h).Score(context.Background(), payment.Payment{CardID: "c", IP: "i"}); err == nil {
		t.Fatal("want error")
	}
	if v.incrCard != 1 {
		t.Fatalf("want card counter bumped once, got %d", v.incrCard)
	}
}
