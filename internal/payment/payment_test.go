package payment

import (
	"errors"
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	ok := Payment{CardID: "c", Amount: 1}
	cases := []struct {
		name string
		mut  func(*Payment)
		bad  bool
	}{
		{"minimal", func(*Payment) {}, false},
		{"usd", func(p *Payment) { p.Currency = "USD" }, false},
		{"ipv4", func(p *Payment) { p.IP = "1.2.3.4" }, false},
		{"ipv6", func(p *Payment) { p.IP = "2001:db8::1" }, false},
		{"no card", func(p *Payment) { p.CardID = "" }, true},
		{"zero amount", func(p *Payment) { p.Amount = 0 }, true},
		{"negative amount", func(p *Payment) { p.Amount = -1 }, true},
		{"other currency", func(p *Payment) { p.Currency = "JPY" }, true},
		{"bad ip", func(p *Payment) { p.IP = "not-an-ip" }, true},
		{"long merchant", func(p *Payment) { p.Merchant = strings.Repeat("x", maxFieldLen+1) }, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := ok
			c.mut(&p)
			err := p.Validate()
			if c.bad != (err != nil) {
				t.Fatalf("err = %v", err)
			}
			if c.bad && !errors.Is(err, ErrInvalid) {
				t.Fatalf("want ErrInvalid, got %v", err)
			}
		})
	}
}
