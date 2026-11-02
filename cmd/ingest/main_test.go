package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func post(t *testing.T, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	// nothing reaches the stores: every case is rejected first
	routes("s", nil, nil, nil).ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)))
	return w
}

func TestPostRejectsBadBodies(t *testing.T) {
	big := `{"card_id":"` + strings.Repeat("x", maxBodyBytes) + `"}`
	cases := []struct {
		name, path, body string
		want             int
	}{
		{"oversized payment", "/v1/payments", big, http.StatusRequestEntityTooLarge},
		{"oversized dispute", "/v1/payments/t/dispute", big, http.StatusRequestEntityTooLarge},
		{"not json", "/v1/payments", "{", http.StatusBadRequest},
		{"no card", "/v1/payments", `{"amount":1}`, http.StatusBadRequest},
		{"bad ip", "/v1/payments", `{"card_id":"c","amount":1,"ip":"x"}`, http.StatusBadRequest},
		{"other currency", "/v1/payments", `{"card_id":"c","amount":1,"currency":"JPY"}`, http.StatusBadRequest},
		{"dispute without card", "/v1/payments/t/dispute", `{}`, http.StatusBadRequest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if w := post(t, c.path, c.body); w.Code != c.want {
				t.Fatalf("got %d: %s", w.Code, w.Body)
			}
		})
	}
}

func TestServerTimeoutsAreSet(t *testing.T) {
	s := newServer(":0", nil)
	if s.ReadHeaderTimeout == 0 || s.ReadTimeout == 0 || s.WriteTimeout == 0 || s.IdleTimeout == 0 {
		t.Fatalf("missing timeout: %+v", s)
	}
}
