package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dn-cuong/fraudguard/internal/config"
	"github.com/dn-cuong/fraudguard/internal/engine"
	"github.com/dn-cuong/fraudguard/internal/history"
	"github.com/dn-cuong/fraudguard/internal/payment"
	"github.com/dn-cuong/fraudguard/internal/scorer"
	"github.com/dn-cuong/fraudguard/internal/stream"
	"github.com/dn-cuong/fraudguard/internal/velocity"
	"github.com/google/uuid"
)

const maxBodyBytes = 64 << 10

func main() {
	cfgPath := flag.String("config", "config/local.yaml", "config file")
	mode := flag.String("mode", "score", "score | kinesis")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	must(err)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// History is always needed so clients can poll GET /v1/payments/{txn_id}.
	hist := history.New(cfg.HistoryConfig())
	must(hist.EnsureTable(ctx))

	var svc *scorer.Service
	var producer *stream.Producer

	switch *mode {
	case "kinesis":
		client := stream.NewClient(stream.Config{
			Region:     cfg.Kinesis.Region,
			StreamName: cfg.Kinesis.StreamName,
			Endpoint:   cfg.Kinesis.Endpoint,
		})
		must(stream.EnsureStream(ctx, client, cfg.Kinesis.StreamName, 2))
		producer = stream.NewProducer(client, cfg.Kinesis.StreamName)
	default:
		engCfg, err := cfg.EngineConfig()
		must(err)
		vel := velocity.New(cfg.RedisAddr)
		must(vel.Ping(ctx))
		svc = scorer.New(engine.New(engCfg), vel, hist, engCfg)
	}

	srv := newServer(cfg.HTTPAddr, routes(cfg.Kinesis.StreamName, hist, svc, producer))
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("shutdown", "err", err)
		}
	}()

	slog.Info("ingest listening", "addr", cfg.HTTPAddr, "mode", *mode)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("serve", "err", err)
		os.Exit(1)
	}
	// ListenAndServe returns as soon as Shutdown starts; wait for it to drain
	<-shutdownDone
}

func newServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

func routes(streamName string, hist *history.Store, svc *scorer.Service, producer *stream.Producer) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("POST /v1/payments", func(w http.ResponseWriter, r *http.Request) {
		var p payment.Payment
		if !decodeBody(w, r, &p) {
			return
		}
		if p.TxnID == "" {
			p.TxnID = uuid.NewString()
		}
		if err := p.Validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if p.Currency == "" {
			p.Currency = "USD"
		}
		// velocity windows run on this, so it can't be a client-supplied value
		p.Timestamp = time.Now().UTC()

		if producer != nil {
			if err := producer.Put(r.Context(), p); err != nil {
				serverError(w, "queue payment", err, http.StatusBadGateway)
				return
			}
			writeJSON(w, http.StatusAccepted, map[string]any{
				"status": "queued",
				"txn_id": p.TxnID,
				"poll":   "/v1/payments/" + p.TxnID,
				"stream": streamName,
			})
			return
		}

		res, err := svc.Score(r.Context(), p)
		if err != nil {
			serverError(w, "score payment", err, http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, res)
	})

	mux.HandleFunc("GET /v1/payments/{txn_id}", func(w http.ResponseWriter, r *http.Request) {
		txnID := r.PathValue("txn_id")
		if txnID == "" {
			http.Error(w, "txn_id required", http.StatusBadRequest)
			return
		}
		rec, err := hist.GetByTxnID(r.Context(), txnID)
		if err != nil {
			serverError(w, "read payment", err, http.StatusInternalServerError)
			return
		}
		if rec == nil {
			writeJSON(w, http.StatusOK, map[string]any{
				"status":  "pending",
				"txn_id":  txnID,
				"message": "Payment accepted; score not ready yet. Poll again shortly.",
			})
			return
		}
		if rec.Decision == payment.DecisionError {
			writeJSON(w, http.StatusOK, map[string]any{
				"status":  "failed",
				"txn_id":  rec.TxnID,
				"card_id": rec.CardID,
				"error":   rec.Error,
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":   "scored",
			"txn_id":   rec.TxnID,
			"card_id":  rec.CardID,
			"amount":   rec.Amount,
			"decision": rec.Decision,
			"score":    rec.Score,
			"merchant": rec.Merchant,
			"currency": rec.Currency,
		})
	})

	mux.HandleFunc("POST /v1/payments/{txn_id}/dispute", func(w http.ResponseWriter, r *http.Request) {
		txnID := r.PathValue("txn_id")
		if txnID == "" {
			http.Error(w, "txn_id required", http.StatusBadRequest)
			return
		}
		var body struct {
			CardID string `json:"card_id"`
		}
		if !decodeBody(w, r, &body) {
			return
		}
		if body.CardID == "" {
			http.Error(w, "card_id required", http.StatusBadRequest)
			return
		}
		err := hist.MarkDisputed(r.Context(), body.CardID, txnID)
		if errors.Is(err, history.ErrNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		if err != nil {
			serverError(w, "mark dispute", err, http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "disputed",
			"txn_id":  txnID,
			"card_id": body.CardID,
		})
	})
	return mux
}

// decodeBody reads a size-limited JSON body and answers the request itself when
// it can't. Real payment bodies are a few hundred bytes.
func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(v)
	var tooBig *http.MaxBytesError
	switch {
	case err == nil:
		return true
	case errors.As(err, &tooBig):
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
	default:
		http.Error(w, "invalid json", http.StatusBadRequest)
	}
	return false
}

// serverError logs the cause and keeps internals out of the response.
func serverError(w http.ResponseWriter, what string, err error, code int) {
	slog.Error(what, "err", err)
	http.Error(w, http.StatusText(code), code)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func must(err error) {
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}
