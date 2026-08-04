package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/mit/fraudguard/internal/config"
	"github.com/mit/fraudguard/internal/engine"
	"github.com/mit/fraudguard/internal/history"
	"github.com/mit/fraudguard/internal/payment"
	"github.com/mit/fraudguard/internal/scorer"
	"github.com/mit/fraudguard/internal/stream"
	"github.com/mit/fraudguard/internal/velocity"
)

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

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("POST /v1/payments", func(w http.ResponseWriter, r *http.Request) {
		var p payment.Payment
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if p.TxnID == "" {
			p.TxnID = uuid.NewString()
		}
		if p.CardID == "" || p.Amount <= 0 {
			http.Error(w, "card_id and amount required", http.StatusBadRequest)
			return
		}
		if p.Timestamp.IsZero() {
			p.Timestamp = time.Now().UTC()
		}

		if producer != nil {
			if err := producer.Put(r.Context(), p); err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			writeJSON(w, http.StatusAccepted, map[string]any{
				"status": "queued",
				"txn_id": p.TxnID,
				"poll":   "/v1/payments/" + p.TxnID,
				"stream": cfg.Kinesis.StreamName,
			})
			return
		}

		res, err := svc.Score(r.Context(), p)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
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
			http.Error(w, err.Error(), http.StatusInternalServerError)
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
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.CardID == "" {
			http.Error(w, "card_id required", http.StatusBadRequest)
			return
		}
		if err := hist.MarkDisputed(r.Context(), body.CardID, txnID); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "disputed",
			"txn_id":  txnID,
			"card_id": body.CardID,
		})
	})

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = srv.Shutdown(shutdownCtx)
	}()

	slog.Info("ingest listening", "addr", cfg.HTTPAddr, "mode", *mode)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("serve", "err", err)
		os.Exit(1)
	}
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
