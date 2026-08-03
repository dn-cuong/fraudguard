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
	engCfg, err := cfg.EngineConfig()
	must(err)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	vel := velocity.New(cfg.RedisAddr)
	must(vel.Ping(ctx))
	hist := history.New(cfg.HistoryConfig())
	must(hist.EnsureTable(ctx))

	svc := scorer.New(engine.New(engCfg), vel, hist, engCfg)

	var producer *stream.Producer
	if *mode == "kinesis" {
		client := stream.NewClient(stream.Config{
			Region:     cfg.Kinesis.Region,
			StreamName: cfg.Kinesis.StreamName,
			Endpoint:   cfg.Kinesis.Endpoint,
		})
		must(stream.EnsureStream(ctx, client, cfg.Kinesis.StreamName, 2))
		producer = stream.NewProducer(client, cfg.Kinesis.StreamName)
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
				"txn_id": p.TxnID,
				"status": "queued",
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
