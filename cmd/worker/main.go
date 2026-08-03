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

	"github.com/mit/fraudguard/internal/config"
	"github.com/mit/fraudguard/internal/engine"
	"github.com/mit/fraudguard/internal/heartbeat"
	"github.com/mit/fraudguard/internal/history"
	"github.com/mit/fraudguard/internal/scorer"
	"github.com/mit/fraudguard/internal/stream"
	"github.com/mit/fraudguard/internal/velocity"
)

func main() {
	cfgPath := flag.String("config", "config/local.yaml", "config file")
	metricsAddr := flag.String("metrics-addr", ":9090", "metrics listen addr")
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
	client := stream.NewClient(stream.Config{
		Region:     cfg.Kinesis.Region,
		StreamName: cfg.Kinesis.StreamName,
		Endpoint:   cfg.Kinesis.Endpoint,
	})
	must(stream.EnsureStream(ctx, client, cfg.Kinesis.StreamName, 2))

	worker := stream.NewWorker(client, cfg.Kinesis.StreamName, cfg.WorkerName, cfg.WorkerCount, svc)
	rep := heartbeat.New(cfg.WorkerName)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		scored, errs, avgMs := worker.Stats()
		rep.Set(scored, errs, avgMs)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(rep.Snapshot())
	})
	go func() {
		slog.Info("metrics listening", "addr", *metricsAddr)
		_ = http.ListenAndServe(*metricsAddr, mux)
	}()

	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				scored, errs, avgMs := worker.Stats()
				rep.Set(scored, errs, avgMs)
				slog.Info("heartbeat", "scored", scored, "errors", errs, "avg_latency_ms", avgMs)
			}
		}
	}()

	slog.Info("worker running", "stream", cfg.Kinesis.StreamName, "pool", cfg.WorkerCount)
	must(worker.Run(ctx))
}

func must(err error) {
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}
