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

	"github.com/dn-cuong/fraudguard/internal/cloudwatchx"
	"github.com/dn-cuong/fraudguard/internal/config"
	"github.com/dn-cuong/fraudguard/internal/engine"
	"github.com/dn-cuong/fraudguard/internal/heartbeat"
	"github.com/dn-cuong/fraudguard/internal/history"
	"github.com/dn-cuong/fraudguard/internal/lease"
	"github.com/dn-cuong/fraudguard/internal/scorer"
	"github.com/dn-cuong/fraudguard/internal/stream"
	"github.com/dn-cuong/fraudguard/internal/velocity"
	"github.com/google/uuid"
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

	leases := lease.New(history.NewClient(cfg.HistoryConfig()), cfg.LeaseTable, 30*time.Second)
	must(leases.EnsureTable(ctx))
	// unique per process: a restarted worker must not be mistaken for the old one
	owner := cfg.WorkerName + "-" + uuid.NewString()[:8]
	worker := stream.NewConsumer(client, cfg.Kinesis.StreamName, owner, cfg.WorkerCount, leases, svc)
	rep := heartbeat.New(cfg.WorkerName)
	cw := cloudwatchx.New(ctx, cfg.Kinesis.Region, cfg.WorkerName)

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
	metrics := &http.Server{
		Addr:              *metricsAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		slog.Info("metrics listening", "addr", *metricsAddr)
		if err := metrics.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics serve", "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = metrics.Shutdown(shutdownCtx)
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
				cw.Beat(ctx)
				slog.Info("heartbeat", "scored", scored, "errors", errs, "avg_latency_ms", avgMs, "shards", worker.Owned())
			}
		}
	}()

	slog.Info("worker running",
		"stream", cfg.Kinesis.StreamName,
		"pool", cfg.WorkerCount,
		"name", cfg.WorkerName,
		"owner", owner,
	)
	must(worker.Run(ctx))
}

func must(err error) {
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}
