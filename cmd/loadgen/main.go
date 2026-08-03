package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/mit/fraudguard/internal/payment"
)

func main() {
	addr := flag.String("addr", "http://localhost:8080", "ingest base URL")
	tps := flag.Int("tps", 1000, "target TPS")
	duration := flag.Duration("duration", 15*time.Second, "run duration")
	workers := flag.Int("workers", 64, "HTTP concurrency")
	amount := flag.Float64("amount", 42.0, "payment amount")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	client := &http.Client{Timeout: 5 * time.Second}
	jobs := make(chan struct{}, *workers*4)

	var okCount, errCount atomic.Int64
	var latencies []float64
	var latMu sync.Mutex

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for range jobs {
				start := time.Now()
				p := payment.Payment{
					TxnID:    uuid.NewString(),
					CardID:   fmt.Sprintf("card-%d", id%200),
					UserID:   fmt.Sprintf("user-%d", id%500),
					Amount:   *amount,
					Currency: "USD",
					Merchant: "loadgen",
					Country:  "US",
					IP:       fmt.Sprintf("10.0.%d.%d", id%50, id%200),
				}
				body, _ := json.Marshal(p)
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, *addr+"/v1/payments", bytes.NewReader(body))
				if err != nil {
					errCount.Add(1)
					continue
				}
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				elapsed := float64(time.Since(start).Microseconds()) / 1000.0
				if err != nil {
					errCount.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode >= 300 {
					errCount.Add(1)
					continue
				}
				okCount.Add(1)
				latMu.Lock()
				latencies = append(latencies, elapsed)
				latMu.Unlock()
			}
		}(i)
	}

	ticker := time.NewTicker(time.Second / time.Duration(*tps))
	defer ticker.Stop()
	deadline := time.Now().Add(*duration)
	slog.Info("loadgen start", "tps", *tps, "duration", duration.String())

	produced := 0
loop:
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
			select {
			case jobs <- struct{}{}:
				produced++
			default:
			}
		}
	}
	close(jobs)
	wg.Wait()

	latMu.Lock()
	sort.Float64s(latencies)
	p50 := percentile(latencies, 50)
	p99 := percentile(latencies, 99)
	latMu.Unlock()

	achieved := float64(okCount.Load()) / duration.Seconds()
	fmt.Printf("\n=== loadgen ===\n")
	fmt.Printf("target_tps:     %d\n", *tps)
	fmt.Printf("queued:         %d\n", produced)
	fmt.Printf("ok:             %d\n", okCount.Load())
	fmt.Printf("errors:         %d\n", errCount.Load())
	fmt.Printf("achieved_tps:   %.1f\n", achieved)
	fmt.Printf("p50_latency_ms: %.2f\n", p50)
	fmt.Printf("p99_latency_ms: %.2f\n", p99)
	if p50 < 80 && achieved >= float64(*tps)*0.8 {
		fmt.Printf("result:         PASS\n")
	} else {
		fmt.Printf("result:         CHECK\n")
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
