package heartbeat

import (
	"sync"
	"time"
)

type Snapshot struct {
	WorkerName   string    `json:"worker_name"`
	ScoredTotal  int64     `json:"scored_total"`
	ErrorTotal   int64     `json:"error_total"`
	AvgLatencyMs float64   `json:"avg_latency_ms"`
	HeartbeatAt  time.Time `json:"heartbeat_at"`
	Alive        bool      `json:"alive"`
}

type Reporter struct {
	mu   sync.RWMutex
	name string
	snap Snapshot
}

func New(name string) *Reporter {
	return &Reporter{
		name: name,
		snap: Snapshot{WorkerName: name, Alive: true, HeartbeatAt: time.Now().UTC()},
	}
}

func (r *Reporter) Set(scored, errs int64, avgLatencyMs float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snap = Snapshot{
		WorkerName:   r.name,
		ScoredTotal:  scored,
		ErrorTotal:   errs,
		AvgLatencyMs: avgLatencyMs,
		HeartbeatAt:  time.Now().UTC(),
		Alive:        true,
	}
}

func (r *Reporter) Snapshot() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.snap
}
