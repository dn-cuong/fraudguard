package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadWorkerReplicasDefault(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	body := []byte(`
redis_addr: "localhost:6379"
dynamo:
  region: "us-east-1"
  table_name: "t"
  endpoint: "http://localhost:8000"
rules:
  amount_high_usd: 2500
  velocity_card_limit: 5
  velocity_card_window: "1m"
  velocity_ip_limit: 20
  velocity_ip_window: "1m"
  decline_score: 80
  review_score: 40
kinesis:
  stream_name: "s"
  region: "us-east-1"
http_addr: ":8080"
worker_name: "fraudguard-worker-0"
`)
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WorkerReplicas != 1 {
		t.Fatalf("want default worker_replicas=1, got %d", cfg.WorkerReplicas)
	}
	eng, err := cfg.EngineConfig()
	if err != nil {
		t.Fatal(err)
	}
	if eng.VelocityCardWindow.String() != "1m0s" {
		t.Fatalf("unexpected card window %v", eng.VelocityCardWindow)
	}
}
