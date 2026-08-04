package config

import (
	"fmt"
	"os"
	"time"

	"github.com/mit/fraudguard/internal/engine"
	"github.com/mit/fraudguard/internal/history"
	"gopkg.in/yaml.v3"
)

type Config struct {
	RedisAddr      string      `yaml:"redis_addr"`
	Dynamo         DynamoYAML  `yaml:"dynamo"`
	Rules          RulesYAML   `yaml:"rules"`
	Kinesis        KinesisYAML `yaml:"kinesis"`
	HTTPAddr       string      `yaml:"http_addr"`
	WorkerCount    int         `yaml:"worker_count"`
	WorkerName     string      `yaml:"worker_name"`
	WorkerReplicas int         `yaml:"worker_replicas"`
}

type DynamoYAML struct {
	Region    string `yaml:"region"`
	TableName string `yaml:"table_name"`
	Endpoint  string `yaml:"endpoint"`
}

type RulesYAML struct {
	AmountHighUSD      float64 `yaml:"amount_high_usd"`
	VelocityCardLimit  int64   `yaml:"velocity_card_limit"`
	VelocityCardWindow string  `yaml:"velocity_card_window"`
	VelocityIPLimit    int64   `yaml:"velocity_ip_limit"`
	VelocityIPWindow   string  `yaml:"velocity_ip_window"`
	DeclineScore       int     `yaml:"decline_score"`
	ReviewScore        int     `yaml:"review_score"`
}

type KinesisYAML struct {
	StreamName string `yaml:"stream_name"`
	Region     string `yaml:"region"`
	Endpoint   string `yaml:"endpoint"`
}

func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return Config{}, err
	}
	// back-compat if older yaml still uses consumer_name
	if cfg.WorkerName == "" {
		cfg.WorkerName = envOr("WORKER_NAME", "fraudguard-worker-0")
	}
	if cfg.WorkerReplicas < 1 {
		cfg.WorkerReplicas = 1
	}
	return cfg, nil
}

func Default() Config {
	return Config{
		RedisAddr: envOr("REDIS_ADDR", "localhost:6379"),
		Dynamo: DynamoYAML{
			Region:    envOr("AWS_REGION", "us-east-1"),
			TableName: envOr("DYNAMO_TABLE", "fraudguard-transactions"),
			Endpoint:  envOr("DYNAMO_ENDPOINT", "http://localhost:8000"),
		},
		Rules: RulesYAML{
			AmountHighUSD:      2500,
			VelocityCardLimit:  5,
			VelocityCardWindow: "1m",
			VelocityIPLimit:    20,
			VelocityIPWindow:   "1m",
			DeclineScore:       80,
			ReviewScore:        40,
		},
		Kinesis: KinesisYAML{
			StreamName: envOr("KINESIS_STREAM", "fraudguard-payments"),
			Region:     envOr("AWS_REGION", "us-east-1"),
			Endpoint:   os.Getenv("KINESIS_ENDPOINT"),
		},
		HTTPAddr:       envOr("HTTP_ADDR", ":8080"),
		WorkerCount:    32,
		WorkerName:     envOr("WORKER_NAME", "fraudguard-worker-0"),
		WorkerReplicas: 1,
	}
}

func (c Config) EngineConfig() (engine.Config, error) {
	cardWin, err := time.ParseDuration(c.Rules.VelocityCardWindow)
	if err != nil {
		return engine.Config{}, fmt.Errorf("velocity_card_window: %w", err)
	}
	ipWin, err := time.ParseDuration(c.Rules.VelocityIPWindow)
	if err != nil {
		return engine.Config{}, fmt.Errorf("velocity_ip_window: %w", err)
	}
	return engine.Config{
		AmountHighUSD:      c.Rules.AmountHighUSD,
		VelocityCardLimit:  c.Rules.VelocityCardLimit,
		VelocityCardWindow: cardWin,
		VelocityIPLimit:    c.Rules.VelocityIPLimit,
		VelocityIPWindow:   ipWin,
		DeclineScore:       c.Rules.DeclineScore,
		ReviewScore:        c.Rules.ReviewScore,
	}, nil
}

func (c Config) HistoryConfig() history.Config {
	return history.Config{
		Region:    c.Dynamo.Region,
		TableName: c.Dynamo.TableName,
		Endpoint:  c.Dynamo.Endpoint,
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
