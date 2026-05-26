package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Chain          string
	Network        string
	ChainID        uint64
	StartHeight    uint64
	BatchSize      uint64
	PollInterval   time.Duration
	HTTPListenAddr string
	CheckpointFile string
	Exporter       ExporterConfig
	RPC            RPCConfig
	RateLimitRPS   float64
	Retry          RetryConfig
}

type ExporterConfig struct {
	Type               string `json:"type"`
	FilePath           string `json:"file_path"`
	QueueSize          int    `json:"queue_size"`
	BackpressurePolicy string `json:"backpressure_policy"`
}

type RPCConfig struct {
	Endpoints []RPCEndpointConfig `json:"endpoints"`
}

type RPCEndpointConfig struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

type RetryConfig struct {
	Attempts         int   `json:"attempts"`
	InitialBackoffMS int64 `json:"initial_backoff_ms"`
	MaxBackoffMS     int64 `json:"max_backoff_ms"`
	RetryBudget      int   `json:"retry_budget"`
	JitterPercent    int   `json:"jitter_percent"`
}

type fileConfig struct {
	Chain          *string         `json:"chain"`
	Network        *string         `json:"network"`
	ChainID        *uint64         `json:"chain_id"`
	StartHeight    *uint64         `json:"start_height"`
	BatchSize      *uint64         `json:"batch_size"`
	PollIntervalMS *int64          `json:"poll_interval_ms"`
	HTTPListenAddr *string         `json:"http_listen_addr"`
	CheckpointFile *string         `json:"checkpoint_file"`
	Exporter       *ExporterConfig `json:"exporter"`
	RPC            *RPCConfig      `json:"rpc"`
	RateLimitRPS   *float64        `json:"rate_limit_rps"`
	Retry          *RetryConfig    `json:"retry"`
}

func Default() Config {
	return Config{
		Chain:          "mockchain",
		Network:        "local",
		ChainID:        31337,
		StartHeight:    1000,
		BatchSize:      5,
		PollInterval:   2 * time.Second,
		HTTPListenAddr: "127.0.0.1:18080",
		CheckpointFile: "crawler-checkpoints.json",
		Exporter: ExporterConfig{
			Type:               "stdout",
			FilePath:           "crawler-events.jsonl",
			QueueSize:          128,
			BackpressurePolicy: "slowdown",
		},
		RPC: RPCConfig{
			Endpoints: []RPCEndpointConfig{{ID: "mock-rpc-1", URL: "mock://local"}},
		},
		RateLimitRPS: 10,
		Retry: RetryConfig{
			Attempts:         3,
			InitialBackoffMS: 100,
			MaxBackoffMS:     2000,
			RetryBudget:      3,
			JitterPercent:    20,
		},
	}
}

func Load(path string) (Config, error) {
	cfg := Default()
	if path != "" {
		if err := applyFile(&cfg, path); err != nil {
			return Config{}, err
		}
	}
	if err := applyEnv(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, Validate(cfg)
}

func applyFile(cfg *Config, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	var raw fileConfig
	if err := json.NewDecoder(file).Decode(&raw); err != nil {
		return err
	}

	if raw.Chain != nil {
		cfg.Chain = *raw.Chain
	}
	if raw.Network != nil {
		cfg.Network = *raw.Network
	}
	if raw.ChainID != nil {
		cfg.ChainID = *raw.ChainID
	}
	if raw.StartHeight != nil {
		cfg.StartHeight = *raw.StartHeight
	}
	if raw.BatchSize != nil {
		cfg.BatchSize = *raw.BatchSize
	}
	if raw.PollIntervalMS != nil {
		cfg.PollInterval = time.Duration(*raw.PollIntervalMS) * time.Millisecond
	}
	if raw.HTTPListenAddr != nil {
		cfg.HTTPListenAddr = *raw.HTTPListenAddr
	}
	if raw.CheckpointFile != nil {
		cfg.CheckpointFile = *raw.CheckpointFile
	}
	if raw.Exporter != nil {
		cfg.Exporter = *raw.Exporter
	}
	if raw.RPC != nil {
		cfg.RPC = *raw.RPC
	}
	if raw.RateLimitRPS != nil {
		cfg.RateLimitRPS = *raw.RateLimitRPS
	}
	if raw.Retry != nil {
		cfg.Retry = *raw.Retry
	}
	return nil
}

func applyEnv(cfg *Config) error {
	var err error
	if value := os.Getenv("CRAWLER_CHAIN"); value != "" {
		cfg.Chain = value
	}
	if value := os.Getenv("CRAWLER_NETWORK"); value != "" {
		cfg.Network = value
	}
	if value := os.Getenv("CRAWLER_CHAIN_ID"); value != "" {
		cfg.ChainID, err = parseUint("CRAWLER_CHAIN_ID", value)
		if err != nil {
			return err
		}
	}
	if value := os.Getenv("CRAWLER_START_HEIGHT"); value != "" {
		cfg.StartHeight, err = parseUint("CRAWLER_START_HEIGHT", value)
		if err != nil {
			return err
		}
	}
	if value := os.Getenv("CRAWLER_BATCH_SIZE"); value != "" {
		cfg.BatchSize, err = parseUint("CRAWLER_BATCH_SIZE", value)
		if err != nil {
			return err
		}
	}
	if value := os.Getenv("CRAWLER_POLL_INTERVAL"); value != "" {
		cfg.PollInterval, err = time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("CRAWLER_POLL_INTERVAL: %w", err)
		}
	}
	if value := os.Getenv("CRAWLER_HTTP_LISTEN"); value != "" {
		cfg.HTTPListenAddr = value
	}
	if value := os.Getenv("CRAWLER_CHECKPOINT_FILE"); value != "" {
		cfg.CheckpointFile = value
	}
	if value := os.Getenv("CRAWLER_EXPORTER"); value != "" {
		cfg.Exporter.Type = value
	}
	if value := os.Getenv("CRAWLER_EXPORT_FILE"); value != "" {
		cfg.Exporter.FilePath = value
	}
	if value := os.Getenv("CRAWLER_EXPORT_QUEUE_SIZE"); value != "" {
		queueSize, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("CRAWLER_EXPORT_QUEUE_SIZE: %w", err)
		}
		cfg.Exporter.QueueSize = queueSize
	}
	if value := os.Getenv("CRAWLER_EXPORT_BACKPRESSURE"); value != "" {
		cfg.Exporter.BackpressurePolicy = value
	}
	if value := os.Getenv("CRAWLER_RATE_LIMIT_RPS"); value != "" {
		cfg.RateLimitRPS, err = strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("CRAWLER_RATE_LIMIT_RPS: %w", err)
		}
	}
	if value := os.Getenv("CRAWLER_RETRY_ATTEMPTS"); value != "" {
		attempts, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("CRAWLER_RETRY_ATTEMPTS: %w", err)
		}
		cfg.Retry.Attempts = attempts
	}
	if value := os.Getenv("CRAWLER_RETRY_BUDGET"); value != "" {
		budget, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("CRAWLER_RETRY_BUDGET: %w", err)
		}
		cfg.Retry.RetryBudget = budget
	}
	return nil
}

func Validate(cfg Config) error {
	if cfg.Chain == "" {
		return errors.New("chain is required")
	}
	if cfg.Network == "" {
		return errors.New("network is required")
	}
	if cfg.BatchSize == 0 {
		return errors.New("batch_size must be greater than zero")
	}
	if cfg.PollInterval <= 0 {
		return errors.New("poll interval must be greater than zero")
	}
	if cfg.HTTPListenAddr == "" {
		return errors.New("http listen address is required")
	}
	if cfg.CheckpointFile == "" {
		return errors.New("checkpoint_file is required")
	}
	if cfg.Exporter.Type == "" {
		return errors.New("exporter type is required")
	}
	switch cfg.Exporter.Type {
	case "stdout":
	case "file":
		if cfg.Exporter.FilePath == "" {
			return errors.New("exporter file_path is required for file exporter")
		}
	default:
		return errors.New("unsupported exporter type")
	}
	if cfg.Exporter.QueueSize <= 0 {
		return errors.New("exporter queue_size must be greater than zero")
	}
	switch cfg.Exporter.BackpressurePolicy {
	case "", "slowdown", "drop", "pause":
	default:
		return errors.New("exporter backpressure_policy must be slowdown, drop, or pause")
	}
	if len(cfg.RPC.Endpoints) == 0 {
		return errors.New("at least one RPC endpoint is required")
	}
	seenEndpoints := make(map[string]struct{}, len(cfg.RPC.Endpoints))
	for i, endpoint := range cfg.RPC.Endpoints {
		if endpoint.ID == "" {
			return fmt.Errorf("rpc endpoint %d id is required", i)
		}
		if endpoint.URL == "" {
			return fmt.Errorf("rpc endpoint %d url is required", i)
		}
		if _, exists := seenEndpoints[endpoint.ID]; exists {
			return fmt.Errorf("rpc endpoint %d id is duplicated", i)
		}
		seenEndpoints[endpoint.ID] = struct{}{}
	}
	if cfg.RateLimitRPS < 0 {
		return errors.New("rate_limit_rps must be zero or greater")
	}
	if cfg.Retry.Attempts <= 0 {
		return errors.New("retry attempts must be greater than zero")
	}
	if cfg.Retry.RetryBudget <= 0 {
		return errors.New("retry budget must be greater than zero")
	}
	if cfg.Retry.RetryBudget > cfg.Retry.Attempts {
		return errors.New("retry budget must not exceed retry attempts")
	}
	if cfg.Retry.InitialBackoffMS <= 0 {
		return errors.New("retry initial_backoff_ms must be greater than zero")
	}
	if cfg.Retry.MaxBackoffMS < cfg.Retry.InitialBackoffMS {
		return errors.New("retry max_backoff_ms must be greater than or equal to initial_backoff_ms")
	}
	if cfg.Retry.JitterPercent < 0 || cfg.Retry.JitterPercent > 100 {
		return errors.New("retry jitter_percent must be between 0 and 100")
	}
	return nil
}

func Summary(cfg Config) map[string]any {
	return map[string]any{
		"chain":                 cfg.Chain,
		"network":               cfg.Network,
		"chain_id":              cfg.ChainID,
		"batch_size":            cfg.BatchSize,
		"poll_interval":         cfg.PollInterval.String(),
		"http_listen_addr":      cfg.HTTPListenAddr,
		"checkpoint_file":       cfg.CheckpointFile,
		"exporter":              cfg.Exporter.Type,
		"export_queue_size":     cfg.Exporter.QueueSize,
		"backpressure_policy":   cfg.Exporter.BackpressurePolicy,
		"rpc_endpoint_count":    len(cfg.RPC.Endpoints),
		"retry_attempts":        cfg.Retry.Attempts,
		"retry_budget":          cfg.Retry.RetryBudget,
		"retry_initial_backoff": cfg.Retry.InitialBackoffMS,
		"retry_max_backoff":     cfg.Retry.MaxBackoffMS,
	}
}

func parseUint(name string, value string) (uint64, error) {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return parsed, nil
}
