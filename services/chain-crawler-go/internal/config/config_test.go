package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAppliesCheckpointFileFromConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crawler.json")
	body := []byte(`{
  "checkpoint_file": "state/checkpoints.json",
  "exporter": {"type": "stdout", "queue_size": 128, "backpressure_policy": "slowdown"},
  "rpc": {"endpoints": [{"id": "rpc-1", "url": "mock://local"}]}
}`)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.CheckpointFile != "state/checkpoints.json" {
		t.Fatalf("checkpoint file = %q", cfg.CheckpointFile)
	}
}

func TestLoadAppliesCheckpointFileFromEnv(t *testing.T) {
	t.Setenv("CRAWLER_CHECKPOINT_FILE", "state/env-checkpoints.json")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.CheckpointFile != "state/env-checkpoints.json" {
		t.Fatalf("checkpoint file = %q", cfg.CheckpointFile)
	}
}

func TestValidateRequiresCheckpointFile(t *testing.T) {
	cfg := Default()
	cfg.CheckpointFile = ""

	if err := Validate(cfg); err == nil {
		t.Fatal("expected checkpoint file validation error")
	}
}
