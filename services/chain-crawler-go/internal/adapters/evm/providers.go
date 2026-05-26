package evm

import (
	"strings"
	"time"
)

type ChainDefaults struct {
	FinalityDepth     uint64
	ReorgWindow       uint64
	MaxBlockRange     uint64
	InitialBlockRange uint64
	MinBlockRange     uint64
}

var chainDefaults = map[string]ChainDefaults{
	"ethereum": {
		FinalityDepth:     64,
		ReorgWindow:       128,
		MaxBlockRange:     5_000,
		InitialBlockRange: 1_000,
		MinBlockRange:     1,
	},
	"eth": {
		FinalityDepth:     64,
		ReorgWindow:       128,
		MaxBlockRange:     5_000,
		InitialBlockRange: 1_000,
		MinBlockRange:     1,
	},
	"bsc": {
		FinalityDepth:     20,
		ReorgWindow:       64,
		MaxBlockRange:     5_000,
		InitialBlockRange: 1_000,
		MinBlockRange:     1,
	},
	"binance-smart-chain": {
		FinalityDepth:     20,
		ReorgWindow:       64,
		MaxBlockRange:     5_000,
		InitialBlockRange: 1_000,
		MinBlockRange:     1,
	},
	"polygon": {
		FinalityDepth:     128,
		ReorgWindow:       256,
		MaxBlockRange:     3_000,
		InitialBlockRange: 1_000,
		MinBlockRange:     1,
	},
	"arbitrum": {
		FinalityDepth:     64,
		ReorgWindow:       128,
		MaxBlockRange:     10_000,
		InitialBlockRange: 2_000,
		MinBlockRange:     1,
	},
	"optimism": {
		FinalityDepth:     64,
		ReorgWindow:       128,
		MaxBlockRange:     10_000,
		InitialBlockRange: 2_000,
		MinBlockRange:     1,
	},
	"base": {
		FinalityDepth:     64,
		ReorgWindow:       128,
		MaxBlockRange:     10_000,
		InitialBlockRange: 2_000,
		MinBlockRange:     1,
	},
	"avalanche": {
		FinalityDepth:     8,
		ReorgWindow:       32,
		MaxBlockRange:     2_048,
		InitialBlockRange: 512,
		MinBlockRange:     1,
	},
	"avalanche-c-chain": {
		FinalityDepth:     8,
		ReorgWindow:       32,
		MaxBlockRange:     2_048,
		InitialBlockRange: 512,
		MinBlockRange:     1,
	},
}

func ProviderDefaults(chainName string) (ChainDefaults, bool) {
	defaults, ok := chainDefaults[normalizeChainName(chainName)]
	return defaults, ok
}

func applyConfigDefaults(cfg Config) Config {
	if defaults, ok := ProviderDefaults(cfg.ChainName); ok {
		if cfg.FinalityDepth == 0 {
			cfg.FinalityDepth = defaults.FinalityDepth
		}
		if cfg.ReorgWindow == 0 {
			cfg.ReorgWindow = defaults.ReorgWindow
		}
		if cfg.MaxBlockRange == 0 {
			cfg.MaxBlockRange = defaults.MaxBlockRange
		}
		if cfg.InitialBlockRange == 0 {
			cfg.InitialBlockRange = defaults.InitialBlockRange
		}
		if cfg.MinBlockRange == 0 {
			cfg.MinBlockRange = defaults.MinBlockRange
		}
	}
	if cfg.FinalityDepth == 0 {
		cfg.FinalityDepth = 64
	}
	if cfg.ReorgWindow == 0 {
		cfg.ReorgWindow = cfg.FinalityDepth * 2
		if cfg.ReorgWindow == 0 {
			cfg.ReorgWindow = 128
		}
	}
	if cfg.MaxBlockRange == 0 {
		cfg.MaxBlockRange = 1_000
	}
	if cfg.MinBlockRange == 0 {
		cfg.MinBlockRange = 1
	}
	if cfg.InitialBlockRange == 0 || cfg.InitialBlockRange > cfg.MaxBlockRange {
		cfg.InitialBlockRange = cfg.MaxBlockRange
	}
	if cfg.InitialBlockRange < cfg.MinBlockRange {
		cfg.InitialBlockRange = cfg.MinBlockRange
	}
	if cfg.MaxGetLogsRetries == 0 {
		cfg.MaxGetLogsRetries = 3
	}
	if cfg.GetLogsRetryBackoff == 0 {
		cfg.GetLogsRetryBackoff = 100 * time.Millisecond
	}
	if cfg.GetLogsRetryMaxBackoff == 0 {
		cfg.GetLogsRetryMaxBackoff = 2 * time.Second
	}
	if cfg.AdaptiveGrowFactor == 0 {
		cfg.AdaptiveGrowFactor = 2
	}
	if cfg.ReconnectInitialBackoff == 0 {
		cfg.ReconnectInitialBackoff = 250 * time.Millisecond
	}
	if cfg.ReconnectMaxBackoff == 0 {
		cfg.ReconnectMaxBackoff = 10 * time.Second
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 30 * time.Second
	}
	return cfg
}

func normalizeChainName(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	replacer := strings.NewReplacer("_", "-", " ", "-")
	return replacer.Replace(name)
}
