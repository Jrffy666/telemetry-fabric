package filter

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"telemetry-fabric/services/chain-crawler-go/pkg/event"
)

func TestEngineClassifiesCriticalImportantAggregateAndDrop(t *testing.T) {
	engine := newTestEngine(t, RuleSet{
		Version:               "test-v1",
		Mode:                  ModeEnforce,
		DefaultClassification: ClassificationDrop,
		Watchlists: Watchlists{
			Addresses: []WatchEntry{
				{
					Address:        "0x1111111111111111111111111111111111111111",
					Label:          "treasury",
					Classification: ClassificationImportant,
				},
			},
		},
		Rules: []Rule{
			{
				ID:             "large_stablecoin_transfer",
				Enabled:        true,
				Classification: ClassificationCritical,
				ForceKeep:      true,
				Match: Match{
					EventTypes:      []string{"token_transfer"},
					TokenSymbols:    []string{"USDC"},
					MinAmountUSD:    1_000_000,
					HasMinAmountUSD: true,
				},
			},
			{
				ID:             "failed_transaction_aggregate",
				Enabled:        true,
				Classification: ClassificationAggregate,
				Match:          Match{FailedTransaction: boolPtr(true)},
			},
			{
				ID:             "normal_noise_drop",
				Enabled:        true,
				Classification: ClassificationDrop,
				DiscardReason:  "normal_noise",
				Match:          Match{EventTypes: []string{"token_transfer", "native_transfer"}},
			},
		},
	})

	critical := engine.ClassifyEvent(baseEvent())
	if critical.Classification != ClassificationCritical || !critical.Keep {
		t.Fatalf("critical classification = %+v", critical)
	}
	if critical.MatchedRule != "large_stablecoin_transfer" {
		t.Fatalf("critical matched rule = %q", critical.MatchedRule)
	}

	importantEvent := baseEvent()
	importantEvent.AmountUSD = "42"
	importantEvent.TokenSymbol = "NOISE"
	importantEvent.ToAddress = "0x1111111111111111111111111111111111111111"
	important := engine.ClassifyEvent(importantEvent)
	if important.Classification != ClassificationImportant || !important.Keep {
		t.Fatalf("important classification = %+v", important)
	}
	if !containsString(important.MatchedRules, "watchlist.address.treasury") {
		t.Fatalf("important matched rules = %#v", important.MatchedRules)
	}

	aggregateEvent := baseEvent()
	aggregateEvent.AmountUSD = "1"
	aggregateEvent.TokenSymbol = "NOISE"
	aggregateEvent.Success = false
	aggregate := engine.ClassifyEvent(aggregateEvent)
	if aggregate.Classification != ClassificationAggregate || !aggregate.Keep {
		t.Fatalf("aggregate classification = %+v", aggregate)
	}

	dropEvent := baseEvent()
	dropEvent.AmountUSD = "1"
	dropEvent.TokenSymbol = "NOISE"
	drop := engine.ClassifyEvent(dropEvent)
	if drop.Classification != ClassificationDrop || drop.Keep {
		t.Fatalf("drop classification = %+v", drop)
	}
	if drop.DiscardReason != "normal_noise" {
		t.Fatalf("discard reason = %q", drop.DiscardReason)
	}
}

func TestShadowModeClassifiesDropWithoutDiscarding(t *testing.T) {
	engine := newTestEngine(t, RuleSet{
		Version:               "shadow-v1",
		Mode:                  ModeShadow,
		DefaultClassification: ClassificationDrop,
	})

	result := engine.ClassifyEvent(baseEvent())
	if result.Classification != ClassificationDrop {
		t.Fatalf("classification = %s", result.Classification)
	}
	if !result.Keep || !result.ShadowMode {
		t.Fatalf("shadow drop should be kept: %+v", result)
	}
	if result.DiscardReason != "no_event_rule_matched" {
		t.Fatalf("discard reason = %q", result.DiscardReason)
	}
}

func TestYAMLLoadingWatchlistsAndDiscardReason(t *testing.T) {
	path := writeTempRuleFile(t, `
rule_version: yaml-v1
mode: enforce
default_classification: DROP
watchlists:
  addresses:
    - address: "0x1111111111111111111111111111111111111111"
      label: "treasury"
      classification: IMPORTANT
rules:
  - id: normal_noise_drop
    classification: DROP
    discard_reason: normal_noise
    match:
      event_types: [token_transfer]
`)

	rules, err := LoadRuleSet(path)
	if err != nil {
		t.Fatalf("load rules: %v", err)
	}
	if rules.Version != "yaml-v1" {
		t.Fatalf("version = %q", rules.Version)
	}

	engine := newTestEngine(t, rules)
	watched := baseEvent()
	watched.AmountUSD = "10"
	watched.ToAddress = "0x1111111111111111111111111111111111111111"
	result := engine.ClassifyEvent(watched)
	if result.Classification != ClassificationImportant || !result.Keep {
		t.Fatalf("watchlist result = %+v", result)
	}

	noise := baseEvent()
	noise.AmountUSD = "10"
	noise.TokenSymbol = "NOISE"
	result = engine.ClassifyEvent(noise)
	if result.Keep || result.DiscardReason != "normal_noise" {
		t.Fatalf("noise result = %+v", result)
	}
}

func TestContractTokenBlacklistAndGasRules(t *testing.T) {
	engine := newTestEngine(t, RuleSet{
		Version:               "watchlists-v1",
		Mode:                  ModeEnforce,
		DefaultClassification: ClassificationDrop,
		GasAnomaly:            GasAnomalyConfig{MaxGasUsed: 1_000_000},
		Watchlists: Watchlists{
			Contracts: []WatchEntry{
				{
					Address:        "0x2222222222222222222222222222222222222222",
					Label:          "usdc-contract",
					Classification: ClassificationImportant,
				},
			},
			Tokens: []TokenWatchEntry{
				{
					Address:        "0x3333333333333333333333333333333333333333",
					Label:          "usdc",
					MinUSD:         100_000,
					Classification: ClassificationCritical,
				},
			},
		},
		Blacklist: Blacklist{
			Addresses: []string{"0x000000000000000000000000000000000000dead"},
		},
	})

	contractEvent := baseEvent()
	contractEvent.TokenAddress = "0x9999999999999999999999999999999999999999"
	contractEvent.AmountUSD = "10"
	contractResult := engine.ClassifyEvent(contractEvent)
	if contractResult.Classification != ClassificationImportant {
		t.Fatalf("contract watch result = %+v", contractResult)
	}
	if !containsString(contractResult.MatchedRules, "watchlist.contract.usdc-contract") {
		t.Fatalf("contract matched rules = %#v", contractResult.MatchedRules)
	}

	tokenEvent := baseEvent()
	tokenResult := engine.ClassifyEvent(tokenEvent)
	if tokenResult.Classification != ClassificationCritical {
		t.Fatalf("token watch result = %+v", tokenResult)
	}
	if !containsString(tokenResult.MatchedRules, "watchlist.token.usdc") {
		t.Fatalf("token matched rules = %#v", tokenResult.MatchedRules)
	}

	blacklistedEvent := baseEvent()
	blacklistedEvent.TokenAddress = "0x9999999999999999999999999999999999999999"
	blacklistedEvent.FromAddress = "0x000000000000000000000000000000000000dead"
	blacklistedResult := engine.ClassifyEvent(blacklistedEvent)
	if blacklistedResult.Classification != ClassificationCritical || !blacklistedResult.Keep {
		t.Fatalf("blacklist result = %+v", blacklistedResult)
	}
	if blacklistedResult.MatchedRule != "blacklist.address" {
		t.Fatalf("blacklist matched rule = %q", blacklistedResult.MatchedRule)
	}

	gasEvent := baseEvent()
	gasEvent.TokenAddress = "0x9999999999999999999999999999999999999999"
	gasEvent.ContractAddress = "0x9999999999999999999999999999999999999998"
	gasEvent.AmountUSD = "1"
	gasEvent.GasUsed = 1_500_000
	gasResult := engine.ClassifyEvent(gasEvent)
	if gasResult.Classification != ClassificationImportant {
		t.Fatalf("gas anomaly result = %+v", gasResult)
	}
	if gasResult.MatchedRule != "builtin.gas_anomaly" {
		t.Fatalf("gas anomaly matched rule = %q", gasResult.MatchedRule)
	}
}

func TestForcedKeepForReorgAndNodeHealthAnomaly(t *testing.T) {
	engine := newTestEngine(t, RuleSet{
		Version:               "forced-v1",
		Mode:                  ModeEnforce,
		DefaultClassification: ClassificationDrop,
	})

	reorg := baseEvent()
	reorg.Reorged = true
	result := engine.ClassifyEvent(reorg)
	if result.Classification != ClassificationCritical || !result.Keep {
		t.Fatalf("reorg result = %+v", result)
	}
	if result.MatchedRule != "builtin.reorg_forced_keep" {
		t.Fatalf("reorg matched rule = %q", result.MatchedRule)
	}

	health := event.NodeHealth{
		Status:   event.NodeStatusUnhealthy,
		Synced:   false,
		BlockLag: 100,
	}
	result = engine.ClassifyNodeHealth(health)
	if result.Classification != ClassificationCritical || !result.Keep {
		t.Fatalf("node health result = %+v", result)
	}
	if result.MatchedRule != "builtin.node_health_anomaly_forced_keep" {
		t.Fatalf("node health matched rule = %q", result.MatchedRule)
	}
}

func TestPrometheusCounters(t *testing.T) {
	engine := newTestEngine(t, RuleSet{
		Version:               "metrics-v1",
		Mode:                  ModeEnforce,
		DefaultClassification: ClassificationDrop,
	})

	_ = engine.ClassifyEvent(baseEvent())

	var out bytes.Buffer
	if err := engine.WritePrometheus(&out); err != nil {
		t.Fatalf("write prometheus: %v", err)
	}

	text := out.String()
	if !strings.Contains(text, "crawler_filter_events_total") {
		t.Fatalf("missing events counter:\n%s", text)
	}
	if !strings.Contains(text, `classification="DROP"`) {
		t.Fatalf("missing classification label:\n%s", text)
	}
	if !strings.Contains(text, "crawler_filter_discard_reasons_total") {
		t.Fatalf("missing discard reason counter:\n%s", text)
	}
}

func TestRepositoryRuleExamplesLoad(t *testing.T) {
	root := repoRoot(t)
	paths := []string{
		filepath.Join(root, "modules", "blockchain", "rules", "default.yaml"),
		filepath.Join(root, "modules", "blockchain", "rules", "large_transfer.yaml"),
		filepath.Join(root, "modules", "blockchain", "rules", "watched_address.yaml"),
		filepath.Join(root, "modules", "blockchain", "config", "watchlists.example.yaml"),
	}

	for _, path := range paths {
		if _, err := LoadRuleSet(path); err != nil {
			t.Fatalf("load %s: %v", path, err)
		}
	}
}

func baseEvent() event.NormalizedEvent {
	return event.NormalizedEvent{
		Chain:           "ethereum",
		Network:         "mainnet",
		EventType:       "token_transfer",
		FromAddress:     "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ToAddress:       "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ContractAddress: "0x2222222222222222222222222222222222222222",
		TokenAddress:    "0x3333333333333333333333333333333333333333",
		TokenSymbol:     "USDC",
		AmountUSD:       "1000000",
		GasUsed:         21000,
		GasPrice:        "30",
		Success:         true,
	}
}

func newTestEngine(t *testing.T, rules RuleSet) *Engine {
	t.Helper()
	engine, err := NewEngine(rules)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return engine
}

func writeTempRuleFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "rules.yaml")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(content)+"\n"), 0o600); err != nil {
		t.Fatalf("write temp rule file: %v", err)
	}
	return path
}

func repoRoot(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}

	for dir := wd; ; dir = filepath.Dir(dir) {
		path := filepath.Join(dir, "modules", "blockchain", "rules", "default.yaml")
		if _, err := os.Stat(path); err == nil {
			return dir
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat %s: %v", path, err)
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
	}

	t.Fatalf("repository root with modules/blockchain/rules/default.yaml not found from %s", wd)
	return ""
}

func boolPtr(value bool) *bool {
	return &value
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
