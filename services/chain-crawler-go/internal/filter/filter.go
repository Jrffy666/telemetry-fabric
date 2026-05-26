package filter

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"

	"telemetry-fabric/services/chain-crawler-go/pkg/event"
)

type Classification string

const (
	ClassificationCritical  Classification = "CRITICAL"
	ClassificationImportant Classification = "IMPORTANT"
	ClassificationAggregate Classification = "AGGREGATE"
	ClassificationDrop      Classification = "DROP"
)

type Mode string

const (
	ModeShadow  Mode = "shadow"
	ModeEnforce Mode = "enforce"
)

type Decision struct {
	Keep           bool           `json:"keep"`
	Reason         string         `json:"reason,omitempty"`
	Classification Classification `json:"classification"`
	MatchedRule    string         `json:"matched_rule,omitempty"`
	MatchedRules   []string       `json:"matched_rules,omitempty"`
	DiscardReason  string         `json:"discard_reason,omitempty"`
	RuleVersion    string         `json:"rule_version,omitempty"`
	ShadowMode     bool           `json:"shadow_mode"`
}

type Result = Decision

type Filter interface {
	Apply(evt event.NormalizedEvent) Decision
}

type AllowAll struct{}

func (AllowAll) Apply(event.NormalizedEvent) Decision {
	return Decision{
		Keep:           true,
		Reason:         "allow_all",
		Classification: ClassificationImportant,
		MatchedRule:    "allow_all",
		MatchedRules:   []string{"allow_all"},
	}
}

type RuleSet struct {
	Version               string
	Mode                  Mode
	DefaultClassification Classification
	LargeTransferUSD      float64
	GasAnomaly            GasAnomalyConfig
	Watchlists            Watchlists
	Blacklist             Blacklist
	Rules                 []Rule
}

type GasAnomalyConfig struct {
	MaxGasUsed      uint64
	MaxGasPriceGwei float64
}

type Watchlists struct {
	Addresses []WatchEntry
	Contracts []WatchEntry
	Tokens    []TokenWatchEntry
}

type Blacklist struct {
	Addresses []string
	Contracts []string
	Tokens    []string
}

type WatchEntry struct {
	Address        string
	Label          string
	Classification Classification
	MinUSD         float64
}

type TokenWatchEntry struct {
	Address        string
	Symbol         string
	Label          string
	Classification Classification
	MinUSD         float64
}

type Rule struct {
	ID             string
	Description    string
	Enabled        bool
	Priority       int
	Classification Classification
	ForceKeep      bool
	DiscardReason  string
	Match          Match
}

type Match struct {
	EventTypes         []string
	Addresses          []string
	Contracts          []string
	Tokens             []string
	TokenSymbols       []string
	NodeStatuses       []event.NodeStatus
	MinAmountUSD       float64
	HasMinAmountUSD    bool
	FailedTransaction  *bool
	Reorged            *bool
	MinGasUsed         uint64
	HasMinGasUsed      bool
	MinGasPriceGwei    float64
	HasMinGasPriceGwei bool
	MinBlockLag        uint64
	HasMinBlockLag     bool
	NodeAnomaly        *bool
}

type Engine struct {
	rules   RuleSet
	metrics *Counters
}

func NewEngine(rules RuleSet) (*Engine, error) {
	normalized, err := normalizeRuleSet(rules)
	if err != nil {
		return nil, err
	}
	return &Engine{rules: normalized, metrics: NewCounters()}, nil
}

func (e *Engine) Apply(evt event.NormalizedEvent) Decision {
	return e.ClassifyEvent(evt)
}

func (e *Engine) ClassifyEvent(evt event.NormalizedEvent) Result {
	matches := make([]ruleMatch, 0, 4)

	if isReorgEvent(evt) {
		matches = append(matches, ruleMatch{
			id:             "builtin.reorg_forced_keep",
			classification: ClassificationCritical,
			forceKeep:      true,
		})
	}

	matches = append(matches, e.matchBlacklists(evt)...)
	matches = append(matches, e.matchWatchlists(evt)...)
	matches = append(matches, e.matchBuiltins(evt)...)

	for _, rule := range e.rules.Rules {
		if !rule.Enabled || !matchEventRule(rule.Match, evt) {
			continue
		}
		matches = append(matches, ruleMatch{
			id:             rule.ID,
			classification: rule.Classification,
			priority:       rule.Priority,
			forceKeep:      rule.ForceKeep,
			discardReason:  rule.DiscardReason,
		})
	}

	result := e.finalize(matches, "no_event_rule_matched")
	e.metrics.Observe(result)
	return result
}

func (e *Engine) ClassifyNodeHealth(health event.NodeHealth) Result {
	matches := make([]ruleMatch, 0, 2)

	if isNodeHealthAnomaly(health) {
		matches = append(matches, ruleMatch{
			id:             "builtin.node_health_anomaly_forced_keep",
			classification: ClassificationCritical,
			forceKeep:      true,
		})
	}

	for _, rule := range e.rules.Rules {
		if !rule.Enabled || !matchNodeHealthRule(rule.Match, health) {
			continue
		}
		matches = append(matches, ruleMatch{
			id:             rule.ID,
			classification: rule.Classification,
			priority:       rule.Priority,
			forceKeep:      rule.ForceKeep,
			discardReason:  rule.DiscardReason,
		})
	}

	result := e.finalize(matches, "no_node_health_rule_matched")
	e.metrics.Observe(result)
	return result
}

func (e *Engine) WritePrometheus(w io.Writer) error {
	return e.metrics.WritePrometheus(w)
}

func (e *Engine) Snapshot() CounterSnapshot {
	return e.metrics.Snapshot()
}

func (e *Engine) finalize(matches []ruleMatch, defaultDiscardReason string) Result {
	if len(matches) == 0 {
		matches = append(matches, ruleMatch{
			id:             "default",
			classification: e.rules.DefaultClassification,
			discardReason:  defaultDiscardReason,
		})
	}

	sort.SliceStable(matches, func(i, j int) bool {
		left := matches[i]
		right := matches[j]
		if left.forceKeep != right.forceKeep {
			return left.forceKeep
		}
		if severity(left.classification) != severity(right.classification) {
			return severity(left.classification) > severity(right.classification)
		}
		return left.priority < right.priority
	})

	best := matches[0]
	matchedRules := make([]string, 0, len(matches))
	seen := make(map[string]struct{}, len(matches))
	for _, match := range matches {
		if _, ok := seen[match.id]; ok {
			continue
		}
		seen[match.id] = struct{}{}
		matchedRules = append(matchedRules, match.id)
	}

	result := Result{
		Keep:           true,
		Classification: best.classification,
		MatchedRule:    best.id,
		MatchedRules:   matchedRules,
		RuleVersion:    e.rules.Version,
		ShadowMode:     e.rules.Mode == ModeShadow,
	}

	if result.Classification == ClassificationDrop {
		result.DiscardReason = best.discardReason
		if result.DiscardReason == "" {
			result.DiscardReason = defaultDiscardReason
		}
		result.Reason = result.DiscardReason
		if e.rules.Mode == ModeEnforce {
			result.Keep = false
		}
		return result
	}

	result.Reason = strings.ToLower(string(result.Classification))
	return result
}

type ruleMatch struct {
	id             string
	classification Classification
	priority       int
	forceKeep      bool
	discardReason  string
}

func (e *Engine) matchBlacklists(evt event.NormalizedEvent) []ruleMatch {
	matches := make([]ruleMatch, 0, 3)
	addresses := eventAddresses(evt)

	for _, address := range e.rules.Blacklist.Addresses {
		if containsAddress(addresses, address) {
			matches = append(matches, ruleMatch{
				id:             "blacklist.address",
				classification: ClassificationCritical,
				forceKeep:      true,
			})
			break
		}
	}

	if containsAddress([]string{evt.ContractAddress}, e.rules.Blacklist.Contracts...) {
		matches = append(matches, ruleMatch{
			id:             "blacklist.contract",
			classification: ClassificationCritical,
			forceKeep:      true,
		})
	}

	if containsAddress([]string{evt.TokenAddress}, e.rules.Blacklist.Tokens...) {
		matches = append(matches, ruleMatch{
			id:             "blacklist.token",
			classification: ClassificationCritical,
			forceKeep:      true,
		})
	}

	return matches
}

func (e *Engine) matchWatchlists(evt event.NormalizedEvent) []ruleMatch {
	matches := make([]ruleMatch, 0, 3)
	addresses := eventAddresses(evt)
	amountUSD := parseAmount(evt.AmountUSD)

	for _, watch := range e.rules.Watchlists.Addresses {
		if containsAddress(addresses, watch.Address) {
			matches = append(matches, ruleMatch{
				id:             labeledRule("watchlist.address", watch.Label, watch.Address),
				classification: watch.classificationOrDefault(ClassificationImportant),
			})
		}
	}

	for _, watch := range e.rules.Watchlists.Contracts {
		if containsAddress([]string{evt.ContractAddress}, watch.Address) {
			matches = append(matches, ruleMatch{
				id:             labeledRule("watchlist.contract", watch.Label, watch.Address),
				classification: watch.classificationOrDefault(ClassificationImportant),
			})
		}
	}

	for _, watch := range e.rules.Watchlists.Tokens {
		if watch.MinUSD > 0 && amountUSD < watch.MinUSD {
			continue
		}
		if watch.Address != "" && containsAddress([]string{evt.TokenAddress}, watch.Address) {
			matches = append(matches, ruleMatch{
				id:             labeledRule("watchlist.token", watch.Label, watch.Address),
				classification: watch.classificationOrDefault(ClassificationImportant),
			})
			continue
		}
		if watch.Symbol != "" && strings.EqualFold(watch.Symbol, evt.TokenSymbol) {
			matches = append(matches, ruleMatch{
				id:             labeledRule("watchlist.token", watch.Label, watch.Symbol),
				classification: watch.classificationOrDefault(ClassificationImportant),
			})
		}
	}

	return matches
}

func (e *Engine) matchBuiltins(evt event.NormalizedEvent) []ruleMatch {
	matches := make([]ruleMatch, 0, 3)

	if e.rules.LargeTransferUSD > 0 && parseAmount(evt.AmountUSD) >= e.rules.LargeTransferUSD {
		matches = append(matches, ruleMatch{
			id:             "builtin.large_transfer",
			classification: ClassificationImportant,
		})
	}

	if !evt.Success {
		matches = append(matches, ruleMatch{
			id:             "builtin.failed_transaction",
			classification: ClassificationAggregate,
		})
	}

	if e.rules.GasAnomaly.MaxGasUsed > 0 && evt.GasUsed >= e.rules.GasAnomaly.MaxGasUsed {
		matches = append(matches, ruleMatch{
			id:             "builtin.gas_anomaly",
			classification: ClassificationImportant,
		})
	}

	if e.rules.GasAnomaly.MaxGasPriceGwei > 0 &&
		parseAmount(evt.GasPrice) >= e.rules.GasAnomaly.MaxGasPriceGwei {
		matches = append(matches, ruleMatch{
			id:             "builtin.gas_price_anomaly",
			classification: ClassificationImportant,
		})
	}

	return matches
}

func matchEventRule(match Match, evt event.NormalizedEvent) bool {
	if len(match.EventTypes) > 0 && !containsFold(match.EventTypes, evt.EventType) {
		return false
	}
	if len(match.Addresses) > 0 && !containsAddress(eventAddresses(evt), match.Addresses...) {
		return false
	}
	if len(match.Contracts) > 0 && !containsAddress([]string{evt.ContractAddress}, match.Contracts...) {
		return false
	}
	if len(match.Tokens) > 0 && !containsAddress([]string{evt.TokenAddress}, match.Tokens...) {
		return false
	}
	if len(match.TokenSymbols) > 0 && !containsFold(match.TokenSymbols, evt.TokenSymbol) {
		return false
	}
	if match.HasMinAmountUSD && parseAmount(evt.AmountUSD) < match.MinAmountUSD {
		return false
	}
	if match.FailedTransaction != nil && (*match.FailedTransaction) != !evt.Success {
		return false
	}
	if match.Reorged != nil && (*match.Reorged) != isReorgEvent(evt) {
		return false
	}
	if match.HasMinGasUsed && evt.GasUsed < match.MinGasUsed {
		return false
	}
	if match.HasMinGasPriceGwei && parseAmount(evt.GasPrice) < match.MinGasPriceGwei {
		return false
	}
	return true
}

func matchNodeHealthRule(match Match, health event.NodeHealth) bool {
	if len(match.EventTypes) > 0 && !containsFold(match.EventTypes, "node_health") {
		return false
	}
	if len(match.NodeStatuses) > 0 {
		found := false
		for _, status := range match.NodeStatuses {
			if status == health.Status {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if match.HasMinBlockLag && health.BlockLag < match.MinBlockLag {
		return false
	}
	if match.NodeAnomaly != nil && (*match.NodeAnomaly) != isNodeHealthAnomaly(health) {
		return false
	}
	return len(match.NodeStatuses) > 0 || match.HasMinBlockLag || match.NodeAnomaly != nil
}

func isReorgEvent(evt event.NormalizedEvent) bool {
	return evt.Reorged ||
		evt.FinalityStatus == event.FinalityStatusReorged ||
		strings.EqualFold(evt.EventType, "reorg") ||
		strings.EqualFold(evt.EventType, "reorg_event")
}

func isNodeHealthAnomaly(health event.NodeHealth) bool {
	if !health.Synced || health.LastError != "" || len(health.Warnings) > 0 {
		return true
	}
	if health.Status == event.NodeStatusDegraded ||
		health.Status == event.NodeStatusUnhealthy ||
		health.Status == event.NodeStatusOffline {
		return true
	}
	return health.ErrorRate >= 0.1
}

func normalizeRuleSet(rules RuleSet) (RuleSet, error) {
	if rules.Version == "" {
		rules.Version = "unversioned"
	}
	if rules.Mode == "" {
		rules.Mode = ModeShadow
	}
	if rules.Mode != ModeShadow && rules.Mode != ModeEnforce {
		return RuleSet{}, fmt.Errorf("invalid filter mode %q", rules.Mode)
	}
	if rules.DefaultClassification == "" {
		rules.DefaultClassification = ClassificationDrop
	}
	if err := validateClassification(rules.DefaultClassification); err != nil {
		return RuleSet{}, err
	}

	for index := range rules.Watchlists.Addresses {
		rules.Watchlists.Addresses[index].Address = normalizeAddress(rules.Watchlists.Addresses[index].Address)
		if rules.Watchlists.Addresses[index].Classification == "" {
			rules.Watchlists.Addresses[index].Classification = ClassificationImportant
		}
		if err := validateClassification(rules.Watchlists.Addresses[index].Classification); err != nil {
			return RuleSet{}, err
		}
	}
	for index := range rules.Watchlists.Contracts {
		rules.Watchlists.Contracts[index].Address = normalizeAddress(rules.Watchlists.Contracts[index].Address)
		if rules.Watchlists.Contracts[index].Classification == "" {
			rules.Watchlists.Contracts[index].Classification = ClassificationImportant
		}
		if err := validateClassification(rules.Watchlists.Contracts[index].Classification); err != nil {
			return RuleSet{}, err
		}
	}
	for index := range rules.Watchlists.Tokens {
		rules.Watchlists.Tokens[index].Address = normalizeAddress(rules.Watchlists.Tokens[index].Address)
		if rules.Watchlists.Tokens[index].Classification == "" {
			rules.Watchlists.Tokens[index].Classification = ClassificationImportant
		}
		if err := validateClassification(rules.Watchlists.Tokens[index].Classification); err != nil {
			return RuleSet{}, err
		}
	}

	rules.Blacklist.Addresses = normalizeAddresses(rules.Blacklist.Addresses)
	rules.Blacklist.Contracts = normalizeAddresses(rules.Blacklist.Contracts)
	rules.Blacklist.Tokens = normalizeAddresses(rules.Blacklist.Tokens)

	for index := range rules.Rules {
		rule := &rules.Rules[index]
		if rule.ID == "" {
			return RuleSet{}, fmt.Errorf("rule at index %d has empty id", index)
		}
		if rule.Classification == "" {
			rule.Classification = ClassificationImportant
		}
		if err := validateClassification(rule.Classification); err != nil {
			return RuleSet{}, fmt.Errorf("rule %s: %w", rule.ID, err)
		}
		if !rule.Enabled {
			continue
		}
		rule.Match.Addresses = normalizeAddresses(rule.Match.Addresses)
		rule.Match.Contracts = normalizeAddresses(rule.Match.Contracts)
		rule.Match.Tokens = normalizeAddresses(rule.Match.Tokens)
	}

	return rules, nil
}

func validateClassification(classification Classification) error {
	switch classification {
	case ClassificationCritical, ClassificationImportant, ClassificationAggregate, ClassificationDrop:
		return nil
	default:
		return fmt.Errorf("invalid classification %q", classification)
	}
}

func severity(classification Classification) int {
	switch classification {
	case ClassificationCritical:
		return 4
	case ClassificationImportant:
		return 3
	case ClassificationAggregate:
		return 2
	case ClassificationDrop:
		return 1
	default:
		return 0
	}
}

func eventAddresses(evt event.NormalizedEvent) []string {
	return []string{evt.FromAddress, evt.ToAddress, evt.ContractAddress, evt.TokenAddress}
}

func containsAddress(values []string, candidates ...string) bool {
	normalized := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = normalizeAddress(value)
		if value == "" {
			continue
		}
		normalized[value] = struct{}{}
	}
	for _, candidate := range candidates {
		if _, ok := normalized[normalizeAddress(candidate)]; ok {
			return true
		}
	}
	return false
}

func containsFold(values []string, candidate string) bool {
	for _, value := range values {
		if strings.EqualFold(value, candidate) {
			return true
		}
	}
	return false
}

func normalizeAddresses(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if normalized := normalizeAddress(value); normalized != "" {
			out = append(out, normalized)
		}
	}
	return out
}

func normalizeAddress(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func parseAmount(value string) float64 {
	value = strings.TrimSpace(strings.ReplaceAll(value, ",", ""))
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0
	}
	return parsed
}

func labeledRule(prefix string, label string, fallback string) string {
	label = strings.TrimSpace(label)
	if label == "" {
		label = strings.TrimSpace(fallback)
	}
	label = strings.ToLower(strings.ReplaceAll(label, " ", "_"))
	if label == "" {
		return prefix
	}
	return prefix + "." + label
}

func (w WatchEntry) classificationOrDefault(defaultValue Classification) Classification {
	if w.Classification == "" {
		return defaultValue
	}
	return w.Classification
}

func (w TokenWatchEntry) classificationOrDefault(defaultValue Classification) Classification {
	if w.Classification == "" {
		return defaultValue
	}
	return w.Classification
}

type Counters struct {
	mu     sync.RWMutex
	values map[string]uint64
	labels map[string]map[string]string
}

type CounterSnapshot map[string]uint64

func NewCounters() *Counters {
	return &Counters{
		values: make(map[string]uint64),
		labels: make(map[string]map[string]string),
	}
}

func (c *Counters) Observe(result Result) {
	decision := "kept"
	if !result.Keep {
		decision = "dropped"
	} else if result.Classification == ClassificationDrop && result.ShadowMode {
		decision = "shadow_drop"
	}

	c.inc("crawler_filter_events_total", map[string]string{
		"classification": string(result.Classification),
		"decision":       decision,
		"mode":           modeLabel(result.ShadowMode),
	})

	for _, rule := range result.MatchedRules {
		c.inc("crawler_filter_matched_rules_total", map[string]string{
			"classification": string(result.Classification),
			"rule":           rule,
		})
	}

	if result.DiscardReason != "" {
		c.inc("crawler_filter_discard_reasons_total", map[string]string{
			"reason": result.DiscardReason,
			"mode":   modeLabel(result.ShadowMode),
		})
	}
}

func (c *Counters) Snapshot() CounterSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make(CounterSnapshot, len(c.values))
	for key, value := range c.values {
		out[key] = value
	}
	return out
}

func (c *Counters) WritePrometheus(w io.Writer) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	descriptors := []struct {
		name string
		help string
	}{
		{name: "crawler_filter_events_total", help: "Total events classified by the blockchain pre-filter."},
		{name: "crawler_filter_matched_rules_total", help: "Total filter rule matches by rule and resulting classification."},
		{name: "crawler_filter_discard_reasons_total", help: "Total discard reasons emitted by the blockchain pre-filter."},
	}

	keys := make([]string, 0, len(c.values))
	for key := range c.values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, descriptor := range descriptors {
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", descriptor.name, descriptor.help, descriptor.name); err != nil {
			return err
		}
		for _, key := range keys {
			name := strings.SplitN(key, "{", 2)[0]
			if name != descriptor.name {
				continue
			}
			if _, err := fmt.Fprintf(w, "%s%s %d\n", descriptor.name, labelString(c.labels[key]), c.values[key]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Counters) inc(name string, labels map[string]string) {
	key := name + labelString(labels)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.values[key]++
	if _, ok := c.labels[key]; !ok {
		c.labels[key] = labels
	}
}

func modeLabel(shadow bool) string {
	if shadow {
		return string(ModeShadow)
	}
	return string(ModeEnforce)
}

func labelString(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString("{")
	for index, key := range keys {
		if index > 0 {
			b.WriteString(",")
		}
		b.WriteString(key)
		b.WriteString(`="`)
		b.WriteString(escapeLabel(labels[key]))
		b.WriteString(`"`)
	}
	b.WriteString("}")
	return b.String()
}

func escapeLabel(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}
