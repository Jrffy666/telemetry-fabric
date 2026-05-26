package filter

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"telemetry-fabric/services/chain-crawler-go/pkg/event"
)

func LoadRuleSet(path string) (RuleSet, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return RuleSet{}, err
	}
	return ParseRuleSetYAML(string(data))
}

func LoadRuleSets(paths ...string) (RuleSet, error) {
	merged := defaultRuleSet()
	for _, path := range paths {
		next, err := LoadRuleSet(path)
		if err != nil {
			return RuleSet{}, err
		}
		merged = mergeRuleSets(merged, next)
	}
	return normalizeRuleSet(merged)
}

func ParseRuleSetYAML(input string) (RuleSet, error) {
	rules := defaultRuleSet()

	var section string
	var subsection string
	var inRuleMatch bool
	var currentRule *Rule
	var currentAddress *WatchEntry
	var currentContract *WatchEntry
	var currentToken *TokenWatchEntry

	lines := strings.Split(input, "\n")
	for lineNumber, raw := range lines {
		line := stripComment(raw)
		if strings.TrimSpace(line) == "" {
			continue
		}

		indent := leadingSpaces(line)
		trimmed := strings.TrimSpace(line)

		if indent == 0 {
			section = ""
			subsection = ""
			inRuleMatch = false
			currentRule = nil
			currentAddress = nil
			currentContract = nil
			currentToken = nil

			key, value, ok := splitKeyValue(trimmed)
			if !ok {
				return RuleSet{}, fmt.Errorf("line %d: expected key/value", lineNumber+1)
			}
			switch key {
			case "version", "rule_version":
				rules.Version = scalar(value)
			case "mode":
				rules.Mode = Mode(strings.ToLower(scalar(value)))
			case "default_classification":
				rules.DefaultClassification = Classification(strings.ToUpper(scalar(value)))
			case "large_transfer_usd":
				parsed, err := parseFloat(value)
				if err != nil {
					return RuleSet{}, fmt.Errorf("line %d: %w", lineNumber+1, err)
				}
				rules.LargeTransferUSD = parsed
			case "gas_anomaly", "watchlists", "blacklist", "rules":
				section = key
			default:
				return RuleSet{}, fmt.Errorf("line %d: unknown top-level key %q", lineNumber+1, key)
			}
			continue
		}

		switch section {
		case "gas_anomaly":
			key, value, ok := splitKeyValue(trimmed)
			if !ok {
				return RuleSet{}, fmt.Errorf("line %d: expected gas anomaly key/value", lineNumber+1)
			}
			switch key {
			case "max_gas_used":
				parsed, err := parseUint(value)
				if err != nil {
					return RuleSet{}, fmt.Errorf("line %d: %w", lineNumber+1, err)
				}
				rules.GasAnomaly.MaxGasUsed = parsed
			case "max_gas_price_gwei":
				parsed, err := parseFloat(value)
				if err != nil {
					return RuleSet{}, fmt.Errorf("line %d: %w", lineNumber+1, err)
				}
				rules.GasAnomaly.MaxGasPriceGwei = parsed
			default:
				return RuleSet{}, fmt.Errorf("line %d: unknown gas anomaly key %q", lineNumber+1, key)
			}

		case "blacklist":
			if indent == 2 {
				key, _, ok := splitKeyValue(trimmed)
				if !ok {
					return RuleSet{}, fmt.Errorf("line %d: expected blacklist section", lineNumber+1)
				}
				subsection = key
				continue
			}
			value, ok := listValue(trimmed)
			if !ok {
				return RuleSet{}, fmt.Errorf("line %d: expected blacklist list item", lineNumber+1)
			}
			switch subsection {
			case "addresses":
				rules.Blacklist.Addresses = append(rules.Blacklist.Addresses, scalar(value))
			case "contracts":
				rules.Blacklist.Contracts = append(rules.Blacklist.Contracts, scalar(value))
			case "tokens":
				rules.Blacklist.Tokens = append(rules.Blacklist.Tokens, scalar(value))
			default:
				return RuleSet{}, fmt.Errorf("line %d: unknown blacklist section %q", lineNumber+1, subsection)
			}

		case "watchlists":
			if indent == 2 {
				key, _, ok := splitKeyValue(trimmed)
				if !ok {
					return RuleSet{}, fmt.Errorf("line %d: expected watchlist section", lineNumber+1)
				}
				subsection = key
				currentAddress = nil
				currentContract = nil
				currentToken = nil
				continue
			}
			if strings.HasPrefix(trimmed, "- ") {
				key, value, ok := splitKeyValue(strings.TrimSpace(strings.TrimPrefix(trimmed, "- ")))
				if !ok {
					return RuleSet{}, fmt.Errorf("line %d: expected watchlist key/value", lineNumber+1)
				}
				switch subsection {
				case "addresses":
					rules.Watchlists.Addresses = append(rules.Watchlists.Addresses, WatchEntry{})
					currentAddress = &rules.Watchlists.Addresses[len(rules.Watchlists.Addresses)-1]
					if err := setWatchEntryValue(currentAddress, key, value); err != nil {
						return RuleSet{}, fmt.Errorf("line %d: %w", lineNumber+1, err)
					}
				case "contracts":
					rules.Watchlists.Contracts = append(rules.Watchlists.Contracts, WatchEntry{})
					currentContract = &rules.Watchlists.Contracts[len(rules.Watchlists.Contracts)-1]
					if err := setWatchEntryValue(currentContract, key, value); err != nil {
						return RuleSet{}, fmt.Errorf("line %d: %w", lineNumber+1, err)
					}
				case "tokens":
					rules.Watchlists.Tokens = append(rules.Watchlists.Tokens, TokenWatchEntry{})
					currentToken = &rules.Watchlists.Tokens[len(rules.Watchlists.Tokens)-1]
					if err := setTokenWatchEntryValue(currentToken, key, value); err != nil {
						return RuleSet{}, fmt.Errorf("line %d: %w", lineNumber+1, err)
					}
				default:
					return RuleSet{}, fmt.Errorf("line %d: unknown watchlist section %q", lineNumber+1, subsection)
				}
				continue
			}
			key, value, ok := splitKeyValue(trimmed)
			if !ok {
				return RuleSet{}, fmt.Errorf("line %d: expected watchlist key/value", lineNumber+1)
			}
			switch {
			case currentAddress != nil:
				if err := setWatchEntryValue(currentAddress, key, value); err != nil {
					return RuleSet{}, fmt.Errorf("line %d: %w", lineNumber+1, err)
				}
			case currentContract != nil:
				if err := setWatchEntryValue(currentContract, key, value); err != nil {
					return RuleSet{}, fmt.Errorf("line %d: %w", lineNumber+1, err)
				}
			case currentToken != nil:
				if err := setTokenWatchEntryValue(currentToken, key, value); err != nil {
					return RuleSet{}, fmt.Errorf("line %d: %w", lineNumber+1, err)
				}
			default:
				return RuleSet{}, fmt.Errorf("line %d: watchlist value without item", lineNumber+1)
			}

		case "rules":
			if indent == 2 && strings.HasPrefix(trimmed, "- ") {
				rules.Rules = append(rules.Rules, Rule{Enabled: true})
				currentRule = &rules.Rules[len(rules.Rules)-1]
				inRuleMatch = false

				item := strings.TrimSpace(strings.TrimPrefix(trimmed, "- "))
				if item == "" {
					continue
				}
				key, value, ok := splitKeyValue(item)
				if !ok {
					return RuleSet{}, fmt.Errorf("line %d: expected rule key/value", lineNumber+1)
				}
				if err := setRuleValue(currentRule, key, value); err != nil {
					return RuleSet{}, fmt.Errorf("line %d: %w", lineNumber+1, err)
				}
				continue
			}
			if currentRule == nil {
				return RuleSet{}, fmt.Errorf("line %d: rule value without rule item", lineNumber+1)
			}
			if indent == 4 {
				key, value, ok := splitKeyValue(trimmed)
				if !ok {
					return RuleSet{}, fmt.Errorf("line %d: expected rule key/value", lineNumber+1)
				}
				if key == "match" {
					inRuleMatch = true
					continue
				}
				inRuleMatch = false
				if err := setRuleValue(currentRule, key, value); err != nil {
					return RuleSet{}, fmt.Errorf("line %d: %w", lineNumber+1, err)
				}
				continue
			}
			if indent >= 6 && inRuleMatch {
				key, value, ok := splitKeyValue(trimmed)
				if !ok {
					return RuleSet{}, fmt.Errorf("line %d: expected match key/value", lineNumber+1)
				}
				if err := setMatchValue(&currentRule.Match, key, value); err != nil {
					return RuleSet{}, fmt.Errorf("line %d: %w", lineNumber+1, err)
				}
				continue
			}
			return RuleSet{}, fmt.Errorf("line %d: unsupported rule indentation", lineNumber+1)

		default:
			return RuleSet{}, fmt.Errorf("line %d: value outside supported section", lineNumber+1)
		}
	}

	return normalizeRuleSet(rules)
}

func defaultRuleSet() RuleSet {
	return RuleSet{
		Version:               "unversioned",
		Mode:                  ModeShadow,
		DefaultClassification: ClassificationDrop,
	}
}

func mergeRuleSets(left RuleSet, right RuleSet) RuleSet {
	if right.Version != "" && right.Version != "unversioned" {
		left.Version = right.Version
	}
	if right.Mode != "" {
		left.Mode = right.Mode
	}
	if right.DefaultClassification != "" {
		left.DefaultClassification = right.DefaultClassification
	}
	if right.LargeTransferUSD > 0 {
		left.LargeTransferUSD = right.LargeTransferUSD
	}
	if right.GasAnomaly.MaxGasUsed > 0 {
		left.GasAnomaly.MaxGasUsed = right.GasAnomaly.MaxGasUsed
	}
	if right.GasAnomaly.MaxGasPriceGwei > 0 {
		left.GasAnomaly.MaxGasPriceGwei = right.GasAnomaly.MaxGasPriceGwei
	}

	left.Watchlists.Addresses = append(left.Watchlists.Addresses, right.Watchlists.Addresses...)
	left.Watchlists.Contracts = append(left.Watchlists.Contracts, right.Watchlists.Contracts...)
	left.Watchlists.Tokens = append(left.Watchlists.Tokens, right.Watchlists.Tokens...)
	left.Blacklist.Addresses = append(left.Blacklist.Addresses, right.Blacklist.Addresses...)
	left.Blacklist.Contracts = append(left.Blacklist.Contracts, right.Blacklist.Contracts...)
	left.Blacklist.Tokens = append(left.Blacklist.Tokens, right.Blacklist.Tokens...)
	left.Rules = append(left.Rules, right.Rules...)
	return left
}

func setWatchEntryValue(entry *WatchEntry, key string, value string) error {
	switch key {
	case "address":
		entry.Address = scalar(value)
	case "label":
		entry.Label = scalar(value)
	case "classification":
		entry.Classification = Classification(strings.ToUpper(scalar(value)))
	case "min_usd":
		parsed, err := parseFloat(value)
		if err != nil {
			return err
		}
		entry.MinUSD = parsed
	default:
		return fmt.Errorf("unknown watchlist key %q", key)
	}
	return nil
}

func setTokenWatchEntryValue(entry *TokenWatchEntry, key string, value string) error {
	switch key {
	case "address":
		entry.Address = scalar(value)
	case "symbol":
		entry.Symbol = scalar(value)
	case "label":
		entry.Label = scalar(value)
	case "classification":
		entry.Classification = Classification(strings.ToUpper(scalar(value)))
	case "min_usd":
		parsed, err := parseFloat(value)
		if err != nil {
			return err
		}
		entry.MinUSD = parsed
	default:
		return fmt.Errorf("unknown token watchlist key %q", key)
	}
	return nil
}

func setRuleValue(rule *Rule, key string, value string) error {
	switch key {
	case "id":
		rule.ID = scalar(value)
	case "description":
		rule.Description = scalar(value)
	case "enabled":
		parsed, err := parseBool(value)
		if err != nil {
			return err
		}
		rule.Enabled = parsed
	case "priority":
		parsed, err := strconv.Atoi(scalar(value))
		if err != nil {
			return fmt.Errorf("invalid priority %q", value)
		}
		rule.Priority = parsed
	case "classification":
		rule.Classification = Classification(strings.ToUpper(scalar(value)))
	case "force_keep":
		parsed, err := parseBool(value)
		if err != nil {
			return err
		}
		rule.ForceKeep = parsed
	case "discard_reason":
		rule.DiscardReason = scalar(value)
	default:
		return fmt.Errorf("unknown rule key %q", key)
	}
	return nil
}

func setMatchValue(match *Match, key string, value string) error {
	switch key {
	case "event_types":
		match.EventTypes = parseStringList(value)
	case "addresses":
		match.Addresses = parseStringList(value)
	case "contracts":
		match.Contracts = parseStringList(value)
	case "tokens":
		match.Tokens = parseStringList(value)
	case "token_symbols":
		match.TokenSymbols = parseStringList(value)
	case "node_statuses":
		values := parseStringList(value)
		match.NodeStatuses = make([]event.NodeStatus, 0, len(values))
		for _, item := range values {
			match.NodeStatuses = append(match.NodeStatuses, event.NodeStatus(item))
		}
	case "min_amount_usd":
		parsed, err := parseFloat(value)
		if err != nil {
			return err
		}
		match.MinAmountUSD = parsed
		match.HasMinAmountUSD = true
	case "failed_transaction":
		parsed, err := parseBool(value)
		if err != nil {
			return err
		}
		match.FailedTransaction = &parsed
	case "reorged":
		parsed, err := parseBool(value)
		if err != nil {
			return err
		}
		match.Reorged = &parsed
	case "gas_used_gte":
		parsed, err := parseUint(value)
		if err != nil {
			return err
		}
		match.MinGasUsed = parsed
		match.HasMinGasUsed = true
	case "gas_price_gwei_gte":
		parsed, err := parseFloat(value)
		if err != nil {
			return err
		}
		match.MinGasPriceGwei = parsed
		match.HasMinGasPriceGwei = true
	case "min_block_lag":
		parsed, err := parseUint(value)
		if err != nil {
			return err
		}
		match.MinBlockLag = parsed
		match.HasMinBlockLag = true
	case "node_anomaly":
		parsed, err := parseBool(value)
		if err != nil {
			return err
		}
		match.NodeAnomaly = &parsed
	default:
		return fmt.Errorf("unknown match key %q", key)
	}
	return nil
}

func stripComment(line string) string {
	inSingle := false
	inDouble := false
	for index, char := range line {
		switch char {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		case '#':
			if !inSingle && !inDouble {
				return line[:index]
			}
		}
	}
	return line
}

func leadingSpaces(line string) int {
	count := 0
	for _, char := range line {
		if char != ' ' {
			break
		}
		count++
	}
	return count
}

func splitKeyValue(value string) (string, string, bool) {
	parts := strings.SplitN(value, ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), true
}

func listValue(value string) (string, bool) {
	if !strings.HasPrefix(value, "- ") {
		return "", false
	}
	return strings.TrimSpace(strings.TrimPrefix(value, "- ")), true
}

func scalar(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, `"`)
	value = strings.Trim(value, `'`)
	return value
}

func parseStringList(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	if strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]") {
		value = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(value, "["), "]"))
		if value == "" {
			return nil
		}
		parts := strings.Split(value, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			out = append(out, scalar(part))
		}
		return out
	}
	return []string{scalar(value)}
}

func parseBool(value string) (bool, error) {
	switch strings.ToLower(scalar(value)) {
	case "true", "yes", "on", "1":
		return true, nil
	case "false", "no", "off", "0":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean %q", value)
	}
}

func parseFloat(value string) (float64, error) {
	parsed, err := strconv.ParseFloat(strings.ReplaceAll(scalar(value), "_", ""), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid float %q", value)
	}
	return parsed, nil
}

func parseUint(value string) (uint64, error) {
	parsed, err := strconv.ParseUint(strings.ReplaceAll(scalar(value), "_", ""), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid unsigned integer %q", value)
	}
	return parsed, nil
}
