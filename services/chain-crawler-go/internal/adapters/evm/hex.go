package evm

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

func quantity(n uint64) string {
	return fmt.Sprintf("0x%x", n)
}

func parseHexUint(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(strings.ToLower(s), "0x")
	if s == "" {
		return 0, nil
	}
	return strconv.ParseUint(s, 16, 64)
}

func parseHexBigDecimalString(s string) (string, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(strings.ToLower(s), "0x")
	if s == "" {
		return "0", nil
	}
	n := new(big.Int)
	if _, ok := n.SetString(s, 16); !ok {
		return "", fmt.Errorf("invalid hex integer %q", s)
	}
	return n.String(), nil
}

func normalizeHex(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return ""
	}
	if strings.HasPrefix(s, "0x") {
		return s
	}
	return "0x" + s
}

func normalizeAddress(s string) string {
	s = normalizeHex(s)
	if len(s) != 42 {
		return s
	}
	return s
}

func addressFromTopic(topic string) (string, error) {
	topic = strings.TrimPrefix(normalizeHex(topic), "0x")
	if len(topic) != 64 {
		return "", fmt.Errorf("invalid indexed address topic length %d", len(topic))
	}
	return "0x" + strings.ToLower(topic[24:]), nil
}

func wordAt(data string, index int) (string, error) {
	data = strings.TrimPrefix(normalizeHex(data), "0x")
	start := index * 64
	end := start + 64
	if start < 0 || end > len(data) {
		return "", fmt.Errorf("missing ABI word %d", index)
	}
	return "0x" + data[start:end], nil
}

func offsetWordToIndex(word string) (int, error) {
	n, err := parseHexUint(word)
	if err != nil {
		return 0, err
	}
	if n%32 != 0 {
		return 0, fmt.Errorf("ABI offset %d is not word aligned", n)
	}
	return int(n / 32), nil
}

func sameHex(a, b string) bool {
	return normalizeHex(a) == normalizeHex(b)
}
