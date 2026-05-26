package evm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFetchRangeDecodesERC20Transfer(t *testing.T) {
	fixture := mustReadFixture(t, "erc20_getlogs_response.json")
	var seenMethods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode RPC request: %v", err)
		}
		seenMethods = append(seenMethods, request.Method)

		switch request.Method {
		case "eth_blockNumber":
			writeRPCResult(t, w, request.ID, "0x50")
		case "eth_getLogs":
			writeRPCResult(t, w, request.ID, json.RawMessage(fixture))
		default:
			t.Fatalf("unexpected RPC method %s", request.Method)
		}
	}))
	defer server.Close()

	adapter := mustAdapter(t, server.URL)
	events, err := adapter.FetchRange(context.Background(), 16, 16)
	if err != nil {
		t.Fatalf("FetchRange returned error: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1", len(events))
	}
	event := events[0]
	if event.Chain != "ethereum" {
		t.Fatalf("chain = %q, want ethereum", event.Chain)
	}
	if event.Network != "mainnet" {
		t.Fatalf("network = %q, want mainnet", event.Network)
	}
	if event.ChainID != 1 {
		t.Fatalf("chain_id = %d, want 1", event.ChainID)
	}
	if event.BlockNumber != 16 {
		t.Fatalf("block_number = %d, want 16", event.BlockNumber)
	}
	if event.TxHash != "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("tx_hash = %q", event.TxHash)
	}
	if event.LogIndex != 0 {
		t.Fatalf("log_index = %d, want 0", event.LogIndex)
	}
	if event.From != "0x1111111111111111111111111111111111111111" {
		t.Fatalf("from = %q", event.From)
	}
	if event.To != "0x2222222222222222222222222222222222222222" {
		t.Fatalf("to = %q", event.To)
	}
	if event.Contract != "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48" {
		t.Fatalf("contract = %q", event.Contract)
	}
	if event.AmountRaw != "1000000" {
		t.Fatalf("amount_raw = %q, want 1000000", event.AmountRaw)
	}
	if event.TokenStandard != TokenStandardERC20 {
		t.Fatalf("token_standard = %q, want %q", event.TokenStandard, TokenStandardERC20)
	}
	if len(seenMethods) != 2 || seenMethods[0] != "eth_blockNumber" || seenMethods[1] != "eth_getLogs" {
		t.Fatalf("methods = %#v, want eth_blockNumber then eth_getLogs", seenMethods)
	}
}

func TestBlockNumberAndHealthCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode RPC request: %v", err)
		}
		if request.Method != "eth_blockNumber" {
			t.Fatalf("method = %s, want eth_blockNumber", request.Method)
		}
		writeRPCResult(t, w, request.ID, "0x2a")
	}))
	defer server.Close()

	adapter := mustAdapter(t, server.URL)
	blockNumber, err := adapter.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf("BlockNumber returned error: %v", err)
	}
	if blockNumber != 42 {
		t.Fatalf("block number = %d, want 42", blockNumber)
	}

	health, err := adapter.HealthCheck(context.Background())
	if err != nil {
		t.Fatalf("HealthCheck returned error: %v", err)
	}
	if !health.Healthy {
		t.Fatalf("health should be healthy: %#v", health)
	}
	if health.LatestBlock != 42 {
		t.Fatalf("health latest block = %d, want 42", health.LatestBlock)
	}
}

func TestDetectReorg(t *testing.T) {
	previous := BlockRef{
		ChainID: 1,
		Number:  100,
		Hash:    "0xaaa",
	}
	next := BlockRef{
		ChainID:    1,
		Number:     101,
		Hash:       "0xbbb",
		ParentHash: "0xccc",
	}

	check := DetectReorg(previous, next)
	if check.Status != ReorgStatusReorged {
		t.Fatalf("status = %s, want %s", check.Status, ReorgStatusReorged)
	}
}

func mustAdapter(t *testing.T, rpcURL string) *Adapter {
	t.Helper()
	adapter, err := NewAdapter(Config{
		ChainName:               "ethereum",
		Network:                 "mainnet",
		ChainID:                 1,
		MaxBlockRange:           100,
		InitialBlockRange:       100,
		MinBlockRange:           1,
		FinalityDepth:           2,
		ReorgWindow:             8,
		MaxGetLogsRetries:       1,
		GetLogsRetryBackoff:     time.Nanosecond,
		GetLogsRetryMaxBackoff:  time.Nanosecond,
		ReconnectInitialBackoff: time.Nanosecond,
		ReconnectMaxBackoff:     time.Nanosecond,
		HeartbeatInterval:       time.Second,
		RPCEndpoints: []EndpointConfig{{
			Name: "mock-rpc",
			URL:  rpcURL,
		}},
		WatchedContracts: []WatchedContract{{
			Address:       "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
			TokenStandard: TokenStandardERC20,
			TokenSymbol:   "USDC",
		}},
	})
	if err != nil {
		t.Fatalf("NewAdapter returned error: %v", err)
	}
	return adapter
}

func mustReadFixture(t *testing.T, name string) []byte {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}

	for dir := wd; ; dir = filepath.Dir(dir) {
		path := filepath.Join(dir, "tests", "fixtures", "evm", name)
		data, err := os.ReadFile(path)
		if err == nil {
			return data
		}
		if !os.IsNotExist(err) {
			t.Fatalf("read fixture %s: %v", name, err)
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
	}

	t.Fatalf("fixture %s not found under tests/fixtures/evm from %s", name, wd)
	return nil
}

func writeRPCResult(t *testing.T, w http.ResponseWriter, id uint64, result interface{}) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  result,
	}); err != nil {
		t.Fatalf("write RPC result: %v", err)
	}
}

func writeRPCError(t *testing.T, w http.ResponseWriter, id uint64, code int, message string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]interface{}{
			"code":    code,
			"message": message,
		},
	}); err != nil {
		t.Fatalf("write RPC error: %v", err)
	}
}
