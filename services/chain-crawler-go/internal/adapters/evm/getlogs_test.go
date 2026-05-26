package evm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestFetchRangeAdaptiveShrinksAndGrowsGetLogsRanges(t *testing.T) {
	var requested [][2]uint64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode RPC request: %v", err)
		}
		switch request.Method {
		case "eth_blockNumber":
			writeRPCResult(t, w, request.ID, "0x20")
		case "eth_getLogs":
			from, to := mustRequestRange(t, request)
			requested = append(requested, [2]uint64{from, to})
			if to-from+1 > 2 {
				writeRPCError(t, w, request.ID, -32005, "block range too large")
				return
			}
			writeRPCResult(t, w, request.ID, []rpcLog{testERC20Log(from, 0, testUSDC, testAlice, testBob, 10)})
		default:
			t.Fatalf("unexpected method %s", request.Method)
		}
	}))
	defer server.Close()

	adapter := mustRangeAdapter(t, server.URL)
	events, err := adapter.FetchRange(context.Background(), 1, 6)
	if err != nil {
		t.Fatalf("FetchRange returned error: %v", err)
	}
	wantRanges := [][2]uint64{{1, 6}, {1, 3}, {1, 2}, {3, 6}, {3, 4}, {5, 6}}
	if !reflect.DeepEqual(requested, wantRanges) {
		t.Fatalf("ranges = %#v, want %#v", requested, wantRanges)
	}
	if len(events) != 3 {
		t.Fatalf("event count = %d, want 3", len(events))
	}
}

func TestFetchRangeContinuesAfterChunkFailureAndReturnsRetryableChunks(t *testing.T) {
	var blockTwoAttempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode RPC request: %v", err)
		}
		switch request.Method {
		case "eth_blockNumber":
			writeRPCResult(t, w, request.ID, "0x8")
		case "eth_getLogs":
			from, to := mustRequestRange(t, request)
			if from != to {
				t.Fatalf("range = %d..%d, want one-block chunks", from, to)
			}
			if from == 2 {
				atomic.AddInt32(&blockTwoAttempts, 1)
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("upstream unavailable"))
				return
			}
			writeRPCResult(t, w, request.ID, []rpcLog{testERC20Log(from, 0, testUSDC, testAlice, testBob, 10)})
		default:
			t.Fatalf("unexpected method %s", request.Method)
		}
	}))
	defer server.Close()

	adapter := mustRangeAdapter(t, server.URL)
	adapter.cfg.InitialBlockRange = 1
	adapter.cfg.MaxBlockRange = 1
	adapter.cfg.MaxGetLogsRetries = 1

	events, err := adapter.FetchRange(context.Background(), 1, 3)
	if err == nil {
		t.Fatalf("FetchRange returned nil error, want RangeFetchError")
	}
	var rangeErr *RangeFetchError
	if !errors.As(err, &rangeErr) {
		t.Fatalf("error = %T, want *RangeFetchError", err)
	}
	if len(rangeErr.FailedChunks) != 1 {
		t.Fatalf("failed chunks = %d, want 1", len(rangeErr.FailedChunks))
	}
	failed := rangeErr.FailedChunks[0]
	if failed.FromBlock != 2 || failed.ToBlock != 2 || failed.ErrorKind != RPCErrorServerError || failed.Attempts != 2 {
		t.Fatalf("failed chunk = %#v", failed)
	}
	if attempts := atomic.LoadInt32(&blockTwoAttempts); attempts != 2 {
		t.Fatalf("block two attempts = %d, want 2", attempts)
	}
	if len(events) != 2 {
		t.Fatalf("event count = %d, want partial events from blocks 1 and 3", len(events))
	}
}

func mustRangeAdapter(t *testing.T, rpcURL string) *Adapter {
	t.Helper()
	adapter, err := NewAdapter(Config{
		ChainName:               "ethereum",
		Network:                 "mainnet",
		ChainID:                 1,
		FinalityDepth:           2,
		ReorgWindow:             8,
		MaxBlockRange:           6,
		InitialBlockRange:       6,
		MinBlockRange:           1,
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
			Address:       testUSDC,
			TokenStandard: TokenStandardERC20,
			TokenSymbol:   "USDC",
		}},
	})
	if err != nil {
		t.Fatalf("NewAdapter returned error: %v", err)
	}
	return adapter
}
