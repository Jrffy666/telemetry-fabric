package evm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSubscribeNewHeadsReconnectsAndBackfillsMissingHeaders(t *testing.T) {
	var latest uint64 = 1
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode RPC request: %v", err)
		}
		switch request.Method {
		case "eth_blockNumber":
			writeRPCResult(t, w, request.ID, quantity(atomic.LoadUint64(&latest)))
		case "eth_getBlockByNumber":
			number, err := parseHexUint(request.Params[0].(string))
			if err != nil {
				t.Fatalf("parse block number: %v", err)
			}
			writeRPCResult(t, w, request.ID, rpcBlock{
				Number:     quantity(number),
				Hash:       quantity(number),
				ParentHash: quantity(number - 1),
				Timestamp:  "0x1",
			})
		default:
			t.Fatalf("unexpected method %s", request.Method)
		}
	}))
	defer server.Close()

	adapter := mustWSAdapter(t, server.URL)
	transport := newMockSubscriptionTransport()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	heads, unsubscribe, err := adapter.SubscribeNewHeads(ctx, transport)
	if err != nil {
		t.Fatalf("SubscribeNewHeads returned error: %v", err)
	}
	defer unsubscribe()

	first := transport.waitForSubscription(t, 1)
	first.send(t, rpcBlock{Number: "0x1", Hash: "0x1", ParentHash: "0x0", Timestamp: "0x1"})
	if got := readHeader(t, heads); got.Number != 1 {
		t.Fatalf("first head = %#v", got)
	}

	atomic.StoreUint64(&latest, 2)
	first.close()
	transport.waitForSubscription(t, 2)
	if got := readHeader(t, heads); got.Number != 2 {
		t.Fatalf("backfilled head = %#v, want block 2", got)
	}
}

func TestSubscribeLogsReconnectsAndBackfillsMissingLogRange(t *testing.T) {
	var latest uint64 = 1
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode RPC request: %v", err)
		}
		switch request.Method {
		case "eth_blockNumber":
			writeRPCResult(t, w, request.ID, quantity(atomic.LoadUint64(&latest)))
		case "eth_getLogs":
			from, to := mustRequestRange(t, request)
			var logs []rpcLog
			for block := from; block <= to; block++ {
				logs = append(logs, testERC20Log(block, 0, testUSDC, testAlice, testBob, 10))
				if block == to {
					break
				}
			}
			writeRPCResult(t, w, request.ID, logs)
		default:
			t.Fatalf("unexpected method %s", request.Method)
		}
	}))
	defer server.Close()

	adapter := mustWSAdapter(t, server.URL)
	transport := newMockSubscriptionTransport()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logs, unsubscribe, err := adapter.SubscribeLogs(ctx, transport, adapter.filtered[0])
	if err != nil {
		t.Fatalf("SubscribeLogs returned error: %v", err)
	}
	defer unsubscribe()

	first := transport.waitForSubscription(t, 1)
	first.send(t, testERC20Log(1, 0, testUSDC, testAlice, testBob, 10))
	if got := readEvent(t, logs); got.BlockNumber != 1 {
		t.Fatalf("first event = %#v", got)
	}

	atomic.StoreUint64(&latest, 2)
	first.close()
	transport.waitForSubscription(t, 2)
	if got := readEvent(t, logs); got.BlockNumber != 2 {
		t.Fatalf("backfilled event = %#v, want block 2", got)
	}
}

func TestSubscribeNewHeadsHeartbeatReconnects(t *testing.T) {
	adapter := mustWSAdapter(t, "http://127.0.0.1/mock")
	adapter.cfg.HeartbeatInterval = time.Millisecond
	transport := newMockSubscriptionTransport()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, unsubscribe, err := adapter.SubscribeNewHeads(ctx, transport)
	if err != nil {
		t.Fatalf("SubscribeNewHeads returned error: %v", err)
	}
	defer unsubscribe()
	transport.waitForSubscription(t, 2)
}

func mustWSAdapter(t *testing.T, rpcURL string) *Adapter {
	t.Helper()
	adapter, err := NewAdapter(Config{
		ChainName:               "ethereum",
		Network:                 "mainnet",
		ChainID:                 1,
		FinalityDepth:           2,
		ReorgWindow:             8,
		MaxBlockRange:           10,
		InitialBlockRange:       10,
		MinBlockRange:           1,
		MaxGetLogsRetries:       1,
		GetLogsRetryBackoff:     time.Nanosecond,
		GetLogsRetryMaxBackoff:  time.Nanosecond,
		ReconnectInitialBackoff: time.Nanosecond,
		ReconnectMaxBackoff:     time.Millisecond,
		HeartbeatInterval:       time.Second,
		RPCEndpoints: []EndpointConfig{{
			Name: "mock-rpc",
			URL:  rpcURL,
		}},
		WSEndpoints: []EndpointConfig{{
			Name: "mock-ws",
			URL:  "ws://127.0.0.1/mock",
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

type mockSubscriptionTransport struct {
	mu            sync.Mutex
	subscriptions []*mockSubscription
}

type mockSubscription struct {
	once sync.Once
	ch   chan json.RawMessage
}

func newMockSubscriptionTransport() *mockSubscriptionTransport {
	return &mockSubscriptionTransport{}
}

func (m *mockSubscriptionTransport) Subscribe(ctx context.Context, endpoint EndpointConfig, method string, params []interface{}) (<-chan json.RawMessage, func(), error) {
	sub := &mockSubscription{ch: make(chan json.RawMessage, 16)}
	m.mu.Lock()
	m.subscriptions = append(m.subscriptions, sub)
	m.mu.Unlock()
	return sub.ch, sub.close, nil
}

func (m *mockSubscriptionTransport) waitForSubscription(t *testing.T, count int) *mockSubscription {
	t.Helper()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for subscription %d", count)
		case <-ticker.C:
			m.mu.Lock()
			if len(m.subscriptions) >= count {
				sub := m.subscriptions[count-1]
				m.mu.Unlock()
				return sub
			}
			m.mu.Unlock()
		}
	}
}

func (s *mockSubscription) send(t *testing.T, value interface{}) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal subscription value: %v", err)
	}
	s.ch <- raw
}

func (s *mockSubscription) close() {
	s.once.Do(func() {
		close(s.ch)
	})
}

func readHeader(t *testing.T, ch <-chan BlockHeader) BlockHeader {
	t.Helper()
	select {
	case header := <-ch:
		return header
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for header")
	}
	return BlockHeader{}
}

func readEvent(t *testing.T, ch <-chan NormalizedChainEvent) NormalizedChainEvent {
	t.Helper()
	select {
	case event := <-ch:
		return event
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for event")
	}
	return NormalizedChainEvent{}
}
