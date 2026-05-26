package evm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRPCErrorClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want RPCErrorKind
	}{
		{name: "timeout", err: context.DeadlineExceeded, want: RPCErrorTimeout},
		{name: "rate limit", err: errors.New("429 too many requests"), want: RPCErrorRateLimited},
		{name: "invalid params", err: &RPCError{Kind: classifyJSONRPCError(-32602, "invalid params")}, want: RPCErrorInvalidParams},
		{name: "block range", err: &RPCError{Kind: classifyJSONRPCError(-32005, "block range too large")}, want: RPCErrorBlockRangeTooLarge},
		{name: "missing block", err: errors.New("header not found"), want: RPCErrorMissingBlock},
		{name: "pruned", err: errors.New("missing trie node: state has been pruned"), want: RPCErrorPrunedData},
		{name: "reverted", err: errors.New("execution reverted: ERC20: transfer amount exceeds balance"), want: RPCErrorExecutionReverted},
		{name: "websocket", err: errors.New("websocket: close 1006 abnormal closure"), want: RPCErrorWebSocketDisconnect},
		{name: "inconsistent", err: errors.New("json: cannot unmarshal object into Go value"), want: RPCErrorInconsistentResponse},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClassifyRPCError(tc.err); got != tc.want {
				t.Fatalf("ClassifyRPCError() = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestRPCClientClassifiesHTTPAndInconsistentResponses(t *testing.T) {
	var mode string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request rpcRequest
		_ = json.NewDecoder(r.Body).Decode(&request)
		switch mode {
		case "rate":
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("rate limited"))
		case "server":
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("bad gateway"))
		case "mismatch":
			writeRPCResult(t, w, request.ID+1, "0x1")
		case "missing":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1}`))
		default:
			writeRPCResult(t, w, request.ID, "0x1")
		}
	}))
	defer server.Close()

	client := newRPCClient(Config{RPCEndpoints: []EndpointConfig{{Name: "mock-rpc", URL: server.URL}}})
	tests := []struct {
		mode string
		want RPCErrorKind
	}{
		{mode: "rate", want: RPCErrorRateLimited},
		{mode: "server", want: RPCErrorServerError},
		{mode: "mismatch", want: RPCErrorInconsistentResponse},
		{mode: "missing", want: RPCErrorInconsistentResponse},
	}
	for _, tc := range tests {
		t.Run(tc.mode, func(t *testing.T) {
			mode = tc.mode
			var out string
			_, err := client.call(context.Background(), "eth_blockNumber", nil, &out)
			if err == nil {
				t.Fatalf("call returned nil error")
			}
			if got := ClassifyRPCError(err); got != tc.want {
				t.Fatalf("ClassifyRPCError() = %s, want %s (%v)", got, tc.want, err)
			}
		})
	}
}
