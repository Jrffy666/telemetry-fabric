package evm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

type rpcClient struct {
	endpoints []EndpointConfig
	client    *http.Client
	limiter   *rateLimiter
	nextID    uint64
}

type rpcRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      uint64        `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      uint64          `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *rpcError       `json:"error"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func newRPCClient(cfg Config) *rpcClient {
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &rpcClient{
		endpoints: cfg.RPCEndpoints,
		client:    client,
		limiter:   newRateLimiter(cfg.RateLimitPerSecond),
	}
}

func (c *rpcClient) call(ctx context.Context, method string, params []interface{}, out interface{}) (string, error) {
	if len(c.endpoints) == 0 {
		return "", ErrNoRPCEndpoints
	}

	var lastErr error
	for _, endpoint := range c.endpoints {
		source, err := c.callEndpoint(ctx, endpoint, method, params, out)
		if err == nil {
			return source, nil
		}
		if ctx.Err() != nil {
			return source, err
		}
		switch ClassifyRPCError(err) {
		case RPCErrorInvalidParams, RPCErrorBlockRangeTooLarge, RPCErrorMissingBlock, RPCErrorPrunedData, RPCErrorExecutionReverted:
			return source, err
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = ErrNoRPCEndpoints
	}
	return "", lastErr
}

func (c *rpcClient) callEndpoint(ctx context.Context, endpoint EndpointConfig, method string, params []interface{}, out interface{}) (string, error) {
	if endpoint.URL == "" {
		return "", errors.New("evm adapter: empty RPC endpoint URL")
	}
	if params == nil {
		params = []interface{}{}
	}
	if err := c.limiter.wait(ctx); err != nil {
		return endpointName(endpoint), err
	}

	if endpoint.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, endpoint.Timeout)
		defer cancel()
	}

	id := atomic.AddUint64(&c.nextID, 1)
	body, err := json.Marshal(rpcRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return endpointName(endpoint), err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.URL, bytes.NewReader(body))
	if err != nil {
		return endpointName(endpoint), err
	}
	req.Header.Set("Content-Type", "application/json")
	for key, value := range endpoint.Headers {
		req.Header.Set(key, value)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return endpointName(endpoint), rpcErr(classifyErrorMessage(err), endpoint, method, "", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return endpointName(endpoint), rpcErr(RPCErrorInconsistentResponse, endpoint, method, "", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return endpointName(endpoint), &RPCError{
			Kind:       classifyHTTPStatus(resp.StatusCode),
			Method:     method,
			Endpoint:   endpointName(endpoint),
			HTTPStatus: resp.StatusCode,
			Message:    string(responseBody),
		}
	}

	var decoded rpcResponse
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return endpointName(endpoint), rpcErr(RPCErrorInconsistentResponse, endpoint, method, "", err)
	}
	if decoded.ID != id {
		return endpointName(endpoint), rpcErr(RPCErrorInconsistentResponse, endpoint, method, fmt.Sprintf("mismatched id: got %d want %d", decoded.ID, id), nil)
	}
	if decoded.Error != nil {
		return endpointName(endpoint), &RPCError{
			Kind:     classifyJSONRPCError(decoded.Error.Code, decoded.Error.Message),
			Method:   method,
			Endpoint: endpointName(endpoint),
			Code:     decoded.Error.Code,
			Message:  decoded.Error.Message,
		}
	}
	if out == nil {
		return endpointName(endpoint), nil
	}
	if len(decoded.Result) == 0 {
		return endpointName(endpoint), rpcErr(RPCErrorInconsistentResponse, endpoint, method, "missing result", nil)
	}
	if bytes.Equal(bytes.TrimSpace(decoded.Result), []byte("null")) {
		kind := RPCErrorInconsistentResponse
		if method == "eth_getBlockByNumber" || method == "eth_getTransactionReceipt" {
			kind = RPCErrorMissingBlock
		}
		return endpointName(endpoint), rpcErr(kind, endpoint, method, "null result", nil)
	}
	if err := json.Unmarshal(decoded.Result, out); err != nil {
		return endpointName(endpoint), rpcErr(RPCErrorInconsistentResponse, endpoint, method, "", err)
	}
	return endpointName(endpoint), nil
}

func endpointName(endpoint EndpointConfig) string {
	if endpoint.Name != "" {
		return endpoint.Name
	}
	parsed, err := url.Parse(endpoint.URL)
	if err != nil || parsed.Host == "" {
		return "unnamed-rpc-endpoint"
	}
	return parsed.Scheme + "://" + parsed.Host
}
