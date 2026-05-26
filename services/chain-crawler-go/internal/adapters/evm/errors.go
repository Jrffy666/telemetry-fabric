package evm

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
)

type RPCErrorKind string

const (
	RPCErrorUnknown              RPCErrorKind = "unknown"
	RPCErrorTimeout              RPCErrorKind = "timeout"
	RPCErrorRateLimited          RPCErrorKind = "rate_limited"
	RPCErrorServerError          RPCErrorKind = "server_error"
	RPCErrorInvalidParams        RPCErrorKind = "invalid_params"
	RPCErrorBlockRangeTooLarge   RPCErrorKind = "block_range_too_large"
	RPCErrorMissingBlock         RPCErrorKind = "missing_block"
	RPCErrorPrunedData           RPCErrorKind = "pruned_data"
	RPCErrorExecutionReverted    RPCErrorKind = "execution_reverted"
	RPCErrorWebSocketDisconnect  RPCErrorKind = "websocket_disconnect"
	RPCErrorInconsistentResponse RPCErrorKind = "inconsistent_response"
)

type RPCError struct {
	Kind       RPCErrorKind
	Method     string
	Endpoint   string
	HTTPStatus int
	Code       int
	Message    string
	Err        error
}

func (e *RPCError) Error() string {
	if e == nil {
		return ""
	}
	parts := []string{"evm RPC"}
	if e.Method != "" {
		parts = append(parts, e.Method)
	}
	if e.Endpoint != "" {
		parts = append(parts, e.Endpoint)
	}
	parts = append(parts, string(e.Kind))
	if e.HTTPStatus != 0 {
		parts = append(parts, fmt.Sprintf("http=%d", e.HTTPStatus))
	}
	if e.Code != 0 {
		parts = append(parts, fmt.Sprintf("code=%d", e.Code))
	}
	if e.Message != "" {
		parts = append(parts, e.Message)
	} else if e.Err != nil {
		parts = append(parts, e.Err.Error())
	}
	return strings.Join(parts, ": ")
}

func (e *RPCError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func ClassifyRPCError(err error) RPCErrorKind {
	if err == nil {
		return ""
	}
	var rpcErr *RPCError
	if errors.As(err, &rpcErr) {
		return rpcErr.Kind
	}
	return classifyErrorMessage(err)
}

func isRetryableRPCError(err error) bool {
	switch ClassifyRPCError(err) {
	case RPCErrorTimeout, RPCErrorRateLimited, RPCErrorServerError, RPCErrorWebSocketDisconnect, RPCErrorInconsistentResponse:
		return true
	default:
		return false
	}
}

func classifyHTTPStatus(status int) RPCErrorKind {
	switch {
	case status == http.StatusTooManyRequests:
		return RPCErrorRateLimited
	case status >= 500 && status <= 599:
		return RPCErrorServerError
	default:
		return RPCErrorUnknown
	}
}

func classifyJSONRPCError(code int, message string) RPCErrorKind {
	lower := strings.ToLower(message)
	switch {
	case strings.Contains(lower, "block range") && (strings.Contains(lower, "too large") || strings.Contains(lower, "exceed") || strings.Contains(lower, "limit")):
		return RPCErrorBlockRangeTooLarge
	case strings.Contains(lower, "query returned more than") || strings.Contains(lower, "more than") && strings.Contains(lower, "results"):
		return RPCErrorBlockRangeTooLarge
	case strings.Contains(lower, "log response size exceeded") || strings.Contains(lower, "response size exceeded"):
		return RPCErrorBlockRangeTooLarge
	case strings.Contains(lower, "header not found") || strings.Contains(lower, "block not found") || strings.Contains(lower, "missing block"):
		return RPCErrorMissingBlock
	case strings.Contains(lower, "pruned") || strings.Contains(lower, "ancient") || strings.Contains(lower, "state unavailable") || strings.Contains(lower, "missing trie node"):
		return RPCErrorPrunedData
	case strings.Contains(lower, "execution reverted") || strings.Contains(lower, "vm execution error") && strings.Contains(lower, "revert"):
		return RPCErrorExecutionReverted
	case strings.Contains(lower, "invalid params") || code == -32602:
		return RPCErrorInvalidParams
	case code == -32005:
		return RPCErrorBlockRangeTooLarge
	case code <= -32000 && code >= -32099:
		return RPCErrorServerError
	default:
		return RPCErrorUnknown
	}
}

func classifyErrorMessage(err error) RPCErrorKind {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return RPCErrorTimeout
	}
	if errors.Is(err, context.Canceled) {
		return RPCErrorUnknown
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return RPCErrorTimeout
	}

	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "context deadline exceeded"), strings.Contains(message, "i/o timeout"), strings.Contains(message, "timeout"):
		return RPCErrorTimeout
	case strings.Contains(message, "429"), strings.Contains(message, "rate limit"), strings.Contains(message, "too many requests"):
		return RPCErrorRateLimited
	case strings.Contains(message, "websocket") && (strings.Contains(message, "close") || strings.Contains(message, "disconnect")):
		return RPCErrorWebSocketDisconnect
	case strings.Contains(message, "connection reset"), strings.Contains(message, "connection refused"), strings.Contains(message, "unexpected eof"), strings.TrimSpace(message) == "eof":
		return RPCErrorWebSocketDisconnect
	case strings.Contains(message, "invalid character") || strings.Contains(message, "cannot unmarshal") || strings.Contains(message, "mismatched id"):
		return RPCErrorInconsistentResponse
	default:
		return classifyJSONRPCError(0, message)
	}
}

func rpcErr(kind RPCErrorKind, endpoint EndpointConfig, method string, message string, err error) *RPCError {
	return &RPCError{
		Kind:     kind,
		Method:   method,
		Endpoint: endpointName(endpoint),
		Message:  message,
		Err:      err,
	}
}
