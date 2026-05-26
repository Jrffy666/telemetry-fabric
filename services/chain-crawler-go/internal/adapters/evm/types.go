package evm

import (
	"errors"
	"net/http"
	"time"
)

var (
	ErrNoRPCEndpoints = errors.New("evm adapter: no RPC endpoints configured")
	ErrNoWSEndpoints  = errors.New("evm adapter: no WebSocket endpoints configured")
	ErrNoTransport    = errors.New("evm adapter: no subscription transport provided")
)

type TokenStandard string

const (
	TokenStandardUnspecified TokenStandard = "TOKEN_STANDARD_UNSPECIFIED"
	TokenStandardERC20       TokenStandard = "TOKEN_STANDARD_ERC20"
	TokenStandardERC721      TokenStandard = "TOKEN_STANDARD_ERC721"
	TokenStandardERC1155     TokenStandard = "TOKEN_STANDARD_ERC1155"
)

type Config struct {
	ChainName               string
	Network                 string
	ChainID                 uint64
	RPCEndpoints            []EndpointConfig
	WSEndpoints             []EndpointConfig
	FinalityDepth           uint64
	ReorgWindow             uint64
	MaxBlockRange           uint64
	InitialBlockRange       uint64
	MinBlockRange           uint64
	MaxGetLogsRetries       int
	GetLogsRetryBackoff     time.Duration
	GetLogsRetryMaxBackoff  time.Duration
	AdaptiveGrowFactor      uint64
	RateLimitPerSecond      int
	ReconnectInitialBackoff time.Duration
	ReconnectMaxBackoff     time.Duration
	HeartbeatInterval       time.Duration
	LogFilters              []LogFilter
	WatchedContracts        []WatchedContract
	HTTPClient              *http.Client
}

type EndpointConfig struct {
	Name      string
	URL       string
	Headers   map[string]string
	Timeout   time.Duration
	RateLimit int
}

type WatchedContract struct {
	Address       string
	TokenStandard TokenStandard
	TokenSymbol   string
}

// LogFilter is the public configuration form for eth_getLogs. Topics are
// grouped by topic position; multiple values inside a position are OR-ed by
// Ethereum JSON-RPC.
type LogFilter struct {
	Addresses []string
	Topics    [][]string
}

// NormalizedChainEvent is the adapter output boundary. It intentionally avoids
// exposing raw EVM log or receipt structures to downstream services.
type NormalizedChainEvent struct {
	Chain           string            `json:"chain"`
	Network         string            `json:"network"`
	ChainID         uint64            `json:"chain_id"`
	BlockNumber     uint64            `json:"block_number"`
	BlockHash       string            `json:"block_hash,omitempty"`
	TxHash          string            `json:"tx_hash"`
	TxIndex         uint64            `json:"tx_index,omitempty"`
	LogIndex        uint64            `json:"log_index"`
	EventType       string            `json:"event_type"`
	From            string            `json:"from_address"`
	To              string            `json:"to_address"`
	Contract        string            `json:"contract_address"`
	TokenAddress    string            `json:"token_address"`
	TokenSymbol     string            `json:"token_symbol,omitempty"`
	TokenStandard   TokenStandard     `json:"token_standard"`
	AmountRaw       string            `json:"amount_raw"`
	TokenID         string            `json:"token_id,omitempty"`
	BatchIndex      uint32            `json:"batch_index,omitempty"`
	FinalityStatus  string            `json:"finality_status,omitempty"`
	Confirmations   uint32            `json:"confirmations,omitempty"`
	SourceRPC       string            `json:"source_rpc,omitempty"`
	Reorged         bool              `json:"reorged"`
	DiscardReason   string            `json:"discard_reason,omitempty"`
	DecodeError     string            `json:"decode_error,omitempty"`
	ObservedAt      time.Time         `json:"observed_at,omitempty"`
	AdapterMetadata map[string]string `json:"adapter_metadata,omitempty"`
}

type EndpointHealth struct {
	Endpoint      string            `json:"endpoint"`
	Healthy       bool              `json:"healthy"`
	LatestBlock   uint64            `json:"latest_block"`
	LatencyMillis int64             `json:"latency_ms"`
	CheckedAt     time.Time         `json:"checked_at"`
	Error         string            `json:"error,omitempty"`
	Attributes    map[string]string `json:"attributes,omitempty"`
}

type BlockRef struct {
	Chain      string `json:"chain"`
	Network    string `json:"network"`
	ChainID    uint64 `json:"chain_id"`
	Number     uint64 `json:"number"`
	Hash       string `json:"hash"`
	ParentHash string `json:"parent_hash"`
}

type BlockHeader struct {
	Chain      string    `json:"chain"`
	Network    string    `json:"network"`
	ChainID    uint64    `json:"chain_id"`
	Number     uint64    `json:"number"`
	Hash       string    `json:"hash"`
	ParentHash string    `json:"parent_hash"`
	Timestamp  time.Time `json:"timestamp,omitempty"`
	SourceRPC  string    `json:"source_rpc,omitempty"`
}

type TransactionReceipt struct {
	TxHash      string `json:"tx_hash"`
	BlockNumber uint64 `json:"block_number"`
	BlockHash   string `json:"block_hash"`
	Status      bool   `json:"success"`
}

type CallRequest struct {
	From     string `json:"from,omitempty"`
	To       string `json:"to,omitempty"`
	Gas      string `json:"gas,omitempty"`
	GasPrice string `json:"gasPrice,omitempty"`
	Value    string `json:"value,omitempty"`
	Data     string `json:"data,omitempty"`
}
