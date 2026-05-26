package event

import "time"

type TokenStandard string

const (
	TokenStandardUnspecified TokenStandard = "TOKEN_STANDARD_UNSPECIFIED"
	TokenStandardNative      TokenStandard = "TOKEN_STANDARD_NATIVE"
	TokenStandardERC20       TokenStandard = "TOKEN_STANDARD_ERC20"
	TokenStandardERC721      TokenStandard = "TOKEN_STANDARD_ERC721"
	TokenStandardERC1155     TokenStandard = "TOKEN_STANDARD_ERC1155"
	TokenStandardSPL         TokenStandard = "TOKEN_STANDARD_SPL"
	TokenStandardTRC20       TokenStandard = "TOKEN_STANDARD_TRC20"
)

type FinalityStatus string

const (
	FinalityStatusUnspecified FinalityStatus = "FINALITY_STATUS_UNSPECIFIED"
	FinalityStatusPending     FinalityStatus = "FINALITY_STATUS_PENDING"
	FinalityStatusAccepted    FinalityStatus = "FINALITY_STATUS_ACCEPTED"
	FinalityStatusConfirmed   FinalityStatus = "FINALITY_STATUS_CONFIRMED"
	FinalityStatusFinalized   FinalityStatus = "FINALITY_STATUS_FINALIZED"
	FinalityStatusReorged     FinalityStatus = "FINALITY_STATUS_REORGED"
)

type NodeStatus string

const (
	NodeStatusUnspecified NodeStatus = "NODE_STATUS_UNSPECIFIED"
	NodeStatusHealthy     NodeStatus = "NODE_STATUS_HEALTHY"
	NodeStatusDegraded    NodeStatus = "NODE_STATUS_DEGRADED"
	NodeStatusUnhealthy   NodeStatus = "NODE_STATUS_UNHEALTHY"
	NodeStatusOffline     NodeStatus = "NODE_STATUS_OFFLINE"
)

// NormalizedEvent mirrors the blockchain ChainEvent contract shape without
// importing generated proto code into this early service skeleton.
type NormalizedEvent struct {
	Chain           string         `json:"chain"`
	Network         string         `json:"network"`
	ChainID         uint64         `json:"chain_id"`
	BlockNumber     uint64         `json:"block_number"`
	BlockHash       string         `json:"block_hash"`
	BlockTimestamp  time.Time      `json:"block_timestamp"`
	TxHash          string         `json:"tx_hash"`
	TxIndex         uint32         `json:"tx_index"`
	LogIndex        uint32         `json:"log_index"`
	EventType       string         `json:"event_type"`
	FromAddress     string         `json:"from_address"`
	ToAddress       string         `json:"to_address"`
	ContractAddress string         `json:"contract_address"`
	TokenAddress    string         `json:"token_address"`
	TokenSymbol     string         `json:"token_symbol"`
	TokenStandard   TokenStandard  `json:"token_standard"`
	AmountRaw       string         `json:"amount_raw"`
	AmountDecimal   string         `json:"amount_decimal"`
	AmountUSD       string         `json:"amount_usd,omitempty"`
	GasUsed         uint64         `json:"gas_used"`
	GasPrice        string         `json:"gas_price"`
	Success         bool           `json:"success"`
	FinalityStatus  FinalityStatus `json:"finality_status"`
	Confirmations   uint32         `json:"confirmations"`
	SourceRPC       string         `json:"source_rpc"`
	Reorged         bool           `json:"reorged"`
}

type BlockHeader struct {
	Chain      string    `json:"chain"`
	Network    string    `json:"network"`
	Height     uint64    `json:"height"`
	Hash       string    `json:"hash"`
	ParentHash string    `json:"parent_hash"`
	Timestamp  time.Time `json:"timestamp"`
	SourceRPC  string    `json:"source_rpc"`
}

type NodeHealth struct {
	NodeID         string            `json:"node_id"`
	Chain          string            `json:"chain"`
	Network        string            `json:"network"`
	ChainID        uint64            `json:"chain_id"`
	RPCEndpointID  string            `json:"rpc_endpoint_id"`
	SourceRPC      string            `json:"source_rpc"`
	Status         NodeStatus        `json:"status"`
	Synced         bool              `json:"synced"`
	LatestBlock    uint64            `json:"latest_block"`
	FinalizedBlock uint64            `json:"finalized_block"`
	BlockLag       uint64            `json:"block_lag"`
	PeerCount      uint32            `json:"peer_count"`
	LatencyMS      uint32            `json:"latency_ms"`
	ErrorRate      float64           `json:"error_rate"`
	ClientVersion  string            `json:"client_version"`
	LastError      string            `json:"last_error,omitempty"`
	CheckedAt      time.Time         `json:"checked_at"`
	Warnings       []string          `json:"warnings,omitempty"`
	Attributes     map[string]string `json:"attributes,omitempty"`
}
