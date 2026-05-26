package evm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"
)

type Adapter struct {
	cfg      Config
	rpc      *rpcClient
	watched  map[string]WatchedContract
	filtered []LogFilter
	finality *FinalityTracker
}

func NewAdapter(cfg Config) (*Adapter, error) {
	if cfg.ChainName == "" {
		return nil, errors.New("evm adapter: chain name is required")
	}
	if cfg.Network == "" {
		return nil, errors.New("evm adapter: network is required")
	}
	if cfg.ChainID == 0 {
		return nil, errors.New("evm adapter: chain_id is required")
	}
	if len(cfg.RPCEndpoints) == 0 {
		return nil, ErrNoRPCEndpoints
	}
	cfg = applyConfigDefaults(cfg)

	watched := make(map[string]WatchedContract, len(cfg.WatchedContracts))
	for _, contract := range cfg.WatchedContracts {
		if contract.Address == "" {
			continue
		}
		contract.Address = normalizeAddress(contract.Address)
		if contract.TokenStandard == "" {
			contract.TokenStandard = TokenStandardUnspecified
		}
		watched[contract.Address] = contract
	}

	return &Adapter{
		cfg:      cfg,
		rpc:      newRPCClient(cfg),
		watched:  watched,
		filtered: buildLogFilters(cfg),
		finality: NewFinalityTracker(cfg),
	}, nil
}

func (a *Adapter) BlockNumber(ctx context.Context) (uint64, error) {
	var result string
	_, err := a.rpc.call(ctx, "eth_blockNumber", nil, &result)
	if err != nil {
		return 0, err
	}
	return parseHexUint(result)
}

func (a *Adapter) FetchRange(ctx context.Context, fromBlock, toBlock uint64) ([]NormalizedChainEvent, error) {
	if toBlock < fromBlock {
		return nil, fmt.Errorf("evm adapter: invalid block range %d..%d", fromBlock, toBlock)
	}

	latest := a.latestBlockForFinality(ctx, toBlock)
	var all []NormalizedChainEvent
	var failures []FailedLogChunk
	seenLogs := make(map[string]struct{})
	for _, filter := range a.filtered {
		logs, chunkFailures := a.fetchLogsAdaptive(ctx, fromBlock, toBlock, filter)
		failures = append(failures, chunkFailures...)
		for _, sourced := range logs {
			key := logDedupeKey(sourced.log)
			if _, ok := seenLogs[key]; ok {
				continue
			}
			seenLogs[key] = struct{}{}

			events, err := a.decodeLog(sourced.log, sourced.sourceRPC)
			if err != nil {
				all = append(all, a.malformedLogEvent(sourced.log, sourced.sourceRPC, err, latest))
				continue
			}
			for _, event := range events {
				a.applyFinality(&event, latest)
				all = append(all, event)
			}
		}
	}
	if len(failures) > 0 {
		return all, &RangeFetchError{FailedChunks: failures}
	}
	return all, nil
}

func (a *Adapter) HealthCheck(ctx context.Context) (EndpointHealth, error) {
	if len(a.cfg.RPCEndpoints) == 0 {
		return EndpointHealth{}, ErrNoRPCEndpoints
	}
	return a.healthCheckEndpoint(ctx, a.cfg.RPCEndpoints[0])
}

func (a *Adapter) HealthCheckEndpoints(ctx context.Context) []EndpointHealth {
	results := make([]EndpointHealth, 0, len(a.cfg.RPCEndpoints))
	for _, endpoint := range a.cfg.RPCEndpoints {
		health, _ := a.healthCheckEndpoint(ctx, endpoint)
		results = append(results, health)
	}
	return results
}

func (a *Adapter) GetBlockByNumber(ctx context.Context, number uint64) (BlockHeader, error) {
	var raw rpcBlock
	sourceRPC, err := a.rpc.call(ctx, "eth_getBlockByNumber", []interface{}{quantity(number), false}, &raw)
	if err != nil {
		return BlockHeader{}, err
	}
	return a.blockHeaderFromRPC(raw, sourceRPC)
}

func (a *Adapter) GetTransactionReceipt(ctx context.Context, txHash string) (TransactionReceipt, error) {
	var raw rpcReceipt
	_, err := a.rpc.call(ctx, "eth_getTransactionReceipt", []interface{}{normalizeHex(txHash)}, &raw)
	if err != nil {
		return TransactionReceipt{}, err
	}
	blockNumber, err := parseHexUint(raw.BlockNumber)
	if err != nil {
		return TransactionReceipt{}, err
	}
	statusValue, err := parseHexUint(raw.Status)
	if err != nil {
		return TransactionReceipt{}, err
	}
	return TransactionReceipt{
		TxHash:      normalizeHex(raw.TransactionHash),
		BlockNumber: blockNumber,
		BlockHash:   normalizeHex(raw.BlockHash),
		Status:      statusValue == 1,
	}, nil
}

func (a *Adapter) GetBalance(ctx context.Context, address, blockTag string) (string, error) {
	if blockTag == "" {
		blockTag = "latest"
	}
	var result string
	_, err := a.rpc.call(ctx, "eth_getBalance", []interface{}{normalizeAddress(address), blockTag}, &result)
	if err != nil {
		return "", err
	}
	return parseHexBigDecimalString(result)
}

func (a *Adapter) Call(ctx context.Context, request CallRequest, blockTag string) (string, error) {
	if blockTag == "" {
		blockTag = "latest"
	}
	var result string
	_, err := a.rpc.call(ctx, "eth_call", []interface{}{request, blockTag}, &result)
	if err != nil {
		return "", err
	}
	return normalizeHex(result), nil
}

func (a *Adapter) healthCheckEndpoint(ctx context.Context, endpoint EndpointConfig) (EndpointHealth, error) {
	start := time.Now()
	checkedAt := start.UTC()
	var result string
	_, err := a.rpc.callEndpoint(ctx, endpoint, "eth_blockNumber", nil, &result)
	latency := time.Since(start).Milliseconds()
	health := EndpointHealth{
		Endpoint:      endpointName(endpoint),
		LatencyMillis: latency,
		CheckedAt:     checkedAt,
		Attributes: map[string]string{
			"chain":   a.cfg.ChainName,
			"network": a.cfg.Network,
		},
	}
	if err != nil {
		health.Healthy = false
		health.Error = err.Error()
		return health, err
	}
	latest, err := parseHexUint(result)
	if err != nil {
		health.Error = err.Error()
		return health, err
	}
	health.Healthy = true
	health.LatestBlock = latest
	return health, nil
}

func (a *Adapter) getLogs(ctx context.Context, fromBlock, toBlock uint64, filter LogFilter) ([]rpcLog, string, error) {
	params := map[string]interface{}{
		"fromBlock": quantity(fromBlock),
		"toBlock":   quantity(toBlock),
	}
	applyLogFilter(params, filter)

	var logs []rpcLog
	sourceRPC, err := a.rpc.call(ctx, "eth_getLogs", []interface{}{params}, &logs)
	if err != nil {
		return nil, sourceRPC, err
	}
	return logs, sourceRPC, nil
}

func (a *Adapter) blockHeaderFromRPC(raw rpcBlock, sourceRPC string) (BlockHeader, error) {
	number, err := parseHexUint(raw.Number)
	if err != nil {
		return BlockHeader{}, err
	}
	timestampValue, err := parseHexUint(raw.Timestamp)
	if err != nil {
		return BlockHeader{}, err
	}
	return BlockHeader{
		Chain:      a.cfg.ChainName,
		Network:    a.cfg.Network,
		ChainID:    a.cfg.ChainID,
		Number:     number,
		Hash:       normalizeHex(raw.Hash),
		ParentHash: normalizeHex(raw.ParentHash),
		Timestamp:  time.Unix(int64(timestampValue), 0).UTC(),
		SourceRPC:  sourceRPC,
	}, nil
}

func buildLogFilters(cfg Config) []LogFilter {
	if len(cfg.LogFilters) > 0 {
		return cfg.LogFilters
	}

	addresses := make([]string, 0, len(cfg.WatchedContracts))
	for _, contract := range cfg.WatchedContracts {
		if contract.Address != "" {
			addresses = append(addresses, normalizeAddress(contract.Address))
		}
	}

	return []LogFilter{{
		Addresses: addresses,
		Topics: [][]string{{
			TopicTransfer,
			TopicERC1155TransferSingle,
			TopicERC1155TransferBatch,
		}},
	}}
}

func applyLogFilter(params map[string]interface{}, filter LogFilter) {
	if len(filter.Addresses) == 1 {
		params["address"] = normalizeAddress(filter.Addresses[0])
	} else if len(filter.Addresses) > 1 {
		addresses := make([]string, 0, len(filter.Addresses))
		for _, address := range filter.Addresses {
			addresses = append(addresses, normalizeAddress(address))
		}
		params["address"] = addresses
	}

	if len(filter.Topics) == 0 {
		return
	}
	topics := make([]interface{}, len(filter.Topics))
	for index, group := range filter.Topics {
		if len(group) == 0 {
			topics[index] = nil
			continue
		}
		if len(group) == 1 {
			topics[index] = normalizeHex(group[0])
			continue
		}
		values := make([]string, 0, len(group))
		for _, topic := range group {
			values = append(values, normalizeHex(topic))
		}
		topics[index] = values
	}
	params["topics"] = topics
}

type rpcLog struct {
	Address          string   `json:"address"`
	Topics           []string `json:"topics"`
	Data             string   `json:"data"`
	BlockNumber      string   `json:"blockNumber"`
	BlockHash        string   `json:"blockHash"`
	TransactionHash  string   `json:"transactionHash"`
	TransactionIndex string   `json:"transactionIndex"`
	LogIndex         string   `json:"logIndex"`
	Removed          bool     `json:"removed"`
}

type rpcBlock struct {
	Number     string `json:"number"`
	Hash       string `json:"hash"`
	ParentHash string `json:"parentHash"`
	Timestamp  string `json:"timestamp"`
}

type rpcReceipt struct {
	TransactionHash string          `json:"transactionHash"`
	BlockNumber     string          `json:"blockNumber"`
	BlockHash       string          `json:"blockHash"`
	Status          string          `json:"status"`
	Logs            json.RawMessage `json:"logs"`
}

func bigIntStringFromWord(word string) (string, error) {
	word = normalizeHex(word)
	if len(word) <= 2 {
		return "", fmt.Errorf("invalid ABI uint256 word %q", word)
	}
	value := new(big.Int)
	if _, ok := value.SetString(word[2:], 16); !ok {
		return "", fmt.Errorf("invalid ABI uint256 word %q", word)
	}
	return value.String(), nil
}
