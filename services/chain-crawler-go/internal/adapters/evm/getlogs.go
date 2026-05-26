package evm

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const (
	FinalityStatusPending   = "FINALITY_STATUS_PENDING"
	FinalityStatusFinalized = "FINALITY_STATUS_FINALIZED"
	FinalityStatusReorged   = "FINALITY_STATUS_REORGED"
)

type FailedLogChunk struct {
	FromBlock uint64       `json:"from_block"`
	ToBlock   uint64       `json:"to_block"`
	Filter    LogFilter    `json:"filter"`
	SourceRPC string       `json:"source_rpc,omitempty"`
	Attempts  int          `json:"attempts"`
	ErrorKind RPCErrorKind `json:"error_kind"`
	Error     string       `json:"error"`
}

type RangeFetchError struct {
	FailedChunks []FailedLogChunk `json:"failed_chunks"`
}

func (e *RangeFetchError) Error() string {
	if e == nil || len(e.FailedChunks) == 0 {
		return ""
	}
	if len(e.FailedChunks) == 1 {
		chunk := e.FailedChunks[0]
		return fmt.Sprintf("evm adapter: 1 eth_getLogs chunk failed (%d..%d, %s)", chunk.FromBlock, chunk.ToBlock, chunk.ErrorKind)
	}
	return fmt.Sprintf("evm adapter: %d eth_getLogs chunks failed", len(e.FailedChunks))
}

type sourcedRPCLog struct {
	log       rpcLog
	sourceRPC string
}

func (a *Adapter) fetchLogsAdaptive(ctx context.Context, fromBlock, toBlock uint64, filter LogFilter) ([]sourcedRPCLog, []FailedLogChunk) {
	chunkSize := a.cfg.InitialBlockRange
	if chunkSize == 0 {
		chunkSize = a.cfg.MaxBlockRange
	}
	chunkSize = clampUint64(chunkSize, a.cfg.MinBlockRange, a.cfg.MaxBlockRange)

	var logs []sourcedRPCLog
	var failures []FailedLogChunk
	for chunkStart := fromBlock; chunkStart <= toBlock; {
		if err := ctx.Err(); err != nil {
			failures = append(failures, FailedLogChunk{
				FromBlock: chunkStart,
				ToBlock:   toBlock,
				Filter:    filter,
				Attempts:  0,
				ErrorKind: ClassifyRPCError(err),
				Error:     err.Error(),
			})
			break
		}

		chunkEnd := boundedChunkEnd(chunkStart, toBlock, chunkSize)
		chunkLogs, sourceRPC, attempts, err := a.getLogsWithRetry(ctx, chunkStart, chunkEnd, filter)
		if err == nil {
			for _, log := range chunkLogs {
				logs = append(logs, sourcedRPCLog{log: log, sourceRPC: sourceRPC})
			}
			chunkSize = growChunkSize(chunkSize, a.cfg.MaxBlockRange, a.cfg.AdaptiveGrowFactor)
			if chunkEnd == toBlock {
				break
			}
			chunkStart = chunkEnd + 1
			continue
		}

		kind := ClassifyRPCError(err)
		if kind == RPCErrorBlockRangeTooLarge && chunkSize > a.cfg.MinBlockRange {
			nextSize := (chunkSize + 1) / 2
			if nextSize == chunkSize {
				nextSize--
			}
			if nextSize < a.cfg.MinBlockRange {
				nextSize = a.cfg.MinBlockRange
			}
			if nextSize < chunkSize {
				chunkSize = nextSize
				continue
			}
		}

		failures = append(failures, FailedLogChunk{
			FromBlock: chunkStart,
			ToBlock:   chunkEnd,
			Filter:    filter,
			SourceRPC: sourceRPC,
			Attempts:  attempts,
			ErrorKind: kind,
			Error:     err.Error(),
		})
		if chunkEnd == toBlock {
			break
		}
		chunkStart = chunkEnd + 1
	}
	return logs, failures
}

func (a *Adapter) getLogsWithRetry(ctx context.Context, fromBlock, toBlock uint64, filter LogFilter) ([]rpcLog, string, int, error) {
	attempts := 0
	retries := 0
	backoff := a.cfg.GetLogsRetryBackoff
	var sourceRPC string
	for {
		attempts++
		logs, source, err := a.getLogs(ctx, fromBlock, toBlock, filter)
		sourceRPC = source
		if err == nil {
			return logs, sourceRPC, attempts, nil
		}
		if ctx.Err() != nil {
			return nil, sourceRPC, attempts, ctx.Err()
		}
		if ClassifyRPCError(err) == RPCErrorBlockRangeTooLarge || !isRetryableRPCError(err) || retries >= a.cfg.MaxGetLogsRetries {
			return nil, sourceRPC, attempts, err
		}
		retries++
		if err := sleepWithContext(ctx, backoff); err != nil {
			return nil, sourceRPC, attempts, err
		}
		backoff = growBackoff(backoff, a.cfg.GetLogsRetryMaxBackoff)
	}
}

func (a *Adapter) latestBlockForFinality(ctx context.Context, fallback uint64) uint64 {
	var result string
	if _, err := a.rpc.call(ctx, "eth_blockNumber", nil, &result); err != nil {
		return fallback
	}
	latest, err := parseHexUint(result)
	if err != nil || latest < fallback {
		return fallback
	}
	return latest
}

func (a *Adapter) applyFinality(event *NormalizedChainEvent, latest uint64) {
	if event == nil {
		return
	}
	if event.Reorged {
		event.FinalityStatus = FinalityStatusReorged
		return
	}
	if latest >= event.BlockNumber {
		diff := latest - event.BlockNumber
		if diff > uint64(^uint32(0)-1) {
			event.Confirmations = ^uint32(0)
		} else {
			event.Confirmations = uint32(diff + 1)
		}
	}
	event.FinalityStatus = statusForBlock(event.BlockNumber, latest, a.cfg.FinalityDepth, false)
}

func (a *Adapter) malformedLogEvent(log rpcLog, sourceRPC string, decodeErr error, latest uint64) NormalizedChainEvent {
	event, err := a.baseEvent(log, sourceRPC)
	if err != nil {
		event = NormalizedChainEvent{
			Chain:         a.cfg.ChainName,
			Network:       a.cfg.Network,
			ChainID:       a.cfg.ChainID,
			BlockHash:     normalizeHex(log.BlockHash),
			TxHash:        normalizeHex(log.TransactionHash),
			Contract:      normalizeAddress(log.Address),
			TokenAddress:  normalizeAddress(log.Address),
			TokenStandard: TokenStandardUnspecified,
			SourceRPC:     sourceRPC,
			Reorged:       log.Removed,
			ObservedAt:    time.Now().UTC(),
		}
		if blockNumber, parseErr := parseHexUint(log.BlockNumber); parseErr == nil {
			event.BlockNumber = blockNumber
		}
		if logIndex, parseErr := parseHexUint(log.LogIndex); parseErr == nil {
			event.LogIndex = logIndex
		}
	}
	event.Contract = normalizeAddress(log.Address)
	event.TokenAddress = normalizeAddress(log.Address)
	if event.TokenStandard == "" {
		event.TokenStandard = TokenStandardUnspecified
	}
	event.EventType = "decode_error"
	event.DiscardReason = "malformed_log"
	event.DecodeError = decodeErr.Error()
	a.applyFinality(&event, latest)
	return event
}

func logDedupeKey(log rpcLog) string {
	return strings.Join([]string{
		normalizeHex(log.BlockHash),
		normalizeHex(log.TransactionHash),
		normalizeHex(log.LogIndex),
		fmt.Sprintf("%t", log.Removed),
	}, "|")
}

func boundedChunkEnd(start, limit, size uint64) uint64 {
	end := start + size - 1
	if end > limit || end < start {
		return limit
	}
	return end
}

func growChunkSize(current, max, factor uint64) uint64 {
	if factor < 2 || current >= max {
		return current
	}
	next := current * factor
	if next < current || next > max {
		return max
	}
	return next
}

func growBackoff(current, max time.Duration) time.Duration {
	if current <= 0 {
		return 0
	}
	next := current * 2
	if max > 0 && next > max {
		return max
	}
	return next
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func clampUint64(value, min, max uint64) uint64 {
	if max > 0 && value > max {
		value = max
	}
	if min > 0 && value < min {
		value = min
	}
	return value
}
