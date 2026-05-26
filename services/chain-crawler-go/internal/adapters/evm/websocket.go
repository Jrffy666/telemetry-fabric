package evm

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SubscriptionTransport is intentionally small so the crawler runtime can plug
// in its preferred WebSocket implementation without making this adapter depend
// on a specific third-party package.
type SubscriptionTransport interface {
	Subscribe(ctx context.Context, endpoint EndpointConfig, method string, params []interface{}) (<-chan json.RawMessage, func(), error)
}

func (a *Adapter) SubscribeNewHeads(ctx context.Context, transport SubscriptionTransport) (<-chan BlockHeader, func(), error) {
	if transport == nil {
		return nil, nil, ErrNoTransport
	}
	if len(a.cfg.WSEndpoints) == 0 {
		return nil, nil, ErrNoWSEndpoints
	}

	runCtx, cancel := context.WithCancel(ctx)
	handle := newSubscriptionHandle(cancel)
	rawCh, unsubscribe, err := a.subscribeRaw(runCtx, transport, "newHeads", nil)
	if err != nil {
		handle.stop()
		return nil, unsubscribe, err
	}
	handle.set(unsubscribe)

	out := make(chan BlockHeader, 64)
	go func() {
		defer close(out)
		defer handle.stop()
		a.runNewHeadsSubscription(runCtx, transport, rawCh, handle, out)
	}()

	return out, handle.stop, nil
}

func (a *Adapter) SubscribeLogs(ctx context.Context, transport SubscriptionTransport, filter LogFilter) (<-chan NormalizedChainEvent, func(), error) {
	if transport == nil {
		return nil, nil, ErrNoTransport
	}
	if len(a.cfg.WSEndpoints) == 0 {
		return nil, nil, ErrNoWSEndpoints
	}

	params := make(map[string]interface{})
	applyLogFilter(params, filter)
	runCtx, cancel := context.WithCancel(ctx)
	handle := newSubscriptionHandle(cancel)
	rawCh, unsubscribe, err := a.subscribeRaw(runCtx, transport, "logs", params)
	if err != nil {
		handle.stop()
		return nil, unsubscribe, err
	}
	handle.set(unsubscribe)

	out := make(chan NormalizedChainEvent, 128)
	go func() {
		defer close(out)
		defer handle.stop()
		a.runLogsSubscription(runCtx, transport, filter, params, rawCh, handle, out)
	}()

	return out, handle.stop, nil
}

func (a *Adapter) runNewHeadsSubscription(ctx context.Context, transport SubscriptionTransport, rawCh <-chan json.RawMessage, handle *subscriptionHandle, out chan<- BlockHeader) {
	backoff := a.cfg.ReconnectInitialBackoff
	var last BlockHeader
	var hasLast bool

	for {
		if rawCh == nil {
			if err := sleepWithContext(ctx, backoff); err != nil {
				return
			}
			var unsubscribe func()
			var err error
			rawCh, unsubscribe, err = a.subscribeRaw(ctx, transport, "newHeads", nil)
			if err != nil {
				backoff = growBackoff(backoff, a.cfg.ReconnectMaxBackoff)
				continue
			}
			handle.set(unsubscribe)
			backoff = a.cfg.ReconnectInitialBackoff
			if hasLast {
				if !a.emitMissingHeaders(ctx, out, &last, last.Number+1, 0) {
					return
				}
			}
		}

		disconnected := a.consumeNewHeads(ctx, rawCh, out, &last, &hasLast)
		if !disconnected || ctx.Err() != nil {
			return
		}
		handle.closeCurrent()
		rawCh = nil
		backoff = growBackoff(backoff, a.cfg.ReconnectMaxBackoff)
	}
}

func (a *Adapter) consumeNewHeads(ctx context.Context, rawCh <-chan json.RawMessage, out chan<- BlockHeader, last *BlockHeader, hasLast *bool) bool {
	timer := time.NewTimer(a.cfg.HeartbeatInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		case raw, ok := <-rawCh:
			if !ok {
				return true
			}
			resetTimer(timer, a.cfg.HeartbeatInterval)

			var header rpcBlock
			if err := json.Unmarshal(raw, &header); err != nil {
				continue
			}
			normalized, err := a.blockHeaderFromRPC(header, endpointName(a.cfg.WSEndpoints[0]))
			if err != nil {
				continue
			}
			if *hasLast && normalized.Number > last.Number+1 {
				if !a.emitMissingHeaders(ctx, out, last, last.Number+1, normalized.Number-1) {
					return false
				}
			}
			if *hasLast && normalized.Number <= last.Number && sameHex(normalized.Hash, last.Hash) {
				continue
			}
			if a.finality != nil {
				a.finality.Observe(normalized)
			}
			if !sendHeader(ctx, out, normalized) {
				return false
			}
			*last = normalized
			*hasLast = true
		}
	}
}

func (a *Adapter) runLogsSubscription(ctx context.Context, transport SubscriptionTransport, filter LogFilter, params map[string]interface{}, rawCh <-chan json.RawMessage, handle *subscriptionHandle, out chan<- NormalizedChainEvent) {
	backoff := a.cfg.ReconnectInitialBackoff
	seen := newEventDeduper(8192)
	var lastBlock uint64

	for {
		if rawCh == nil {
			if err := sleepWithContext(ctx, backoff); err != nil {
				return
			}
			var unsubscribe func()
			var err error
			rawCh, unsubscribe, err = a.subscribeRaw(ctx, transport, "logs", params)
			if err != nil {
				backoff = growBackoff(backoff, a.cfg.ReconnectMaxBackoff)
				continue
			}
			handle.set(unsubscribe)
			backoff = a.cfg.ReconnectInitialBackoff
			if lastBlock > 0 {
				if !a.emitMissingLogEvents(ctx, out, filter, seen, &lastBlock) {
					return
				}
			}
		}

		disconnected := a.consumeLogs(ctx, rawCh, out, seen, &lastBlock)
		if !disconnected || ctx.Err() != nil {
			return
		}
		handle.closeCurrent()
		rawCh = nil
		backoff = growBackoff(backoff, a.cfg.ReconnectMaxBackoff)
	}
}

func (a *Adapter) consumeLogs(ctx context.Context, rawCh <-chan json.RawMessage, out chan<- NormalizedChainEvent, seen *eventDeduper, lastBlock *uint64) bool {
	timer := time.NewTimer(a.cfg.HeartbeatInterval)
	defer timer.Stop()
	sourceRPC := endpointName(a.cfg.WSEndpoints[0])

	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		case raw, ok := <-rawCh:
			if !ok {
				return true
			}
			resetTimer(timer, a.cfg.HeartbeatInterval)
			var log rpcLog
			if err := json.Unmarshal(raw, &log); err != nil {
				continue
			}
			events, err := a.decodeLog(log, sourceRPC)
			if err != nil {
				blockNumber, _ := parseHexUint(log.BlockNumber)
				event := a.malformedLogEvent(log, sourceRPC, err, blockNumber)
				if !sendEvent(ctx, out, seen, event) {
					return false
				}
				if event.BlockNumber > *lastBlock {
					*lastBlock = event.BlockNumber
				}
				continue
			}
			for _, event := range events {
				a.applyFinality(&event, event.BlockNumber)
				if !sendEvent(ctx, out, seen, event) {
					return false
				}
				if event.BlockNumber > *lastBlock {
					*lastBlock = event.BlockNumber
				}
			}
		}
	}
}

func (a *Adapter) subscribeRaw(ctx context.Context, transport SubscriptionTransport, subscription string, params map[string]interface{}) (<-chan json.RawMessage, func(), error) {
	args := []interface{}{subscription}
	if params != nil {
		args = append(args, params)
	}
	rawCh, unsubscribe, err := transport.Subscribe(ctx, a.cfg.WSEndpoints[0], "eth_subscribe", args)
	if err != nil {
		return nil, unsubscribe, rpcErr(RPCErrorWebSocketDisconnect, a.cfg.WSEndpoints[0], "eth_subscribe", err.Error(), err)
	}
	return rawCh, unsubscribe, nil
}

func (a *Adapter) emitMissingHeaders(ctx context.Context, out chan<- BlockHeader, last *BlockHeader, from uint64, to uint64) bool {
	if from == 0 {
		return true
	}
	if to == 0 {
		latest, err := a.BlockNumber(ctx)
		if err != nil || latest < from {
			return true
		}
		to = latest
	}
	for number := from; number <= to; number++ {
		header, err := a.GetBlockByNumber(ctx, number)
		if err != nil {
			continue
		}
		if last != nil && header.Number <= last.Number && sameHex(header.Hash, last.Hash) {
			continue
		}
		if a.finality != nil {
			a.finality.Observe(header)
		}
		if !sendHeader(ctx, out, header) {
			return false
		}
		if last != nil {
			*last = header
		}
		if number == to {
			break
		}
	}
	return true
}

func (a *Adapter) emitMissingLogEvents(ctx context.Context, out chan<- NormalizedChainEvent, filter LogFilter, seen *eventDeduper, lastBlock *uint64) bool {
	latest, err := a.BlockNumber(ctx)
	if err != nil || latest <= *lastBlock {
		return true
	}
	logs, _ := a.fetchLogsAdaptive(ctx, *lastBlock+1, latest, filter)
	for _, sourced := range logs {
		events, err := a.decodeLog(sourced.log, sourced.sourceRPC)
		if err != nil {
			event := a.malformedLogEvent(sourced.log, sourced.sourceRPC, err, latest)
			if !sendEvent(ctx, out, seen, event) {
				return false
			}
			if event.BlockNumber > *lastBlock {
				*lastBlock = event.BlockNumber
			}
			continue
		}
		for _, event := range events {
			a.applyFinality(&event, latest)
			if !sendEvent(ctx, out, seen, event) {
				return false
			}
			if event.BlockNumber > *lastBlock {
				*lastBlock = event.BlockNumber
			}
		}
	}
	return true
}

type subscriptionHandle struct {
	mu          sync.Mutex
	cancel      context.CancelFunc
	unsubscribe func()
}

func newSubscriptionHandle(cancel context.CancelFunc) *subscriptionHandle {
	return &subscriptionHandle{cancel: cancel}
}

func (h *subscriptionHandle) set(unsubscribe func()) {
	h.mu.Lock()
	previous := h.unsubscribe
	h.unsubscribe = unsubscribe
	h.mu.Unlock()
	if previous != nil {
		previous()
	}
}

func (h *subscriptionHandle) closeCurrent() {
	h.mu.Lock()
	unsubscribe := h.unsubscribe
	h.unsubscribe = nil
	h.mu.Unlock()
	if unsubscribe != nil {
		unsubscribe()
	}
}

func (h *subscriptionHandle) stop() {
	if h.cancel != nil {
		h.cancel()
	}
	h.closeCurrent()
}

type eventDeduper struct {
	limit int
	seen  map[string]struct{}
	order []string
}

func newEventDeduper(limit int) *eventDeduper {
	return &eventDeduper{
		limit: limit,
		seen:  make(map[string]struct{}),
	}
}

func (d *eventDeduper) add(key string) bool {
	if d == nil {
		return true
	}
	if _, ok := d.seen[key]; ok {
		return false
	}
	d.seen[key] = struct{}{}
	d.order = append(d.order, key)
	if d.limit > 0 && len(d.order) > d.limit {
		oldest := d.order[0]
		d.order = d.order[1:]
		delete(d.seen, oldest)
	}
	return true
}

func sendHeader(ctx context.Context, out chan<- BlockHeader, header BlockHeader) bool {
	select {
	case <-ctx.Done():
		return false
	case out <- header:
		return true
	}
}

func sendEvent(ctx context.Context, out chan<- NormalizedChainEvent, seen *eventDeduper, event NormalizedChainEvent) bool {
	if !seen.add(eventDedupeKey(event)) {
		return true
	}
	select {
	case <-ctx.Done():
		return false
	case out <- event:
		return true
	}
}

func eventDedupeKey(event NormalizedChainEvent) string {
	return strings.Join([]string{
		strconv.FormatUint(event.ChainID, 10),
		normalizeHex(event.BlockHash),
		normalizeHex(event.TxHash),
		strconv.FormatUint(event.LogIndex, 10),
		strconv.FormatUint(uint64(event.BatchIndex), 10),
		strconv.FormatBool(event.Reorged),
	}, "|")
}

func resetTimer(timer *time.Timer, delay time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(delay)
}
