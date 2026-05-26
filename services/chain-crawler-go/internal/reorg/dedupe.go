package reorg

import (
	"strconv"
	"strings"
	"sync"

	"telemetry-fabric/services/chain-crawler-go/pkg/event"
)

func EVMEventDedupeKey(evt event.NormalizedEvent) string {
	return joinKey(
		strconv.FormatUint(evt.ChainID, 10),
		evt.BlockHash,
		evt.TxHash,
		strconv.FormatUint(uint64(evt.LogIndex), 10),
	)
}

func EVMEventIdempotencyKey(evt event.NormalizedEvent) string {
	return EVMEventDedupeKey(evt)
}

func EventDedupeKey(evt event.NormalizedEvent) string {
	return EVMEventDedupeKey(evt)
}

func TransactionDedupeKey(evt event.NormalizedEvent) string {
	return joinKey(
		strconv.FormatUint(evt.ChainID, 10),
		evt.BlockHash,
		evt.TxHash,
	)
}

func TransactionIdempotencyKey(evt event.NormalizedEvent) string {
	return TransactionDedupeKey(evt)
}

type DuplicateDetector struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

func NewDuplicateDetector() *DuplicateDetector {
	return &DuplicateDetector{seen: make(map[string]struct{})}
}

func (d *DuplicateDetector) Seen(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, exists := d.seen[key]
	return exists
}

func (d *DuplicateDetector) Remember(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.seen[key]; exists {
		return false
	}
	d.seen[key] = struct{}{}
	return true
}

func (d *DuplicateDetector) IsDuplicate(key string) bool {
	return !d.Remember(key)
}

func (d *DuplicateDetector) CheckAndRemember(key string) bool {
	return d.IsDuplicate(key)
}

func (d *DuplicateDetector) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seen = make(map[string]struct{})
}

func joinKey(parts ...string) string {
	escaped := make([]string, 0, len(parts))
	for _, part := range parts {
		escaped = append(escaped, strings.ReplaceAll(part, "|", "||"))
	}
	return strings.Join(escaped, "|")
}
