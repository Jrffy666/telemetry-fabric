package metrics

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

type metricKind string

const (
	kindGauge   metricKind = "gauge"
	kindCounter metricKind = "counter"
)

type descriptor struct {
	name string
	help string
	kind metricKind
}

var descriptors = []descriptor{
	{name: "crawler_chain_head_height", help: "Latest chain head height observed by the crawler.", kind: kindGauge},
	{name: "crawler_processed_height", help: "Highest block height processed by the crawler.", kind: kindGauge},
	{name: "crawler_lag_blocks", help: "Block lag between chain head and processed checkpoint.", kind: kindGauge},
	{name: "crawler_lag_seconds", help: "Approximate time lag between chain head and processed checkpoint.", kind: kindGauge},
	{name: "crawler_rpc_latency_ms", help: "Last observed RPC latency in milliseconds.", kind: kindGauge},
	{name: "crawler_rpc_errors_total", help: "Total RPC errors observed by the crawler.", kind: kindCounter},
	{name: "crawler_rpc_rate_limited_total", help: "Total RPC rate limit events observed by the crawler.", kind: kindCounter},
	{name: "crawler_reorg_events_total", help: "Total reorg events observed by the crawler.", kind: kindCounter},
	{name: "crawler_discarded_events_total", help: "Total normalized events discarded by filters.", kind: kindCounter},
	{name: "crawler_kept_events_total", help: "Total normalized events kept after filters.", kind: kindCounter},
	{name: "crawler_checkpoint_height", help: "Persisted checkpoint height.", kind: kindGauge},
	{name: "crawler_block_gap_total", help: "Total detected block gaps.", kind: kindCounter},
	{name: "crawler_duplicate_events_total", help: "Total duplicate normalized events detected before export.", kind: kindCounter},
	{name: "crawler_ws_reconnect_total", help: "Total websocket reconnects.", kind: kindCounter},
	{name: "crawler_worker_inflight", help: "Current number of worker tasks in flight.", kind: kindGauge},
	{name: "crawler_exporter_backpressure_total", help: "Total exporter backpressure events by policy and action.", kind: kindCounter},
}

type Registry struct {
	mu      sync.RWMutex
	values  map[string]float64
	labels  map[string]string
	ordered []string
}

func NewRegistry() *Registry {
	return &Registry{
		values: make(map[string]float64),
		labels: make(map[string]string),
	}
}

func (r *Registry) SetGauge(name string, labels map[string]string, value float64) {
	r.set(name, labels, value)
}

func (r *Registry) IncCounter(name string, labels map[string]string, delta float64) {
	if delta == 0 {
		return
	}

	key := metricKey(name, labels)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.values[key]; !exists {
		r.ordered = append(r.ordered, key)
		r.labels[key] = labelString(labels)
	}
	r.values[key] += delta
}

func (r *Registry) set(name string, labels map[string]string, value float64) {
	key := metricKey(name, labels)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.values[key]; !exists {
		r.ordered = append(r.ordered, key)
		r.labels[key] = labelString(labels)
	}
	r.values[key] = value
}

func (r *Registry) WritePrometheus(w io.Writer) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, desc := range descriptors {
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", desc.name, desc.help, desc.name, desc.kind); err != nil {
			return err
		}
		for _, key := range r.ordered {
			name := strings.SplitN(key, "{", 2)[0]
			if name != desc.name {
				continue
			}
			if _, err := fmt.Fprintf(w, "%s%s %v\n", desc.name, r.labels[key], r.values[key]); err != nil {
				return err
			}
		}
	}
	return nil
}

func ChainLabels(chain string, network string) map[string]string {
	return map[string]string{"chain": chain, "network": network}
}

func metricKey(name string, labels map[string]string) string {
	return name + labelString(labels)
}

func labelString(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}

	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString("{")
	for i, key := range keys {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(key)
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(labels[key]))
		b.WriteString(`"`)
	}
	b.WriteString("}")
	return b.String()
}

func escapeLabelValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}
