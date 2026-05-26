package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type ReadinessCheck func(context.Context) error

type HealthState struct {
	startedAt time.Time
	ready     atomic.Bool
	mu        sync.RWMutex
	checks    map[string]ReadinessCheck
}

func NewHealthState() *HealthState {
	return &HealthState{
		startedAt: time.Now().UTC(),
		checks:    make(map[string]ReadinessCheck),
	}
}

func (s *HealthState) SetReady(ready bool) {
	s.ready.Store(ready)
}

func (s *HealthState) Ready() bool {
	return s.ready.Load()
}

func (s *HealthState) AddCheck(name string, check ReadinessCheck) {
	if name == "" || check == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checks[name] = check
}

func (s *HealthState) Check(ctx context.Context) map[string]string {
	results := make(map[string]string)
	if !s.Ready() {
		results["state"] = "not_ready"
	}

	s.mu.RLock()
	checks := make(map[string]ReadinessCheck, len(s.checks))
	for name, check := range s.checks {
		checks[name] = check
	}
	s.mu.RUnlock()

	for name, check := range checks {
		if err := check(ctx); err != nil {
			results[name] = err.Error()
		} else {
			results[name] = "ok"
		}
	}
	return results
}

func RegisterHandlers(mux *http.ServeMux, registry *Registry, state *HealthState) {
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status":     "ok",
			"started_at": state.startedAt,
		})
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		checks := state.Check(ctx)
		if !ready(checks) {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"status": "not_ready",
				"checks": checks,
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "ready",
			"checks": checks,
		})
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		if err := registry.WritePrometheus(w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

func ready(checks map[string]string) bool {
	if len(checks) == 0 {
		return true
	}
	for _, result := range checks {
		if result != "ok" {
			return false
		}
	}
	return true
}

func ReadyError(message string) ReadinessCheck {
	return func(context.Context) error {
		return errors.New(message)
	}
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
