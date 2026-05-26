package checkpoint

import (
	"context"
	"sync"
	"time"
)

type FinalityStatus string

const (
	FinalityStatusPending   FinalityStatus = "FINALITY_STATUS_PENDING"
	FinalityStatusFinalized FinalityStatus = "FINALITY_STATUS_FINALIZED"
)

type Checkpoint struct {
	Chain           string            `json:"chain"`
	Network         string            `json:"network"`
	Height          uint64            `json:"height"`
	PendingHeight   uint64            `json:"pending_height,omitempty"`
	FinalizedHeight uint64            `json:"finalized_height,omitempty"`
	Cursor          string            `json:"cursor"`
	FinalityStatus  FinalityStatus    `json:"finality_status,omitempty"`
	UpdatedAt       time.Time         `json:"updated_at"`
	Attributes      map[string]string `json:"attributes,omitempty"`
}

type Store interface {
	Load(ctx context.Context, chain string, network string) (Checkpoint, bool, error)
	Save(ctx context.Context, checkpoint Checkpoint) error
}

type RecoveryStore interface {
	Store
	LoadAll(ctx context.Context) ([]Checkpoint, error)
	Recover(ctx context.Context) ([]Checkpoint, error)
}

type MemoryStore struct {
	mu          sync.RWMutex
	checkpoints map[string]Checkpoint
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{checkpoints: make(map[string]Checkpoint)}
}

func (s *MemoryStore) Load(ctx context.Context, chain string, network string) (Checkpoint, bool, error) {
	if err := ctx.Err(); err != nil {
		return Checkpoint{}, false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	cp, ok := s.checkpoints[key(chain, network)]
	return cp, ok, nil
}

func (s *MemoryStore) Save(ctx context.Context, cp Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	cp.UpdatedAt = time.Now().UTC()
	if cp.Cursor == "" {
		cp.Cursor = cp.Chain + "/" + cp.Network + "/block/" + formatHeight(cp.Height)
	}
	if cp.PendingHeight == 0 {
		cp.PendingHeight = cp.Height
	}
	if cp.FinalityStatus == "" {
		cp.FinalityStatus = statusFor(cp.Height, cp.FinalizedHeight)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoints[key(cp.Chain, cp.Network)] = cp
	return nil
}

func (s *MemoryStore) LoadAll(ctx context.Context) ([]Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	checkpoints := make([]Checkpoint, 0, len(s.checkpoints))
	for _, cp := range s.checkpoints {
		checkpoints = append(checkpoints, cp)
	}
	return checkpoints, nil
}

func (s *MemoryStore) Recover(ctx context.Context) ([]Checkpoint, error) {
	return s.LoadAll(ctx)
}

func SafeFinalizedHeight(latestHeight uint64, finalityDepth uint64, reorgWindow uint64) uint64 {
	depth := finalityDepth
	if reorgWindow > depth {
		depth = reorgWindow
	}
	if latestHeight <= depth {
		return 0
	}
	return latestHeight - depth
}

func FinalityForHeight(height uint64, finalizedHeight uint64) FinalityStatus {
	return statusFor(height, finalizedHeight)
}

func statusFor(height uint64, finalizedHeight uint64) FinalityStatus {
	if height > 0 && height <= finalizedHeight {
		return FinalityStatusFinalized
	}
	return FinalityStatusPending
}

func key(chain string, network string) string {
	return chain + ":" + network
}

func formatHeight(height uint64) string {
	if height == 0 {
		return "0"
	}

	var digits [20]byte
	i := len(digits)
	for height > 0 {
		i--
		digits[i] = byte('0' + height%10)
		height /= 10
	}
	return string(digits[i:])
}
