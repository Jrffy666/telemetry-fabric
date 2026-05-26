package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type FileStore struct {
	mu   sync.Mutex
	path string
}

type fileSnapshot struct {
	Checkpoints []Checkpoint `json:"checkpoints"`
}

func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

func OpenFileStore(path string) (*FileStore, error) {
	store := NewFileStore(path)
	if _, err := store.LoadAll(context.Background()); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *FileStore) Load(ctx context.Context, chain string, network string) (Checkpoint, bool, error) {
	if err := ctx.Err(); err != nil {
		return Checkpoint{}, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	checkpoints, err := s.loadLocked()
	if err != nil {
		return Checkpoint{}, false, err
	}
	cp, ok := checkpoints[key(chain, network)]
	return cp, ok, nil
}

func (s *FileStore) Save(ctx context.Context, cp Checkpoint) error {
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

	checkpoints, err := s.loadLocked()
	if err != nil {
		return err
	}
	checkpoints[key(cp.Chain, cp.Network)] = cp
	return s.saveLocked(checkpoints)
}

func (s *FileStore) LoadAll(ctx context.Context) ([]Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	checkpoints, err := s.loadLocked()
	if err != nil {
		return nil, err
	}
	all := make([]Checkpoint, 0, len(checkpoints))
	for _, cp := range checkpoints {
		all = append(all, cp)
	}
	return all, nil
}

func (s *FileStore) Recover(ctx context.Context) ([]Checkpoint, error) {
	return s.LoadAll(ctx)
}

func (s *FileStore) loadLocked() (map[string]Checkpoint, error) {
	checkpoints := make(map[string]Checkpoint)
	if s.path == "" {
		return checkpoints, errors.New("checkpoint file path is empty")
	}

	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return checkpoints, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return checkpoints, nil
	}

	var snapshot fileSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, err
	}
	for _, cp := range snapshot.Checkpoints {
		checkpoints[key(cp.Chain, cp.Network)] = cp
	}
	return checkpoints, nil
}

func (s *FileStore) saveLocked(checkpoints map[string]Checkpoint) error {
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	snapshot := fileSnapshot{Checkpoints: make([]Checkpoint, 0, len(checkpoints))}
	for _, cp := range checkpoints {
		snapshot.Checkpoints = append(snapshot.Checkpoints, cp)
	}

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	tmp, err := os.CreateTemp(dir, filepath.Base(s.path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		if removeErr := os.Remove(s.path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return err
		}
		return os.Rename(tmpPath, s.path)
	}
	return nil
}
