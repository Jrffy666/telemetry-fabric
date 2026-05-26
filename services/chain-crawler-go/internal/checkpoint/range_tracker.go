package checkpoint

import (
	"errors"
	"sync"
)

var (
	ErrUnknownRange     = errors.New("checkpoint range is not assigned")
	ErrInvalidRange     = errors.New("checkpoint range is invalid")
	ErrDuplicateRangeID = errors.New("checkpoint range id already assigned")
)

type RangeTrackerOptions struct {
	StartHeight      uint64
	CheckpointHeight uint64
	BatchSize        uint64
	FinalityDepth    uint64
	ReorgWindow      uint64
}

type RangeAssignment struct {
	ID             string         `json:"id"`
	From           uint64         `json:"from"`
	To             uint64         `json:"to"`
	FinalityStatus FinalityStatus `json:"finality_status"`
}

type CheckpointAdvance struct {
	Advanced        bool           `json:"advanced"`
	Height          uint64         `json:"height"`
	PendingHeight   uint64         `json:"pending_height"`
	FinalizedHeight uint64         `json:"finalized_height"`
	FinalityStatus  FinalityStatus `json:"finality_status"`
	Gaps            []BlockGap     `json:"gaps,omitempty"`
}

type RangeTracker struct {
	mu              sync.Mutex
	nextHeight      uint64
	checkpoint      uint64
	batchSize       uint64
	finalityDepth   uint64
	reorgWindow     uint64
	latestTarget    uint64
	inFlight        map[string]RangeAssignment
	completedByFrom map[uint64]RangeAssignment
	gaps            []BlockGap
}

type BlockRangeTracker = RangeTracker

func NewRangeTracker(options RangeTrackerOptions) *RangeTracker {
	batchSize := options.BatchSize
	if batchSize == 0 {
		batchSize = 1
	}

	checkpointHeight := options.CheckpointHeight
	if checkpointHeight == 0 && options.StartHeight > 0 {
		checkpointHeight = options.StartHeight - 1
	}

	nextHeight := checkpointHeight + 1
	if nextHeight == 1 && options.StartHeight > 0 {
		nextHeight = options.StartHeight
	}

	return &RangeTracker{
		nextHeight:      nextHeight,
		checkpoint:      checkpointHeight,
		batchSize:       batchSize,
		finalityDepth:   options.FinalityDepth,
		reorgWindow:     options.ReorgWindow,
		inFlight:        make(map[string]RangeAssignment),
		completedByFrom: make(map[uint64]RangeAssignment),
	}
}

func NewBlockRangeTracker(options RangeTrackerOptions) *RangeTracker {
	return NewRangeTracker(options)
}

func (t *RangeTracker) Assign(targetHeight uint64) (RangeAssignment, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if targetHeight > t.latestTarget {
		t.latestTarget = targetHeight
	}
	if t.nextHeight > targetHeight {
		return RangeAssignment{}, false, nil
	}

	assignment := RangeAssignment{
		From: t.nextHeight,
		To:   minUint64(t.nextHeight+t.batchSize-1, targetHeight),
	}
	assignment.ID = RangeID(assignment.From, assignment.To)
	assignment.FinalityStatus = statusFor(assignment.To, SafeFinalizedHeight(t.latestTarget, t.finalityDepth, t.reorgWindow))

	if _, exists := t.inFlight[assignment.ID]; exists {
		return RangeAssignment{}, false, ErrDuplicateRangeID
	}
	t.inFlight[assignment.ID] = assignment
	t.nextHeight = assignment.To + 1
	return assignment, true, nil
}

func (t *RangeTracker) AssignRange(targetHeight uint64) (RangeAssignment, bool, error) {
	return t.Assign(targetHeight)
}

func (t *RangeTracker) Complete(id string, observedHeights ...uint64) (CheckpointAdvance, error) {
	if len(observedHeights) == 0 {
		return t.CompleteWithObservedHeights(id, nil)
	}
	return t.CompleteWithObservedHeights(id, observedHeights)
}

func (t *RangeTracker) CompleteRange(from uint64, to uint64, observedHeights ...uint64) (CheckpointAdvance, error) {
	return t.Complete(RangeID(from, to), observedHeights...)
}

func (t *RangeTracker) CompleteWithObservedHeights(id string, observedHeights []uint64) (CheckpointAdvance, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	assignment, exists := t.inFlight[id]
	if !exists {
		return t.currentAdvanceLocked(false, nil), ErrUnknownRange
	}
	if assignment.From > assignment.To {
		return t.currentAdvanceLocked(false, nil), ErrInvalidRange
	}

	if observedHeights != nil {
		gaps := DetectBlockGaps(assignment.From, assignment.To, observedHeights)
		if len(gaps) > 0 {
			t.gaps = append(t.gaps, gaps...)
			return t.currentAdvanceLocked(false, gaps), nil
		}
	}

	delete(t.inFlight, id)
	t.completedByFrom[assignment.From] = assignment

	advanced := false
	for {
		next := t.checkpoint + 1
		completed, exists := t.completedByFrom[next]
		if !exists {
			break
		}
		delete(t.completedByFrom, next)
		t.checkpoint = completed.To
		advanced = true
	}

	return t.currentAdvanceLocked(advanced, nil), nil
}

func (t *RangeTracker) CheckpointHeight() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.checkpoint
}

func (t *RangeTracker) PendingHeight() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.checkpoint
}

func (t *RangeTracker) FinalizedHeight() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return minUint64(t.checkpoint, SafeFinalizedHeight(t.latestTarget, t.finalityDepth, t.reorgWindow))
}

func (t *RangeTracker) Gaps() []BlockGap {
	t.mu.Lock()
	defer t.mu.Unlock()

	gaps := make([]BlockGap, len(t.gaps))
	copy(gaps, t.gaps)
	return gaps
}

func (t *RangeTracker) Snapshot(chain string, network string) Checkpoint {
	t.mu.Lock()
	defer t.mu.Unlock()

	finalizedHeight := minUint64(t.checkpoint, SafeFinalizedHeight(t.latestTarget, t.finalityDepth, t.reorgWindow))
	return Checkpoint{
		Chain:           chain,
		Network:         network,
		Height:          t.checkpoint,
		PendingHeight:   t.checkpoint,
		FinalizedHeight: finalizedHeight,
		FinalityStatus:  statusFor(t.checkpoint, finalizedHeight),
	}
}

func RangeID(from uint64, to uint64) string {
	return formatHeight(from) + "-" + formatHeight(to)
}

func (t *RangeTracker) currentAdvanceLocked(advanced bool, gaps []BlockGap) CheckpointAdvance {
	finalizedHeight := minUint64(t.checkpoint, SafeFinalizedHeight(t.latestTarget, t.finalityDepth, t.reorgWindow))
	return CheckpointAdvance{
		Advanced:        advanced,
		Height:          t.checkpoint,
		PendingHeight:   t.checkpoint,
		FinalizedHeight: finalizedHeight,
		FinalityStatus:  statusFor(t.checkpoint, finalizedHeight),
		Gaps:            gaps,
	}
}

func minUint64(a uint64, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
