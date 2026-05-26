package checkpoint

import (
	"context"
	"path/filepath"
	"testing"
)

func TestRangeTrackerOutOfOrderCompletionAdvancesInOrder(t *testing.T) {
	tracker := NewRangeTracker(RangeTrackerOptions{
		StartHeight:   1,
		BatchSize:     10,
		FinalityDepth: 2,
		ReorgWindow:   3,
	})

	first, ok, err := tracker.Assign(30)
	if err != nil || !ok {
		t.Fatalf("assign first: ok=%v err=%v", ok, err)
	}
	second, ok, err := tracker.Assign(30)
	if err != nil || !ok {
		t.Fatalf("assign second: ok=%v err=%v", ok, err)
	}
	third, ok, err := tracker.Assign(30)
	if err != nil || !ok {
		t.Fatalf("assign third: ok=%v err=%v", ok, err)
	}

	advance, err := tracker.Complete(second.ID)
	if err != nil {
		t.Fatalf("complete second: %v", err)
	}
	if advance.Advanced || advance.Height != 0 {
		t.Fatalf("out-of-order completion advanced checkpoint: %+v", advance)
	}

	advance, err = tracker.Complete(first.ID)
	if err != nil {
		t.Fatalf("complete first: %v", err)
	}
	if !advance.Advanced || advance.Height != 20 {
		t.Fatalf("expected ordered checkpoint to advance through first two ranges, got %+v", advance)
	}
	if advance.FinalizedHeight != 20 || advance.FinalityStatus != FinalityStatusFinalized {
		t.Fatalf("expected finalized checkpoint at 20, got %+v", advance)
	}

	advance, err = tracker.Complete(third.ID)
	if err != nil {
		t.Fatalf("complete third: %v", err)
	}
	if !advance.Advanced || advance.Height != 30 {
		t.Fatalf("expected checkpoint to reach 30, got %+v", advance)
	}
	if advance.FinalizedHeight != 27 {
		t.Fatalf("expected reorg window to cap finalized height at 27, got %+v", advance)
	}
	if advance.FinalityStatus != FinalityStatusPending {
		t.Fatalf("expected head inside reorg window to stay pending, got %+v", advance)
	}
}

func TestRangeTrackerBlockGapBlocksCheckpointAdvancement(t *testing.T) {
	tracker := NewRangeTracker(RangeTrackerOptions{
		StartHeight: 1,
		BatchSize:   5,
	})

	assignment, ok, err := tracker.Assign(5)
	if err != nil || !ok {
		t.Fatalf("assign: ok=%v err=%v", ok, err)
	}

	advance, err := tracker.CompleteWithObservedHeights(assignment.ID, []uint64{1, 2, 4, 5})
	if err != nil {
		t.Fatalf("complete with gap: %v", err)
	}
	if advance.Advanced || advance.Height != 0 {
		t.Fatalf("gap should block checkpoint advancement, got %+v", advance)
	}
	if len(advance.Gaps) != 1 || advance.Gaps[0] != (BlockGap{From: 3, To: 3}) {
		t.Fatalf("expected gap at block 3, got %+v", advance.Gaps)
	}

	advance, err = tracker.CompleteWithObservedHeights(assignment.ID, []uint64{1, 2, 3, 4, 5})
	if err != nil {
		t.Fatalf("retry complete without gap: %v", err)
	}
	if !advance.Advanced || advance.Height != 5 {
		t.Fatalf("expected retry to advance checkpoint, got %+v", advance)
	}
}

func TestFileStoreCrashRecovery(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "checkpoints.json")
	store := NewFileStore(path)

	want := Checkpoint{
		Chain:           "evm",
		Network:         "mainnet",
		Height:          100,
		PendingHeight:   100,
		FinalizedHeight: 94,
	}
	if err := store.Save(ctx, want); err != nil {
		t.Fatalf("save: %v", err)
	}

	recovered := NewFileStore(path)
	got, ok, err := recovered.Load(ctx, "evm", "mainnet")
	if err != nil || !ok {
		t.Fatalf("load recovered checkpoint: ok=%v err=%v", ok, err)
	}
	if got.Height != want.Height || got.FinalizedHeight != want.FinalizedHeight {
		t.Fatalf("recovered checkpoint mismatch: got %+v want %+v", got, want)
	}
}
