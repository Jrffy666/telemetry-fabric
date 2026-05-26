package evm

import "testing"

func TestFinalityTrackerMarksPendingFinalizedAndReorgs(t *testing.T) {
	tracker := NewFinalityTracker(Config{
		ChainName:     "ethereum",
		FinalityDepth: 2,
		ReorgWindow:   4,
	})

	update1 := tracker.Observe(testHeader(10, "0x10", "0x0f"))
	if update1.FinalityStatus != FinalityStatusPending {
		t.Fatalf("block 10 status = %s, want pending", update1.FinalityStatus)
	}
	tracker.Observe(testHeader(11, "0x11", "0x10"))
	update3 := tracker.Observe(testHeader(12, "0x12", "0x11"))
	if update3.FinalizedHeight != 10 {
		t.Fatalf("finalized height = %d, want 10", update3.FinalizedHeight)
	}
	if status := statusForBlock(10, tracker.LatestHeight(), 2, false); status != FinalityStatusFinalized {
		t.Fatalf("block 10 status = %s, want finalized", status)
	}

	reorg := tracker.Observe(testHeader(12, "0xfeed", "0x11"))
	if reorg.Reorg == nil {
		t.Fatalf("expected same-height hash mismatch reorg")
	}
	if !reorg.Reorg.Correctable || reorg.Reorg.OldHash != "0x12" || reorg.Reorg.NewHash != "0xfeed" {
		t.Fatalf("reorg event = %#v", reorg.Reorg)
	}

	parentMismatch := tracker.Observe(testHeader(13, "0x13", "0xdead"))
	if parentMismatch.Reorg == nil || parentMismatch.Reorg.ExpectedParentHash != "0xfeed" || parentMismatch.Reorg.ParentHash != "0xdead" {
		t.Fatalf("parent mismatch = %#v", parentMismatch.Reorg)
	}
}

func TestFinalityTrackerPrunesOutsideReorgWindow(t *testing.T) {
	tracker := NewFinalityTracker(Config{
		ChainName:     "ethereum",
		FinalityDepth: 1,
		ReorgWindow:   2,
	})
	tracker.Observe(testHeader(1, "0x1", "0x0"))
	tracker.Observe(testHeader(2, "0x2", "0x1"))
	tracker.Observe(testHeader(3, "0x3", "0x2"))
	tracker.Observe(testHeader(4, "0x4", "0x3"))
	if _, ok := tracker.blocks[1]; ok {
		t.Fatalf("block 1 should be pruned outside reorg window")
	}
	if _, ok := tracker.blocks[3]; !ok {
		t.Fatalf("block 3 should remain inside reorg window")
	}
}

func testHeader(number uint64, hash string, parentHash string) BlockHeader {
	return BlockHeader{
		Chain:      "ethereum",
		Network:    "mainnet",
		ChainID:    1,
		Number:     number,
		Hash:       hash,
		ParentHash: parentHash,
	}
}
