package reorg

import (
	"testing"

	"telemetry-fabric/services/chain-crawler-go/pkg/event"
)

func TestDetectorDetectsHashChangeReorg(t *testing.T) {
	detector := NewDetector(DetectorOptions{Window: 6})

	first := header(10, "0xaaa", "0xparent")
	if reorgEvent, reorged := detector.Check(first); reorged {
		t.Fatalf("first observation should not reorg: %+v", reorgEvent)
	}

	replacement := header(10, "0xbbb", "0xparent2")
	reorgEvent, reorged := detector.Check(replacement)
	if !reorged {
		t.Fatal("expected hash change at same height to be detected as reorg")
	}
	if reorgEvent.Height != 10 || reorgEvent.OldHash != "0xaaa" || reorgEvent.NewHash != "0xbbb" {
		t.Fatalf("unexpected reorg event: %+v", reorgEvent)
	}
}

func TestDetectorDetectsParentHashContinuityBreak(t *testing.T) {
	detector := NewDetector()

	if _, err := detector.Observe(header(20, "0x20", "0x19")); err != nil {
		t.Fatalf("observe parent: %v", err)
	}
	detection, err := detector.Observe(header(21, "0x21", "0xwrong"))
	if err != nil {
		t.Fatalf("observe child: %v", err)
	}
	if !detection.Reorged {
		t.Fatal("expected parent hash mismatch to be detected as reorg")
	}
	if detection.Reorg.ExpectedParentHash != "0x20" || detection.Reorg.ParentHash != "0xwrong" {
		t.Fatalf("unexpected continuity event: %+v", detection.Reorg)
	}
}

func TestDetectorReportsBlockGap(t *testing.T) {
	detector := NewDetector()

	if _, err := detector.Observe(header(1, "0x1", "0x0")); err != nil {
		t.Fatalf("observe first: %v", err)
	}
	detection, err := detector.Observe(header(4, "0x4", "0x3"))
	if err != nil {
		t.Fatalf("observe gap: %v", err)
	}
	if !detection.HasGap || detection.Gap != (BlockGap{From: 2, To: 3}) {
		t.Fatalf("expected gap 2-3, got %+v", detection)
	}
}

func TestDedupeKeysAndDuplicateDetector(t *testing.T) {
	evt := event.NormalizedEvent{
		ChainID:   1,
		BlockHash: "0xblock",
		TxHash:    "0xtx",
		LogIndex:  7,
	}

	if got, want := EVMEventDedupeKey(evt), "1|0xblock|0xtx|7"; got != want {
		t.Fatalf("event dedupe key = %q, want %q", got, want)
	}
	if got, want := TransactionDedupeKey(evt), "1|0xblock|0xtx"; got != want {
		t.Fatalf("transaction dedupe key = %q, want %q", got, want)
	}

	detector := NewDuplicateDetector()
	if detector.IsDuplicate(EVMEventDedupeKey(evt)) {
		t.Fatal("first key should not be duplicate")
	}
	if !detector.IsDuplicate(EVMEventDedupeKey(evt)) {
		t.Fatal("second key should be duplicate")
	}
}

func header(height uint64, hash string, parentHash string) event.BlockHeader {
	return event.BlockHeader{
		Chain:      "evm",
		Network:    "testnet",
		Height:     height,
		Hash:       hash,
		ParentHash: parentHash,
	}
}
