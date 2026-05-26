package evm

import "strings"

type ReorgStatus string

const (
	ReorgStatusUnknown    ReorgStatus = "unknown"
	ReorgStatusCanonical  ReorgStatus = "canonical"
	ReorgStatusReorged    ReorgStatus = "reorged"
	ReorgStatusSequential ReorgStatus = "sequential"
)

type ReorgCheck struct {
	Status ReorgStatus
	Depth  uint64
	Reason string
}

type ReorgEvent struct {
	Height             uint64 `json:"height"`
	Depth              uint64 `json:"depth"`
	OldHash            string `json:"old_hash,omitempty"`
	NewHash            string `json:"new_hash,omitempty"`
	ExpectedParentHash string `json:"expected_parent_hash,omitempty"`
	ParentHash         string `json:"parent_hash,omitempty"`
	Reason             string `json:"reason"`
	Correctable        bool   `json:"correctable"`
}

type FinalityUpdate struct {
	Header          BlockHeader `json:"header"`
	FinalityStatus  string      `json:"finality_status"`
	FinalizedHeight uint64      `json:"finalized_height"`
	Reorg           *ReorgEvent `json:"reorg,omitempty"`
}

type FinalityTracker struct {
	finalityDepth   uint64
	reorgWindow     uint64
	latest          uint64
	finalizedHeight uint64
	blocks          map[uint64]BlockRef
}

func NewFinalityTracker(cfg Config) *FinalityTracker {
	cfg = applyConfigDefaults(cfg)
	return &FinalityTracker{
		finalityDepth: cfg.FinalityDepth,
		reorgWindow:   cfg.ReorgWindow,
		blocks:        make(map[uint64]BlockRef),
	}
}

func DetectReorg(previous, next BlockRef) ReorgCheck {
	if previous.Number == 0 || next.Number == 0 {
		return ReorgCheck{Status: ReorgStatusUnknown, Reason: "missing block number"}
	}
	if previous.ChainID != 0 && next.ChainID != 0 && previous.ChainID != next.ChainID {
		return ReorgCheck{Status: ReorgStatusUnknown, Reason: "different chain_id"}
	}

	prevHash := strings.ToLower(previous.Hash)
	nextHash := strings.ToLower(next.Hash)
	nextParent := strings.ToLower(next.ParentHash)

	if previous.Number == next.Number {
		if prevHash == nextHash {
			return ReorgCheck{Status: ReorgStatusCanonical, Reason: "same block hash"}
		}
		return ReorgCheck{Status: ReorgStatusReorged, Depth: 1, Reason: "same height has different hash"}
	}

	if next.Number == previous.Number+1 {
		if nextParent == prevHash {
			return ReorgCheck{Status: ReorgStatusSequential, Reason: "next block extends previous"}
		}
		return ReorgCheck{Status: ReorgStatusReorged, Depth: 1, Reason: "next block parent does not match previous hash"}
	}

	if next.Number > previous.Number+1 {
		return ReorgCheck{Status: ReorgStatusUnknown, Depth: next.Number - previous.Number, Reason: "gap between checked blocks"}
	}

	return ReorgCheck{Status: ReorgStatusReorged, Depth: previous.Number - next.Number + 1, Reason: "candidate block is behind previous block"}
}

func (t *FinalityTracker) Observe(header BlockHeader) FinalityUpdate {
	if t.blocks == nil {
		t.blocks = make(map[uint64]BlockRef)
	}
	ref := BlockRef{
		Chain:      header.Chain,
		Network:    header.Network,
		ChainID:    header.ChainID,
		Number:     header.Number,
		Hash:       normalizeHex(header.Hash),
		ParentHash: normalizeHex(header.ParentHash),
	}

	var reorg *ReorgEvent
	if previous, ok := t.blocks[ref.Number]; ok && !sameHex(previous.Hash, ref.Hash) {
		reorg = &ReorgEvent{
			Height:      ref.Number,
			Depth:       reorgDepth(t.latest, ref.Number),
			OldHash:     normalizeHex(previous.Hash),
			NewHash:     normalizeHex(ref.Hash),
			Reason:      "same height has different hash",
			Correctable: t.inReorgWindow(ref.Number),
		}
	} else if ref.Number > 0 {
		if previous, ok := t.blocks[ref.Number-1]; ok && !sameHex(previous.Hash, ref.ParentHash) {
			reorg = &ReorgEvent{
				Height:             ref.Number,
				Depth:              1,
				ExpectedParentHash: normalizeHex(previous.Hash),
				ParentHash:         normalizeHex(ref.ParentHash),
				Reason:             "block parent hash mismatch",
				Correctable:        t.inReorgWindow(ref.Number),
			}
		}
	}

	if ref.Number > t.latest {
		t.latest = ref.Number
	}
	t.blocks[ref.Number] = ref
	t.finalizedHeight = finalizedHeight(t.latest, t.finalityDepth)
	t.prune()

	return FinalityUpdate{
		Header:          header,
		FinalityStatus:  statusForBlock(ref.Number, t.latest, t.finalityDepth, false),
		FinalizedHeight: t.finalizedHeight,
		Reorg:           reorg,
	}
}

func (t *FinalityTracker) FinalizedHeight() uint64 {
	if t == nil {
		return 0
	}
	return t.finalizedHeight
}

func (t *FinalityTracker) LatestHeight() uint64 {
	if t == nil {
		return 0
	}
	return t.latest
}

func statusForBlock(block, latest, finalityDepth uint64, reorged bool) string {
	if reorged {
		return FinalityStatusReorged
	}
	if latest >= block && latest-block >= finalityDepth {
		return FinalityStatusFinalized
	}
	return FinalityStatusPending
}

func finalizedHeight(latest, finalityDepth uint64) uint64 {
	if latest < finalityDepth {
		return 0
	}
	return latest - finalityDepth
}

func reorgDepth(latest, height uint64) uint64 {
	if latest < height {
		return 1
	}
	return latest - height + 1
}

func (t *FinalityTracker) inReorgWindow(height uint64) bool {
	if t.reorgWindow == 0 || t.latest <= t.reorgWindow {
		return true
	}
	return height >= t.latest-t.reorgWindow
}

func (t *FinalityTracker) prune() {
	if t.reorgWindow == 0 || t.latest <= t.reorgWindow {
		return
	}
	min := t.latest - t.reorgWindow
	for height := range t.blocks {
		if height < min {
			delete(t.blocks, height)
		}
	}
}
