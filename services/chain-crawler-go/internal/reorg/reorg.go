package reorg

import (
	"errors"
	"sync"

	"telemetry-fabric/services/chain-crawler-go/pkg/event"
)

var ErrFinalizedReorg = errors.New("reorg intersects finalized height")

type Event struct {
	Chain              string `json:"chain"`
	Network            string `json:"network"`
	Height             uint64 `json:"height"`
	OldHash            string `json:"old_hash"`
	NewHash            string `json:"new_hash"`
	ParentHash         string `json:"parent_hash"`
	ExpectedParentHash string `json:"expected_parent_hash,omitempty"`
}

type BlockGap struct {
	From uint64 `json:"from"`
	To   uint64 `json:"to"`
}

type Detection struct {
	Reorg          Event                `json:"reorg,omitempty"`
	Reorged        bool                 `json:"reorged"`
	Gap            BlockGap             `json:"gap,omitempty"`
	HasGap         bool                 `json:"has_gap"`
	FinalityStatus event.FinalityStatus `json:"finality_status"`
}

type DetectorOptions struct {
	Window          uint64
	FinalizedHeight uint64
}

type ReorgDetector interface {
	Check(header event.BlockHeader) (Event, bool)
	Observe(header event.BlockHeader) (Detection, error)
}

type Detector struct {
	mu              sync.Mutex
	headers         map[uint64]event.BlockHeader
	highest         uint64
	window          uint64
	finalizedHeight uint64
}

func NewDetector(options ...DetectorOptions) *Detector {
	var option DetectorOptions
	if len(options) > 0 {
		option = options[0]
	}
	return &Detector{
		headers:         make(map[uint64]event.BlockHeader),
		window:          option.Window,
		finalizedHeight: option.FinalizedHeight,
	}
}

func (d *Detector) Check(header event.BlockHeader) (Event, bool) {
	detection, _ := d.Observe(header)
	return detection.Reorg, detection.Reorged
}

func (d *Detector) Observe(header event.BlockHeader) (Detection, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	detection := Detection{
		FinalityStatus: FinalityStatusForHeight(header.Height, d.finalizedHeight),
	}

	if d.highest > 0 && header.Height > d.highest+1 {
		detection.Gap = BlockGap{From: d.highest + 1, To: header.Height - 1}
		detection.HasGap = true
	}

	if oldHeader, exists := d.headers[header.Height]; exists && oldHeader.Hash != header.Hash {
		detection.Reorged = true
		detection.Reorg = Event{
			Chain:      header.Chain,
			Network:    header.Network,
			Height:     header.Height,
			OldHash:    oldHeader.Hash,
			NewHash:    header.Hash,
			ParentHash: header.ParentHash,
		}
	} else if header.Height > 0 {
		if parent, exists := d.headers[header.Height-1]; exists {
			if continuity, ok := CheckHashContinuity(parent, header); !ok {
				detection.Reorged = true
				detection.Reorg = continuity
			}
		}
	}

	d.headers[header.Height] = header
	if header.Height > d.highest {
		d.highest = header.Height
	}
	d.pruneLocked()

	if detection.Reorged && detection.Reorg.Height <= d.finalizedHeight {
		return detection, ErrFinalizedReorg
	}
	return detection, nil
}

func (d *Detector) SetFinalizedHeight(height uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.finalizedHeight = height
	d.pruneLocked()
}

func (d *Detector) FinalizedHeight() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.finalizedHeight
}

func (d *Detector) ReorgWindow() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.window
}

func CheckHashContinuity(parent event.BlockHeader, child event.BlockHeader) (Event, bool) {
	if child.Height == 0 {
		return Event{}, true
	}
	if parent.Height+1 != child.Height {
		return Event{
			Chain:              child.Chain,
			Network:            child.Network,
			Height:             child.Height,
			OldHash:            parent.Hash,
			NewHash:            child.Hash,
			ParentHash:         child.ParentHash,
			ExpectedParentHash: parent.Hash,
		}, false
	}
	if child.ParentHash == "" || parent.Hash == "" || child.ParentHash == parent.Hash {
		return Event{}, true
	}
	return Event{
		Chain:              child.Chain,
		Network:            child.Network,
		Height:             child.Height,
		OldHash:            parent.Hash,
		NewHash:            child.Hash,
		ParentHash:         child.ParentHash,
		ExpectedParentHash: parent.Hash,
	}, false
}

func WindowedFinalizedHeight(latestHeight uint64, finalityDepth uint64, reorgWindow uint64) uint64 {
	depth := finalityDepth
	if reorgWindow > depth {
		depth = reorgWindow
	}
	if latestHeight <= depth {
		return 0
	}
	return latestHeight - depth
}

func FinalityStatusForHeight(height uint64, finalizedHeight uint64) event.FinalityStatus {
	if height > 0 && height <= finalizedHeight {
		return event.FinalityStatusFinalized
	}
	return event.FinalityStatusPending
}

func (d *Detector) pruneLocked() {
	if d.window == 0 || d.highest <= d.window {
		return
	}
	keepFrom := d.highest - d.window
	for height := range d.headers {
		if height < keepFrom {
			delete(d.headers, height)
		}
	}
}
