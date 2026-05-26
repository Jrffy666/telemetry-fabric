package checkpoint

import "sort"

type BlockGap struct {
	From uint64 `json:"from"`
	To   uint64 `json:"to"`
}

type GapDetector struct{}

func NewGapDetector() GapDetector {
	return GapDetector{}
}

func (d GapDetector) Detect(from uint64, to uint64, observedHeights []uint64) []BlockGap {
	return DetectBlockGaps(from, to, observedHeights)
}

func (d GapDetector) HasGap(from uint64, to uint64, observedHeights []uint64) bool {
	return len(d.Detect(from, to, observedHeights)) > 0
}

func (g BlockGap) Empty() bool {
	return g.From == 0 && g.To == 0
}

func (g BlockGap) Contains(height uint64) bool {
	return height >= g.From && height <= g.To
}

func DetectBlockGaps(from uint64, to uint64, observedHeights []uint64) []BlockGap {
	if from > to {
		return nil
	}
	if len(observedHeights) == 0 {
		return []BlockGap{{From: from, To: to}}
	}

	heights := make([]uint64, 0, len(observedHeights))
	for _, height := range observedHeights {
		if height >= from && height <= to {
			heights = append(heights, height)
		}
	}
	if len(heights) == 0 {
		return []BlockGap{{From: from, To: to}}
	}

	sort.Slice(heights, func(i int, j int) bool {
		return heights[i] < heights[j]
	})

	gaps := make([]BlockGap, 0)
	expected := from
	lastObserved := uint64(0)
	hasLastObserved := false
	for _, height := range heights {
		if hasLastObserved && height == lastObserved {
			continue
		}
		hasLastObserved = true
		lastObserved = height

		if height > expected {
			gaps = append(gaps, BlockGap{From: expected, To: height - 1})
		}
		if height >= expected {
			expected = height + 1
		}
	}
	if expected <= to {
		gaps = append(gaps, BlockGap{From: expected, To: to})
	}
	return gaps
}
