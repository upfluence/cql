package scroller

import "math"

// TokenRange is a range of the murmur3 token ring, exclusive of From and
// inclusive of To.
type TokenRange struct {
	From int64
	To   int64
}

// FullTokenRange covers the whole token ring.
var FullTokenRange = TokenRange{From: math.MinInt64, To: math.MaxInt64}

// SplitTokenRange splits the whole token ring in n contiguous ranges, so they
// can be scrolled concurrently.
func SplitTokenRange(n int) []TokenRange {
	return FullTokenRange.Split(n)
}

// Split splits the range in n contiguous ranges of the same width, the last one
// absorbing the remainder. It returns fewer ranges when the range holds fewer
// than n tokens.
func (tr TokenRange) Split(n int) []TokenRange {
	// Computed on uint64 so the width of the full ring does not overflow.
	var width = uint64(tr.To) - uint64(tr.From) //nolint:gosec

	if n <= 1 || width <= 1 {
		return []TokenRange{tr}
	}

	if uint64(n) > width {
		n = int(width) //nolint:gosec
	}

	var (
		ranges = make([]TokenRange, n)
		step   = width / uint64(n)
		from   = tr.From
	)

	for i := range ranges {
		to := tr.To

		if i < n-1 {
			to = int64(uint64(tr.From) + uint64(i+1)*step) //nolint:gosec
		}

		ranges[i] = TokenRange{From: from, To: to}
		from = to
	}

	return ranges
}
