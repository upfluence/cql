package scroller

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTokenRangeSplit(t *testing.T) {
	for _, tt := range []struct {
		name      string
		haveRange TokenRange
		haveN     int
		want      []TokenRange
	}{
		{
			name:      "full ring in one range",
			haveRange: FullTokenRange,
			haveN:     1,
			want:      []TokenRange{FullTokenRange},
		},
		{
			name:      "full ring in four ranges",
			haveRange: FullTokenRange,
			haveN:     4,
			want: []TokenRange{
				{From: math.MinInt64, To: -4611686018427387905},
				{From: -4611686018427387905, To: -2},
				{From: -2, To: 4611686018427387901},
				{From: 4611686018427387901, To: math.MaxInt64},
			},
		},
		{
			name:      "more ranges than tokens",
			haveRange: TokenRange{From: 0, To: 3},
			haveN:     10,
			want:      []TokenRange{{From: 0, To: 1}, {From: 1, To: 2}, {From: 2, To: 3}},
		},
		{
			name:      "no split",
			haveRange: TokenRange{From: 0, To: 10},
			haveN:     0,
			want:      []TokenRange{{From: 0, To: 10}},
		},
		{
			name:      "single token",
			haveRange: TokenRange{From: 4, To: 5},
			haveN:     4,
			want:      []TokenRange{{From: 4, To: 5}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.haveRange.Split(tt.haveN))
		})
	}
}

func TestSplitTokenRange(t *testing.T) {
	assert.Equal(t, FullTokenRange.Split(8), SplitTokenRange(8))
}
