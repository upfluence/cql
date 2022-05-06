// Code generated by "stringer -type=Consistency"; DO NOT EDIT.

package cql

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[Any-0]
	_ = x[One-1]
	_ = x[Two-2]
	_ = x[Three-3]
	_ = x[Quorum-4]
	_ = x[All-5]
	_ = x[LocalQuorum-6]
	_ = x[EachQuorum-7]
	_ = x[LocalOne-10]
}

const (
	_Consistency_name_0 = "AnyOneTwoThreeQuorumAllLocalQuorumEachQuorum"
	_Consistency_name_1 = "LocalOne"
)

var (
	_Consistency_index_0 = [...]uint8{0, 3, 6, 9, 14, 20, 23, 34, 44}
)

func (i Consistency) String() string {
	switch {
	case i <= 7:
		return _Consistency_name_0[_Consistency_index_0[i]:_Consistency_index_0[i+1]]
	case i == 10:
		return _Consistency_name_1
	default:
		return "Consistency(" + strconv.FormatInt(int64(i), 10) + ")"
	}
}