// Copyright 2019 eBay Inc.
// Primary authors: Simon Fell, Diego Ongaro,
//                  Raymond Kroeker, and Sathish Kandasamy.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package space defines abstract notions of points and ranges. It's used in
// Akutan to describe the keys and hashes that servers host.
package space

import (
	"fmt"
	"sort"
)

// A Point is a location in a space. For example, a hash value in a hash-space
// is a Point. In this package, Hash64 and Key implement Point.
type Point interface {
	// Less compares point values. It returns true if this point has a strictly
	// smaller value than 'other', false otherwise.
	Less(other Point) bool
}

// ImplementPoint is a list of types that implement Point in this package.
// This serves as documentation and as a compile-time check.
var ImplementPoint = []Point{
	Key(""),
	Hash64(0),
	Infinity,
}

// PointLt returns true if a < b.
func PointLt(a, b Point) bool {
	return a.Less(b)
}

// PointLeq returns true if a <= b.
func PointLeq(a, b Point) bool {
	if a.Less(b) {
		return true
	}
	return !b.Less(a)
}

// PointEq returns true if a == b.
func PointEq(a, b Point) bool {
	return !a.Less(b) && !b.Less(a)
}

// PointGt returns true if a > b.
func PointGt(a, b Point) bool {
	return b.Less(a)
}

// PointGeq returns true if a >= b.
func PointGeq(a, b Point) bool {
	if b.Less(a) {
		return true
	}
	return !a.Less(b)
}

// PointMin returns the lesser of the 2 points
func PointMin(a, b Point) Point {
	if a.Less(b) {
		return a
	}
	return b
}

// Hash64 is a type of Point in a 64-bit hash-space.
type Hash64 uint64

// Less implements Point.Less using < on the integers values. All Hash64 values
// compare less than 'Infinity', so the full key-space is given by the following
// range:
// 		Range { Start: Hash64(0), End: Infinity }
func (a Hash64) Less(b Point) bool {
	switch b := b.(type) {
	case Hash64:
		return uint64(a) < uint64(b)
	case InfinitePoint:
		return true
	}
	panic(fmt.Sprintf("Hash64.Less: Unexpected type %T", b))
}

// String() returns the hex-formatted hash value.
func (a Hash64) String() string {
	return fmt.Sprintf("0x%016X", uint64(a))
}

// Key is a type of Point in a lexically-ordered string key-space.
type Key string

// Less implements Point.Less using < on the string values. All Key values
// compare less than 'Infinity', so the full key-space is given by the following
// range:
// 		Range { Start: Key(""), End: Infinity }
func (a Key) Less(b Point) bool {
	switch b := b.(type) {
	case Key:
		return string(a) < string(b)
	case InfinitePoint:
		return true
	}
	panic(fmt.Sprintf("Key.Less: Unexpected type %T", b))
}

// a.String() simply returns string(a). Note: this is not suitable for binary keys.
func (a Key) String() string {
	return string(a)
}

// InfinitePoint is a sentinel for the largest possible Point value. It's
// commonly used as being the end of an entire space.
type InfinitePoint struct{}

// Infinity is an instance of InfinitePoint, for convenience.
var Infinity InfinitePoint

// Less implements Point.Less by always returning false.
func (a InfinitePoint) Less(b Point) bool {
	return false
}

// Returns "∞".
func (a InfinitePoint) String() string {
	return "∞"
}

// A Range is an interval from the Start point up to but excluding the End point.
type Range struct {
	// The first Point in the range.
	Start Point
	// Just past the last Point in the range.
	End Point
}

// Contains returns true if the point falls within the range, false otherwise.
func (r Range) Contains(point Point) bool {
	return PointLeq(r.Start, point) && PointLt(point, r.End)
}

// Overlaps returns true if both ranges cover at least one common point, false otherwise.
func (r Range) Overlaps(other Range) bool {
	return r.Start.Less(other.End) && PointGt(r.End, other.Start)
}

// Equals returns true if both ranges cover the exact same points, false otherwise.
func (r Range) Equals(other Range) bool {
	return PointEq(r.Start, other.Start) && PointEq(r.End, other.End)
}

// A PartitionedRange is a compressed representation of a split up range. The
// entire range is StartPoints[0] to End. It's partitioned at the boundaries
// given by StartPoints[1:], so the number of subranges is len(StartPoints).
type PartitionedRange struct {
	// The first point of each subrange, in sorted order. StartPoints[i] represents
	// the subrange from StartPoints[i] to StartPoints[i+1]. The last entry
	// represents the subrange from itself to End.
	StartPoints []Point
	// Just past the final point in the overall range.
	End Point
}

// OverallRange returns the range from the first point of the first subrange to
// the end point of the last subrange.
func (ranges *PartitionedRange) OverallRange() Range {
	return Range{
		Start: ranges.StartPoints[0],
		End:   ranges.End,
	}
}

// Get returns the i-th subrange.
func (ranges *PartitionedRange) Get(i int) Range {
	start := ranges.StartPoints[i]
	end := ranges.End
	if i+1 < len(ranges.StartPoints) {
		end = ranges.StartPoints[i+1]
	}
	return Range{Start: start, End: end}
}

// Find returns the index of the subrange containing 'point'. It returns -1 if
// the point falls outside OverallRange().
func (ranges *PartitionedRange) Find(point Point) int {
	if !ranges.OverallRange().Contains(point) {
		return -1
	}
	i := sort.Search(len(ranges.StartPoints),
		func(i int) bool {
			return point.Less(ranges.StartPoints[i])
		})
	return i - 1
}
