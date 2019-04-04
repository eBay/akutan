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

package space

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Point_comparisons(t *testing.T) {
	assert := assert.New(t)
	type Test struct {
		a, b Point
		exp  int // -1 for <, 0 for =, 1 for >
	}
	tests := []Test{
		{a: Hash64(0), b: Hash64(1), exp: -1},
		{a: Hash64(10), b: Hash64(10), exp: 0},
		{a: Hash64(12), b: Hash64(10), exp: 1},
	}
	for _, test := range tests {
		tests = append(tests, Test{
			a:   test.b,
			b:   test.a,
			exp: test.exp * -1,
		})
	}
	for _, test := range tests {
		switch test.exp {
		case -1:
			assert.True(test.a.Less(test.b), "Less %+v", test)
			assert.True(PointLt(test.a, test.b), "lt %+v", test)
			assert.True(PointLeq(test.a, test.b), "leq %+v", test)
			assert.False(PointEq(test.a, test.b), "eq %+v", test)
			assert.False(PointGeq(test.a, test.b), "geq %+v", test)
			assert.False(PointGt(test.a, test.b), "gt %+v", test)
		case 0:
			assert.False(test.a.Less(test.b), "Less %+v", test)
			assert.False(PointLt(test.a, test.b), "lt %+v", test)
			assert.True(PointLeq(test.a, test.b), "leq %+v", test)
			assert.True(PointEq(test.a, test.b), "eq %+v", test)
			assert.True(PointGeq(test.a, test.b), "geq %+v", test)
			assert.False(PointGt(test.a, test.b), "gt %+v", test)
		case 1:
			assert.False(test.a.Less(test.b), "Less %+v", test)
			assert.False(PointLt(test.a, test.b), "lt %+v", test)
			assert.False(PointLeq(test.a, test.b), "leq %+v", test)
			assert.False(PointEq(test.a, test.b), "eq %+v", test)
			assert.True(PointGeq(test.a, test.b), "geq %+v", test)
			assert.True(PointGt(test.a, test.b), "gt %+v", test)
		}
	}
}

func Test_Hash64_Less(t *testing.T) {
	assert := assert.New(t)
	assert.True(Hash64(10).Less(Hash64(11)))
	assert.False(Hash64(10).Less(Hash64(10)))
	assert.False(Hash64(10).Less(Hash64(9)))
	assert.True(Hash64(0).Less(Hash64(1)))
	assert.False(Hash64(0).Less(Hash64(0)))
	assert.True(Hash64(0).Less(Infinity))
	assert.True(Hash64(300).Less(Infinity))
	assert.True(Hash64(^uint64(0)).Less(Infinity))
	assert.True(PointGt(Infinity, Hash64(^uint64(0))))
}

func ExampleHash64_String() {
	fmt.Printf("%v\n", Hash64(0))
	fmt.Printf("%v\n", Hash64(0x100))
	fmt.Printf("%v\n", Hash64(^uint64(0)))
	// Output:
	// 0x0000000000000000
	// 0x0000000000000100
	// 0xFFFFFFFFFFFFFFFF
}

func Test_Key_Less(t *testing.T) {
	assert := assert.New(t)
	assert.True(Key("a").Less(Key("b")))
	assert.False(Key("a").Less(Key("a")))
	assert.False(Key("b").Less(Key("a")))
	assert.True(Key("").Less(Key("\000")))
	assert.False(Key("").Less(Key("")))
	assert.True(Key("").Less(Infinity))
	assert.True(Key("hi").Less(Infinity))
	assert.True(Key("\uffff").Less(Infinity))
	assert.True(PointGt(Infinity, Key("\uffff")))
}

func ExampleKey_String() {
	var zero Key
	fmt.Printf("1. %v.\n", zero)
	fmt.Printf("2. %v.\n", Key(""))
	fmt.Printf("3. %v.\n", Key("hello"))
	// Output:
	// 1. .
	// 2. .
	// 3. hello.
}

func Test_InfinitePoint_Less(t *testing.T) {
	assert := assert.New(t)
	assert.False(Infinity.Less(Infinity))
	assert.False(Infinity.Less(Hash64(3)))
	assert.False(Infinity.Less(Key("hello")))
}

func Test_PointMin(t *testing.T) {
	type test struct {
		pLess, pMore Point
	}
	tests := []test{
		{Hash64(0), Infinity},
		{Hash64(0), Hash64(1)},
		{Hash64(10), Hash64(10)},
		{Key("Alice"), Key("Bob")},
		{Key("Alice"), Infinity},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.pLess, PointMin(tc.pLess, tc.pMore))
		assert.Equal(t, tc.pLess, PointMin(tc.pMore, tc.pLess))
	}
}

func Test_Range_Contains(t *testing.T) {
	assert := assert.New(t)
	r := Range{
		Start: Hash64(10),
		End:   Hash64(20),
	}
	assert.False(r.Contains(Hash64(8)))
	assert.False(r.Contains(Hash64(9)))
	assert.True(r.Contains(Hash64(10)))
	assert.True(r.Contains(Hash64(19)))
	assert.False(r.Contains(Hash64(20)))
	assert.False(r.Contains(Hash64(21)))

	r = Range{
		Start: Key(""),
		End:   Infinity,
	}
	assert.True(r.Contains(Key("")))
	assert.True(r.Contains(Key("hello")))
	assert.False(r.Contains(Infinity))
}

func Test_Range_Overlaps(t *testing.T) {
	all := Range{Start: Hash64(0), End: Infinity}
	last := Range{Start: Hash64(1000), End: Infinity}
	r := func(start, end uint64) Range {
		return Range{Start: Hash64(start), End: Hash64(end)}
	}
	assert.True(t, all.Overlaps(r(0, 1)))
	assert.True(t, all.Overlaps(all))
	assert.True(t, all.Overlaps(last))
	assert.True(t, all.Overlaps(r(123, 1234)))
	assert.True(t, r(100, 200).Overlaps(r(110, 120)))
	assert.True(t, r(100, 200).Overlaps(r(50, 101)))
	assert.True(t, r(100, 200).Overlaps(r(199, 201)))
	assert.False(t, r(100, 200).Overlaps(r(50, 100)))
	assert.False(t, r(100, 200).Overlaps(r(200, 300)))
}

func Test_Range_Equals(t *testing.T) {
	all := Range{Start: Hash64(0), End: Infinity}
	all2 := Range{Start: Hash64(0), End: Infinity}
	assert.True(t, all.Equals(all2))
	assert.True(t, all2.Equals(all))
	r1 := Range{Start: Hash64(100), End: Infinity}
	r2 := Range{Start: Hash64(0), End: Hash64(100)}
	assert.True(t, r1.Equals(r1))
	assert.False(t, r1.Equals(r2))
	assert.False(t, r1.Equals(all))
	assert.False(t, r2.Equals(all))
}

func ExamplePartitionedRange_OverallRange() {
	ranges := PartitionedRange{
		StartPoints: []Point{Key("d"), Key("m"), Key("t")},
		End:         Key("x"),
	}
	fmt.Printf("%+v\n", ranges.OverallRange())
	// Output:
	// {Start:d End:x}
}

func Test_PartitionedRange_OverallRange(t *testing.T) {
	assert := assert.New(t)
	ranges := PartitionedRange{
		StartPoints: []Point{Hash64(0), Hash64(10)},
		End:         Infinity,
	}
	assert.Equal(Range{Start: Hash64(0), End: Infinity}, ranges.OverallRange())
	ranges = PartitionedRange{
		StartPoints: []Point{Hash64(30)},
		End:         Hash64(50),
	}
	assert.Equal(Range{Start: Hash64(30), End: Hash64(50)}, ranges.OverallRange())
}

func ExamplePartitionedRange_Get() {
	ranges := PartitionedRange{
		StartPoints: []Point{Key("d"), Key("m"), Key("t")},
		End:         Key("x"),
	}
	fmt.Printf("%+v\n", ranges.Get(0))
	fmt.Printf("%+v\n", ranges.Get(2))
	// Output:
	// {Start:d End:m}
	// {Start:t End:x}
}

func Test_PartitionedRange_Get(t *testing.T) {
	assert := assert.New(t)
	ranges := PartitionedRange{
		StartPoints: []Point{Hash64(0), Hash64(10), Hash64(20), Hash64(30)},
		End:         Infinity,
	}
	assert.Equal(Range{Start: Hash64(0), End: Hash64(10)}, ranges.Get(0))
	assert.Equal(Range{Start: Hash64(10), End: Hash64(20)}, ranges.Get(1))
	assert.Equal(Range{Start: Hash64(20), End: Hash64(30)}, ranges.Get(2))
	assert.Equal(Range{Start: Hash64(30), End: Infinity}, ranges.Get(3))
}

func ExamplePartitionedRange_Find() {
	ranges := PartitionedRange{
		StartPoints: []Point{Key("d"), Key("m"), Key("t")},
		End:         Key("x"),
	}
	fmt.Print(
		ranges.Find(Key("diego")),
		ranges.Find(Key("alice")),
		ranges.Find(Key("raymond")),
		ranges.Find(Key("zara")),
		ranges.Find(Key("taylor")),
		ranges.Find(Key("simon")),
	)
	// Output: 0 -1 1 -1 2 1
}
func Test_HashRanges_Find(t *testing.T) {
	assert := assert.New(t)
	ranges := PartitionedRange{
		StartPoints: []Point{Hash64(5), Hash64(10), Hash64(20), Hash64(30), Hash64(40)},
		End:         Hash64(301),
	}
	assert.Equal(-1, ranges.Find(Hash64(0)))
	assert.Equal(-1, ranges.Find(Hash64(4)))
	assert.Equal(0, ranges.Find(Hash64(5)))
	assert.Equal(0, ranges.Find(Hash64(8)))
	assert.Equal(0, ranges.Find(Hash64(9)))
	assert.Equal(1, ranges.Find(Hash64(10)))
	assert.Equal(1, ranges.Find(Hash64(12)))
	assert.Equal(1, ranges.Find(Hash64(19)))
	assert.Equal(2, ranges.Find(Hash64(20)))
	assert.Equal(2, ranges.Find(Hash64(25)))
	assert.Equal(2, ranges.Find(Hash64(29)))
	assert.Equal(3, ranges.Find(Hash64(30)))
	assert.Equal(3, ranges.Find(Hash64(35)))
	assert.Equal(3, ranges.Find(Hash64(39)))
	assert.Equal(4, ranges.Find(Hash64(40)))
	assert.Equal(4, ranges.Find(Hash64(45)))
	assert.Equal(4, ranges.Find(Hash64(300)))
	assert.Equal(-1, ranges.Find(Hash64(301)))
	assert.Equal(-1, ranges.Find(Hash64(322)))
}
