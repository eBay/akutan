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

package partitioning

import (
	"fmt"
	"testing"

	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/space"
	"github.com/stretchr/testify/assert"
)

func Test_PO_hashRange4(t *testing.T) {
	assert := assert.New(t)
	p0 := NewHashPredicateObjectPartition(0, 4).HashRange()
	p1 := NewHashPredicateObjectPartition(1, 4).HashRange()
	p2 := NewHashPredicateObjectPartition(2, 4).HashRange()
	p3 := NewHashPredicateObjectPartition(3, 4).HashRange()
	s := func(r space.Range) string {
		return fmt.Sprintf("%v to %v", r.Start, r.End)
	}
	assert.Equal("0x0000000000000000 to 0x4000000000000000", s(p0))
	assert.Equal("0x4000000000000000 to 0x8000000000000000", s(p1))
	assert.Equal("0x8000000000000000 to 0xC000000000000000", s(p2))
	assert.Equal("0xC000000000000000 to ∞", s(p3))
}

func Test_NewPOPartition(t *testing.T) {
	p := NewHashPredicateObjectPartition(0, 4)
	assert.Equal(t, rpc.KeyEncodingPOS, p.Encoding())
	assert.Equal(t, space.Hash64(0), p.HashRange().Start)
	assert.Equal(t, space.Hash64(0x4000000000000000), p.HashRange().End)
	p2 := NewHashPredicateObject(space.Range{Start: space.Hash64(0), End: space.Hash64(0x4000000000000000)})
	assert.True(t, p.HashRange().Equals(p2.HashRange()))
	obj := rpc.AString("Bob", 0)
	var doneHas, doneHasnot bool
	for pred := uint64(1); !doneHas || !doneHasnot; pred++ {
		f := rpc.Fact{Predicate: pred, Object: obj, Subject: 1}
		f2 := rpc.Fact{Predicate: pred, Object: obj, Subject: 2}
		if HashPO(pred, obj) <= space.Hash64(0x4000000000000000) {
			assert.True(t, p.HasFact(&f))
			assert.True(t, p.HasFact(&f2))
			doneHas = true
		} else {
			assert.False(t, p.HasFact(&f))
			assert.False(t, p.HasFact(&f2))
			doneHasnot = true
		}
	}
}

func Test_NewSPPartition(t *testing.T) {
	p := NewHashSubjectPredicatePartition(1, 4)
	assert.Equal(t, rpc.KeyEncodingSPO, p.Encoding())
	assert.Equal(t, space.Hash64(0x4000000000000000), p.HashRange().Start)
	assert.Equal(t, space.Hash64(0x8000000000000000), p.HashRange().End)
	p2 := NewHashSubjectPredicate(space.Range{Start: space.Hash64(0x4000000000000000), End: space.Hash64(0x8000000000000000)})
	assert.True(t, p.HashRange().Equals(p2.HashRange()))
	var doneHas, doneHasnot bool
	for pred := uint64(1); !doneHas || !doneHasnot; pred++ {
		f := rpc.Fact{Predicate: pred, Object: rpc.AString("Bob", 0), Subject: 1}
		f2 := rpc.Fact{Predicate: pred, Object: rpc.AString("Eve", 0), Subject: 1}
		h := HashSP(1, pred)
		if h >= space.Hash64(0x4000000000000000) && h < space.Hash64(0x8000000000000000) {
			assert.True(t, p.HasFact(&f))
			assert.True(t, p.HasFact(&f2))
			doneHas = true
		} else {
			assert.False(t, p.HasFact(&f))
			assert.False(t, p.HasFact(&f2))
			doneHasnot = true
		}
	}
}

func Benchmark_HashSP(b *testing.B) {
	s := uint64(5577006791947779410)
	p := uint64(734811035)

	for i := 0; i < b.N; i++ {
		HashSP(s, p)
	}
}

func Test_HashSP(t *testing.T) {
	h := HashSP(1, 1)
	h2 := HashSP(20, 1)
	h3 := HashSP(1, 20)
	assert.NotEqual(t, h, h2)
	assert.NotEqual(t, h, h3)
	assert.NotEqual(t, h2, h3)
}

func Benchmark_HashPO(b *testing.B) {
	p := uint64(734811035)
	o := rpc.AString("Bob", 0)

	for i := 0; i < b.N; i++ {
		HashPO(p, o)
	}
}

func Test_HashPO(t *testing.T) {
	h := HashPO(1, rpc.AString("Bob", 0))
	h2 := HashPO(2, rpc.AString("Bob", 0))
	h3 := HashPO(1, rpc.AString("Bob", 1))
	h4 := HashPO(1, rpc.AString("Eve", 0))
	h5 := HashPO(1, rpc.AKID(2))
	assert.NotEqual(t, h, h2)
	assert.NotEqual(t, h, h3)
	assert.NotEqual(t, h, h4)
	assert.NotEqual(t, h, h5)
	assert.NotEqual(t, h2, h3)
	assert.NotEqual(t, h2, h4)
	assert.NotEqual(t, h2, h5)
	assert.NotEqual(t, h3, h4)
	assert.NotEqual(t, h3, h5)
	assert.NotEqual(t, h4, h5)
}

func Test_PO_hashRange_coverage(t *testing.T) {
	assert := assert.New(t)
	for n := 1; n < 20; n++ {
		first := NewHashPredicateObjectPartition(0, n).HashRange()
		last := NewHashPredicateObjectPartition(n-1, n).HashRange()

		// Check that all hashes are covered.
		assert.Equal(space.Hash64(0), first.Start)
		assert.Equal(space.Infinity, last.End)
		prev := first
		for i := 1; i < n; i++ {
			r := NewHashPredicateObjectPartition(i, n).HashRange()
			assert.Equal(prev.End, r.Start, "i=%v, n=%v", i, n)
			prev = r
		}

		// Check that sizes differ by at most 1, and larger sizes are assigned to the
		// earlier partitions.
		if n == 1 {
			continue // can't represent 2^64 minus 0
		}
		size := func(r space.Range) uint64 {
			start := r.Start.(space.Hash64)
			switch end := r.End.(type) {
			case space.Hash64:
				return uint64(end) - uint64(r.Start.(space.Hash64))
			case space.InfinitePoint:
				if start == 0 {
					panic("can't represent 2^64 minus 0")
				}
				return ^uint64(0) - uint64(start) + 1
			default:
				panic(fmt.Sprintf("unexpected type %T", r.End))
			}
		}
		for i := 1; i < n; i++ {
			r := NewHashPredicateObjectPartition(i, n).HashRange()
			assert.True(size(first)-1 <= size(r) && size(r) <= size(first),
				"i=%v, n=%v, pSize=%v, firstSize=%v", i, n, size(r), size(first))
		}
	}
}

func Test_CarouselHashPartition(t *testing.T) {
	type test struct {
		partition FactPartition
		expected  rpc.CarouselHashPartitionRequest
	}
	tests := []test{
		{
			NewHashSubjectPredicatePartition(0, 4),
			rpc.CarouselHashPartitionRequest{
				Encoding: rpc.KeyEncodingSPO,
				Start:    0,
				End:      0x4000000000000000,
			},
		}, {
			NewHashSubjectPredicatePartition(1, 2),
			rpc.CarouselHashPartitionRequest{
				Encoding: rpc.KeyEncodingSPO,
				Start:    0x8000000000000000,
				End:      0,
			},
		}, {
			NewHashPredicateObjectPartition(1, 4),
			rpc.CarouselHashPartitionRequest{
				Encoding: rpc.KeyEncodingPOS,
				Start:    0x4000000000000000,
				End:      0x8000000000000000,
			},
		},
	}
	for _, tc := range tests {
		act := CarouselHashPartition(tc.partition)
		assert.Equal(t, tc.expected, act, "Wrong result for FactPartition %#v", tc.partition)
		p2, err := PartitionFromCarouselHashPartition(act)
		assert.NoError(t, err)
		assert.Equal(t, tc.partition, p2, "FactPartition failed to round trip through CarouselHashPartition")
	}
}

func Test_PartitionFromCarouselChecksRPC(t *testing.T) {
	p, err := PartitionFromCarouselHashPartition(rpc.CarouselHashPartitionRequest{
		Encoding: 42,
	})
	assert.Error(t, err)
	assert.Nil(t, p)
}

func Test_String(t *testing.T) {
	type test struct {
		fp  FactPartition
		exp string
	}
	tests := []test{
		{NewHashSubjectPredicatePartition(0, 1), "SPO: 0x00.. - ∞"},
		{NewHashSubjectPredicatePartition(0, 4), "SPO: 0x00.. - 0x40.."},
		{NewHashSubjectPredicatePartition(1, 4), "SPO: 0x40.. - 0x80.."},
		{NewHashSubjectPredicatePartition(1, 3), "SPO: 0x5555555555555556 - 0xAAAAAAAAAAAAAAAB"},
		{NewHashSubjectPredicatePartition(7, 8), "SPO: 0xE0.. - ∞"},
		{NewHashPredicateObjectPartition(0, 1), "POS: 0x00.. - ∞"},
		{NewHashPredicateObjectPartition(0, 4), "POS: 0x00.. - 0x40.."},
	}
	for _, tc := range tests {
		t.Run(tc.exp, func(t *testing.T) {
			assert.Equal(t, tc.exp, String(tc.fp))
		})
	}
}
