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

// Package partitioning provides ways to describe how the set of facts have been partitioned.
//
// Partitions are describes in 2 axis, the Key Encoding which describe how the fact is converted
// to/from a diskview Key, and Range, which describe the subset of points in the space that the
// partition covers. Currently all spaces are Hash partitioned, that might change in the future.
package partitioning

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/cespare/xxhash"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/space"
)

// FactPartition describes a particular partitioning of fact data, the
// primary functionality this provides is to ask if a particular fact
// is in the partition.
type FactPartition interface {
	// HasFact returns true if the supplied fact is part of this partition
	HasFact(f *rpc.Fact) bool
	// Encoding returns an indication of how keys are constructed/ordered in this partition
	Encoding() rpc.FactKeyEncoding
	// HashRange returns the range out of a Hash64 hash space that this partition
	// occupies
	HashRange() space.Range
}

type subjectPredicatePartition struct {
	hashRange space.Range
	encoding  rpc.FactKeyEncoding
}

// NewHashSubjectPredicatePartition will construct a new FactPartition instance for a
// Subject+Predicate based hash partition. The hash space is divided up into 'numPartitions'
// sections and the returned Partition is for the n-th item in the divided space.
// partition should therefore be strictly less than numPartitions. for example
// with numPartitions equal to 4, the available partions are 0,1,2,3.
func NewHashSubjectPredicatePartition(partition, numPartitions int) FactPartition {
	return NewHashSubjectPredicate(hashRange(uint64(partition), uint64(numPartitions)))
}

// NewHashSubjectPredicate constructs a new FactPartition instance for a
// Subject+Predicate based hash partition. The partition contains all the hashes
// within the supplied 'hashRange'
func NewHashSubjectPredicate(hashRange space.Range) FactPartition {
	return &subjectPredicatePartition{
		hashRange: hashRange,
		encoding:  rpc.KeyEncodingSPO,
	}
}

func (s *subjectPredicatePartition) HasFact(f *rpc.Fact) bool {
	return s.hashRange.Contains(HashSP(f.Subject, f.Predicate))
}

func (s *subjectPredicatePartition) Encoding() rpc.FactKeyEncoding {
	return s.encoding
}

func (s *subjectPredicatePartition) HashRange() space.Range {
	return s.hashRange
}

type predicateObjectPartition struct {
	hashRange space.Range
	encoding  rpc.FactKeyEncoding
}

// NewHashPredicateObjectPartition will construct a new FactPartition instance for a
// Predicate+Object based hash partition. The hash space is divided up into 'numPartitions'
// sections and the returned Partition is for the n-th item in the divided space.
// partition should therefore be strictly less than numPartitions. for example
// with numPartitions equal to 4, the available partions are 0,1,2,3.
func NewHashPredicateObjectPartition(partition, numPartitions int) FactPartition {
	return NewHashPredicateObject(hashRange(uint64(partition), uint64(numPartitions)))
}

// NewHashPredicateObject constructs a new FactPartition instance for a
// Predicate+Object based hash partition. The partition contains all the hashes
// within the supplied 'hashRange'
func NewHashPredicateObject(hashRange space.Range) FactPartition {
	return &predicateObjectPartition{
		hashRange: hashRange,
		encoding:  rpc.KeyEncodingPOS,
	}
}

func (p *predicateObjectPartition) HasFact(f *rpc.Fact) bool {
	return p.hashRange.Contains(HashPO(f.Predicate, f.Object))
}

func (p *predicateObjectPartition) Encoding() rpc.FactKeyEncoding {
	return p.encoding
}

func (p *predicateObjectPartition) HashRange() space.Range {
	return p.hashRange
}

// hashRange returns the range of hashes served by the given partition.
// 'partition' must be strictly less than 'numPartitions'. This assigns each
// partition a nearly equal range. The lower-numbered partitions may serve one
// more hash than the higher-numbered partitions if the number of partitions
// does not evenly divide the 64-bit space.
func hashRange(partition uint64, numPartitions uint64) space.Range {
	// This code has to avoid overflowing a 64-bit integer, which gets pretty
	// tricky. Its correctness leans heavily on the unit tests.

	// The partitions from 0 up to remainder will have size psz + 1.
	// The partitions from remainder up to numPartitions will have psz.
	psz := ^uint64(0) / numPartitions
	remainder := ^uint64(0) - psz*numPartitions + 1
	if remainder == numPartitions {
		// numPartitions evenly divides 2^64
		psz++
		remainder = 0
	}

	var start uint64
	switch {
	case partition < remainder:
		start = partition*psz + partition
	case partition >= remainder:
		start = partition*psz + remainder
	}

	var end space.Point
	switch {
	case partition == numPartitions-1:
		end = space.Infinity
	case partition < remainder:
		end = space.Hash64(start + psz + 1)
	case partition >= remainder:
		end = space.Hash64(start + psz)
	}

	return space.Range{Start: space.Hash64(start), End: end}
}

// HashSP returns a consistent Hash64 value calculated from the subject & predicate
func HashSP(subject, predicate uint64) space.Hash64 {
	d := xxhash.New()

	s := make([]byte, 8)
	binary.LittleEndian.PutUint64(s, subject)
	d.Write(s)

	p := make([]byte, 8)
	binary.LittleEndian.PutUint64(p, predicate)
	d.Write(p)

	return space.Hash64(d.Sum64())
}

// HashPO returns a consistent Hash64 value calculated from the predicate & object
func HashPO(predicate uint64, obj rpc.KGObject) space.Hash64 {
	d := xxhash.New()

	p := make([]byte, 8)
	binary.LittleEndian.PutUint64(p, predicate)
	d.Write(p)

	d.WriteString(obj.AsString())

	return space.Hash64(d.Sum64())
}

// PartitionFromCarouselHashPartition will take a CarouselHashPartitionRequest structure used
// in Carousel RPC requests and return an equivalent FactPartition instance from it.
func PartitionFromCarouselHashPartition(p rpc.CarouselHashPartitionRequest) (FactPartition, error) {
	rng := space.Range{Start: space.Hash64(p.Start), End: space.Infinity}
	if p.End != 0 {
		rng.End = space.Hash64(p.End)
	}
	switch p.Encoding {
	case rpc.KeyEncodingSPO:
		return &subjectPredicatePartition{hashRange: rng, encoding: p.Encoding}, nil
	case rpc.KeyEncodingPOS:
		return &predicateObjectPartition{hashRange: rng, encoding: p.Encoding}, nil
	}
	return nil, fmt.Errorf("unexpected FactKeyEncoding value of %s", p.Encoding)
}

// CarouselHashPartition will create a rpc.CarouselHashPartitionRequest for use in carousel
// RPCs which describes the supplied FactPartition
func CarouselHashPartition(partition FactPartition) rpc.CarouselHashPartitionRequest {
	rng := partition.HashRange()
	res := rpc.CarouselHashPartitionRequest{
		Encoding: partition.Encoding(),
		Start:    uint64(rng.Start.(space.Hash64)),
		End:      0, // Infinity is represented as 0 in the End field for the RPC types
	}
	if rng.End.Less(space.Infinity) {
		res.End = uint64(rng.End.(space.Hash64))
	}
	return res
}

// String returns a textual description of the partition, for human consumption
func String(p FactPartition) string {
	if p == nil {
		return "(nil)"
	}
	b := new(strings.Builder)
	switch p.Encoding() {
	case rpc.KeyEncodingSPO:
		b.WriteString("SPO: ")
	case rpc.KeyEncodingPOS:
		b.WriteString("POS: ")
	default:
		b.WriteString("???: ")
	}
	appendPoint(b, p.HashRange().Start)
	b.WriteString(" - ")
	appendPoint(b, p.HashRange().End)
	return b.String()
}

func appendPoint(b *strings.Builder, p space.Point) {
	switch h := p.(type) {
	case space.Hash64:
		s := h.String()
		// trim trailing XXX's
		last := s[len(s)-1]
		p := len(s) - 2
		for ; p > 4 && s[p] == last; p-- {
		}
		if p <= len(s)-6 {
			b.WriteString(s[:p])
			b.WriteString("..")
		} else {
			b.WriteString(s)
		}
	default:
		fmt.Fprintf(b, "%v", p)
	}
}
