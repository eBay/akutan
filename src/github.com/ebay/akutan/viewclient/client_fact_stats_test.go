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

package viewclient

import (
	"math"
	"sort"
	"testing"

	"github.com/ebay/akutan/partitioning"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/space"
	utilStats "github.com/ebay/akutan/util/stats"
	"github.com/ebay/akutan/viewclient/fanout"
	"github.com/stretchr/testify/assert"
)

var foo = rpc.AString("foo", 0)

func makeTestStatResults() (po []*rpc.FactStatsResult, sp []*rpc.FactStatsResult) {
	po = []*rpc.FactStatsResult{
		{
			Predicates: []rpc.PredicateStats{
				{Predicate: 1, Count: 180},
				{Predicate: 2, Count: 100},
			},
			PredicateObjects: []rpc.PredicateObjectStats{
				{Predicate: 2, Object: foo, Count: 220},
			},
			NumFacts:                 3000,
			DistinctPredicates:       500,
			DistinctPredicateObjects: 700,
		},
		{
			Predicates: []rpc.PredicateStats{
				{Predicate: 2, Count: 50},
			},
			PredicateObjects: []rpc.PredicateObjectStats{
				{Predicate: 2, Object: foo, Count: 1},
			},
			NumFacts:                 4000,
			DistinctPredicates:       550,
			DistinctPredicateObjects: 750,
		},
	}
	// SP results
	sp = []*rpc.FactStatsResult{
		{
			Subjects: []rpc.SubjectStats{
				{Subject: 3, Count: 321},
				{Subject: 4, Count: 192},
			},
			SubjectPredicates: []rpc.SubjectPredicateStats{
				{Subject: 5, Predicate: 2, Count: 628},
				{Subject: 6, Predicate: 3, Count: 123},
			},
			NumFacts:                  2000,
			DistinctSubjects:          600,
			DistinctSubjectPredicates: 800,
		},
		{
			Subjects: []rpc.SubjectStats{
				{Subject: 3, Count: 9},
			},
			SubjectPredicates: []rpc.SubjectPredicateStats{
				{Subject: 5, Predicate: 2, Count: 2},
			},
			NumFacts:                  5100,
			DistinctSubjects:          650,
			DistinctSubjectPredicates: 850,
		},
	}
	return
}
func makeTestFactStats() *FactStats {
	po, sp := makeTestStatResults()
	res := FactStats{}
	for _, poRes := range po {
		res.poStats.consume(poRes, fullRange, fullRange)
	}
	for _, spRes := range sp {
		res.spStats.consume(spRes, fullRange, fullRange)
	}
	res.completed()
	return &res
}

func Test_FactStats_aggregates(t *testing.T) {
	assert := assert.New(t)
	stats := makeTestFactStats()
	assert.Equal(7100, stats.totalFacts)
	assert.Equal(550, stats.distinctP)
	assert.Equal(700+750, stats.distinctPO)
	assert.Equal(650, stats.distinctS)
	assert.Equal(800+850, stats.distinctSP)
	sum := 0
	for _, count := range stats.frequentPCounts {
		sum += count
	}
	assert.Equal(sum, stats.frequentPTotal)
	sum = 0
	for _, count := range stats.frequentPOCounts {
		sum += count
	}
	assert.Equal(sum, stats.frequentPOTotal)
	sum = 0
	for _, count := range stats.frequentSCounts {
		sum += count
	}
	assert.Equal(sum, stats.frequentSTotal)
	sum = 0
	for _, count := range stats.frequentSPCounts {
		sum += count
	}
	assert.Equal(sum, stats.frequentSPTotal)
}

func Test_FactStats_frequent(t *testing.T) {
	assert := assert.New(t)
	stats := makeTestFactStats()
	assert.Equal(180, stats.NumFactsP(1))
	assert.Equal(150, stats.NumFactsP(2))
	assert.Equal(221, stats.NumFactsPO(2, foo))
	assert.Equal(330, stats.NumFactsS(3))
	assert.Equal(192, stats.NumFactsS(4))
	assert.Equal(630, stats.NumFactsSP(5, 2))
	assert.Equal(123, stats.NumFactsSP(6, 3))
}

func Test_FactStats_ToRPCFactStats(t *testing.T) {
	assert := assert.New(t)
	stats := makeTestFactStats()
	rcpStats := stats.ToRPCFactStats()
	utilStats.SortStats(rcpStats) // by descending count
	assert.Equal([]rpc.PredicateStats{
		{Predicate: 1, Count: 180},
		{Predicate: 2, Count: 150},
	}, rcpStats.Predicates)

	assert.Equal([]rpc.PredicateObjectStats{
		{Predicate: 2, Object: rpc.AString("foo", 0), Count: 221},
	}, rcpStats.PredicateObjects)

	assert.Equal([]rpc.SubjectStats{
		{Subject: 3, Count: 330},
		{Subject: 4, Count: 192},
	}, rcpStats.Subjects)

	assert.Equal([]rpc.SubjectPredicateStats{
		{Subject: 5, Predicate: 2, Count: 630},
		{Subject: 6, Predicate: 3, Count: 123},
	}, rcpStats.SubjectPredicates)
}

func Test_PartialStatsCounts(t *testing.T) {
	po, sp := makeTestStatResults()
	s := FactStats{}
	views := newViews(2)
	s.poStats.consume(po[0], fullRange, views[0].Serves())
	assert.Equal(t, 1500, s.poStats.poTotalFacts)
	assert.Equal(t, 250, s.poStats.distinctP)
	assert.Equal(t, 350, s.poStats.distinctPO)
	assert.Equal(t, 90, s.poStats.frequentPCounts[1])

	s.spStats.consume(sp[0], fullRange, views[1].Serves())
	assert.Equal(t, 1000, s.spStats.spTotalFacts)
	assert.Equal(t, 300, s.spStats.distinctS)
	assert.Equal(t, 400, s.spStats.distinctSP)
	assert.Equal(t, 96, s.spStats.frequentSCounts[4])
}

func Test_DrainStatsResults(t *testing.T) {
	resCh := make(chan fanout.Chunk, 4)
	views4 := newViews(4)
	views3 := newViews(3)
	viewRanges := make(map[space.Range]bool)
	for _, v := range append(views4, views3...) {
		viewRanges[v.Serves()] = true
	}
	consumed := make(map[space.Range]int)
	consume := func(r *rpc.FactStatsResult, rr space.Range, cr space.Range) {
		consumed[cr]++
		assert.True(t, viewRanges[rr], "consume got a results Range that isn't from any View")
	}
	p := fanout.Partition(fullRange, flatten(views3, views4))
	wait := drainStatsResults(t.Name(), resCh, consume, p)
	// results are sent with a mix of partition sizes, and some views are
	// repeated to simulate failures & hedging. Note that there's no full
	// set of results for view4 or view3, its a subset of both.
	orderedViews := []fanout.View{
		views4[0], views3[0], views4[3], views3[1], views4[0], views4[2], views3[0], views3[1],
	}
	for _, v := range orderedViews {
		offsets := []int{}
		first := p.Find(v.Serves().Start)
		last := p.Find(v.Serves().End)
		if last == -1 {
			// End is at the end of all the partitions,
			last = len(p.StartPoints)
		}
		for o := first; o < last; o++ {
			offsets = append(offsets, o)
		}
		resCh <- fanout.Chunk{Result: new(rpc.FactStatsResult), View: v, Offsets: offsets}
	}
	close(resCh)
	assert.NoError(t, wait())
	for cr, count := range consumed {
		assert.Equal(t, 1, count, "Range %v should only be consumed once", cr)
	}
	// verify that the entire range was covered.
	c := make([]space.Range, 0, len(consumed))
	for cr := range consumed {
		c = append(c, cr)
	}
	sort.Slice(c, func(i, j int) bool {
		return c[i].Start.Less(c[j].Start)
	})
	var pos space.Point = space.Hash64(0)
	for _, r := range c {
		assert.Equal(t, pos, r.Start)
		pos = r.End
	}
	assert.Equal(t, space.Infinity, pos)
}

func Test_DrainStatsResultsMissing(t *testing.T) {
	resCh := make(chan fanout.Chunk, 4)
	views := newViews(4)
	consume := func(r *rpc.FactStatsResult, rr space.Range, cr space.Range) {}
	wait := drainStatsResults("PO", resCh, consume, fanout.Partition(fullRange, flatten(views)))
	// send results that are missing some partitions
	for offset, v := range views[:3] {
		resCh <- fanout.Chunk{Result: new(rpc.FactStatsResult), View: v, Offsets: []int{offset}}
	}
	close(resCh)
	assert.EqualError(t, wait(), "missing statistics for {0xC000000000000000 ∞} in space PO")
}

// Test_PartialStatsHashing tests that poStats/spStats correctly applies the
// partition range filtering when range to consume is smaller than the results range
func Test_PartialStatsHashing(t *testing.T) {
	// we build a bunch of facts, and build a stats result that contains
	// all the generated po & sp combo's. In addition we build the expected
	// counts for a particular partition. Then we consume the results and
	// check the counts generated by that match out expected ones.
	spPartition := partitioning.NewHashSubjectPredicatePartition(0, 2)
	poPartition := partitioning.NewHashPredicateObjectPartition(0, 2)
	poCounts := make(map[predicateObject]int)
	spCounts := make(map[subjectPredicate]int)
	req := rpc.FactStatsResult{}
	for pred := uint64(1); true; pred++ {
		f := rpc.Fact{Subject: 42 + pred, Predicate: pred, Object: rpc.AString("Bob", pred+3)}
		if len(poCounts) < 10 {
			req.PredicateObjects = append(req.PredicateObjects,
				rpc.PredicateObjectStats{Predicate: f.Predicate, Object: f.Object, Count: 42})
			if poPartition.HasFact(&f) {
				poCounts[predicateObject{f.Predicate, f.Object}] = 42
			}
		}
		if len(spCounts) < 10 {
			req.SubjectPredicates = append(req.SubjectPredicates,
				rpc.SubjectPredicateStats{Subject: f.Subject, Predicate: f.Predicate, Count: 24})
			if spPartition.HasFact(&f) {
				spCounts[subjectPredicate{f.Subject, f.Predicate}] = 24
			}
		}
		if len(poCounts) < len(req.PredicateObjects) && len(poCounts) > 5 &&
			len(spCounts) < len(req.SubjectPredicates) && len(spCounts) > 5 {
			break
		}
	}
	t.Logf("poCounts:%d spCounts:%d req.PO:%d req.SP:%d", len(poCounts), len(spCounts),
		len(req.PredicateObjects), len(req.SubjectPredicates))
	stats := FactStats{}
	stats.poStats.consume(&req, fullRange, poPartition.HashRange())
	stats.spStats.consume(&req, fullRange, spPartition.HashRange())
	assert.Equal(t, poCounts, stats.poStats.frequentPOCounts)
	assert.Equal(t, spCounts, stats.spStats.frequentSPCounts)
}

func Test_RangeSize(t *testing.T) {
	assert.Equal(t, float64(1<<64), rangeSize(fullRange))
	allmostFull := space.Range{Start: space.Hash64(0), End: space.Hash64(math.MaxUint64)}
	assert.Equal(t, float64(1<<64)-1, rangeSize(allmostFull))
	views := newViews(4)
	for _, v := range views {
		assert.Equal(t, float64(1<<64)/4, rangeSize(v.Serves()), "for range %v", v.Serves())
	}
}

func Test_FlattenRanges(t *testing.T) {
	rngOf := func(views ...fanout.View) []space.Range {
		var res []space.Range
		for _, v := range views {
			res = append(res, v.Serves())
		}
		return res
	}
	v3 := newViews(3) // 0      33%       66%     100%
	v5 := newViews(5) // 0  20%    40% 60%   80%  100%
	// offsets		     0   1   2  3   4  5  6
	p := fanout.Partition(fullRange, flatten(v3, v5))
	assert.Equal(t, rngOf(v5[0]), flattenRanges([]int{0}, p))
	assert.Equal(t, rngOf(v3[0]), flattenRanges([]int{0, 1}, p))
	assert.Equal(t, rngOf(v3[0], v3[2]), flattenRanges([]int{0, 1, 5, 6}, p))
	assert.Equal(t, rngOf(v5[0], v5[2]), flattenRanges([]int{0, 3}, p))
	assert.Equal(t, []space.Range{{Start: v3[0].Serves().Start, End: v5[1].Serves().End}},
		flattenRanges([]int{0, 1, 2}, p))
	assert.Equal(t, append(
		rngOf(v3[0]), space.Range{Start: v5[3].Serves().Start, End: v3[2].Serves().Start}),
		flattenRanges([]int{0, 1, 4}, p))
	assert.Empty(t, flattenRanges(nil, p))
}
