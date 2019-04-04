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
	"context"
	"fmt"

	"github.com/ebay/beam/partitioning"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/space"
	"github.com/ebay/beam/util/cmp"
	"github.com/ebay/beam/util/errors"
	"github.com/ebay/beam/util/parallel"
	"github.com/ebay/beam/viewclient/fanout"
)

// FactStats collects data about the distributions of facts in the store. It
// fans out to many view servers, which have this data readily available, and
// collects it all together.
func (c *Client) FactStats(ctx context.Context, overallReq *rpc.FactStatsRequest) (*FactStats, error) {
	results := FactStats{}
	// Fan out the requests to cover both the PO hash-space and the SP hash-space.
	err := parallel.Invoke(ctx,
		func(ctx context.Context) error {
			rpc := func(ctx context.Context, view fanout.View,
				offsets []int, results func(fanout.Result)) error {
				res, err := view.(*ReadFactsPOClient).Stub.FactStats(ctx, overallReq)
				if err != nil {
					return err
				}
				results(res)
				return nil
			}
			views := c.ReadFactsPOViews()
			ranges := fanout.Partition(fullRange, views)
			resultsCh := make(chan fanout.Chunk, 4)
			wait := drainStatsResults("PO", resultsCh, results.poStats.consume, ranges)
			err := fanout.Call(ctx, ranges.StartPoints, views, rpc, resultsCh)
			if err != nil {
				err = fmt.Errorf("error getting PO FactStats: %v", err)
			}
			return errors.Any(wait(), err)
		},
		func(ctx context.Context) error {
			rpc := func(ctx context.Context, view fanout.View,
				offsets []int, results func(fanout.Result)) error {
				res, err := view.(*ReadFactsSPClient).Stub.FactStats(ctx, overallReq)
				if err != nil {
					return err
				}
				results(res)
				return nil
			}
			views := c.ReadFactsSPViews()
			ranges := fanout.Partition(fullRange, views)
			resultsCh := make(chan fanout.Chunk, 4)
			wait := drainStatsResults("SP", resultsCh, results.spStats.consume, ranges)
			err := fanout.Call(ctx, ranges.StartPoints, views, rpc, resultsCh)
			if err != nil {
				err = fmt.Errorf("error getting SP FactStats: %v", err)
			}
			return errors.Any(wait(), err)
		},
	)
	if err != nil {
		return nil, err
	}
	results.completed()
	return &results, nil
}

// statsConsumer defines a callback function used with drainStatsResults. The
// results in 'r' cover the entire range in 'resultsRange'. Only data that's in
// the 'consumeRange' should be applied. 'consumeRange' may be equal to or
// smaller than 'resultsRange'. If smaller then statsConsumer implementations
// will be required to scale the results 'r' down appropriately.
type statsConsumer func(r *rpc.FactStatsResult, resultsRange space.Range, consumeRange space.Range)

// drainStatsResults starts a goroutine that will read results from resultsCh,
// and pass them onto the supplied consume function. drainStatsResults will deal
// with getting duplicates and overlapping results from fanout. The collective
// set of consumeRanges passed to the consume function will cover the space at
// most once (there may be gaps in the event of multiple RPC errors). consume
// may be called multiple times, but never concurrently.
//
// The returned function can be used to wait until the resultsCh is closed and
// the results have been fully processed. In the event of not having full
// coverage of the space in the stats results, an error will be returned from
// the returned waiter when called.
func drainStatsResults(hashSpace string, resultsCh chan fanout.Chunk, consume statsConsumer, partitions space.PartitionedRange) func() error {
	return parallel.GoCaptureError(func() error {
		offsetsDone := make([]bool, len(partitions.StartPoints))
		offsetsNeeded := func(offsets []int) []int {
			var res []int
			for _, offset := range offsets {
				if !offsetsDone[offset] {
					offsetsDone[offset] = true
					res = append(res, offset)
				}
			}
			return res
		}
		for chunk := range resultsCh {
			r := chunk.Result.(*rpc.FactStatsResult)
			viewRange := chunk.View.Serves()
			for _, consumeRange := range flattenRanges(offsetsNeeded(chunk.Offsets), partitions) {
				consume(r, viewRange, consumeRange)
			}
		}
		for offset, done := range offsetsDone {
			if !done {
				return fmt.Errorf("missing statistics for %v in space %v",
					partitions.Get(offset), hashSpace)
			}
		}
		return nil
	})
}

// flattenRanges will collect the set of ranges given the offsets into the
// partitions, and collapse any adjacent ranges. Returns the resulting list
// of ranges. It assumes offsets are already in ascending order.
func flattenRanges(offsets []int, partitions space.PartitionedRange) []space.Range {
	var result []space.Range
	var current *space.Range
	for _, offset := range offsets {
		rng := partitions.Get(offset)
		if current != nil && space.PointEq(rng.Start, current.End) {
			current.End = rng.End
			continue
		}
		result = append(result, rng)
		current = &result[len(result)-1]
	}
	return result
}

// FactStats contains statistics about how facts are distributed in the overall
// database.
type FactStats struct {
	totalFacts int
	poStats
	spStats
}

type poStats struct {
	poTotalFacts int

	distinctP       int
	frequentPCounts map[uint64]int
	frequentPTotal  int

	distinctPO       int
	frequentPOCounts map[predicateObject]int
	frequentPOTotal  int
}

type spStats struct {
	spTotalFacts int

	distinctS       int
	frequentSCounts map[uint64]int
	frequentSTotal  int

	distinctSP       int
	frequentSPCounts map[subjectPredicate]int
	frequentSPTotal  int
}

// predicateObject is a pair of (predicate, object) used in FactStats.
type predicateObject struct {
	Predicate uint64
	Object    rpc.KGObject
}

// subjectPredicate is a pair of (subject, predicate) used in FactStats.
type subjectPredicate struct {
	Subject   uint64
	Predicate uint64
}

// consume will update poStats with data from 'r', filtering down the data in
// the event that consumeRange is smaller than resultsRange.
func (stats *poStats) consume(r *rpc.FactStatsResult, resultsRange space.Range, consumeRange space.Range) {
	if stats.frequentPOCounts == nil {
		stats.frequentPOCounts = make(map[predicateObject]int)
	}
	if stats.frequentPCounts == nil {
		stats.frequentPCounts = make(map[uint64]int)
	}
	ratio := rangeSize(consumeRange) / rangeSize(resultsRange)
	stats.poTotalFacts += int(ratio * float64(r.NumFacts))

	stats.distinctP = cmp.MaxInt(stats.distinctP, int(ratio*float64(r.DistinctPredicates)))
	for _, item := range r.Predicates {
		count := int(ratio * float64(item.Count))
		stats.frequentPCounts[item.Predicate] += count
		stats.frequentPTotal += count
	}

	stats.distinctPO += int(ratio * float64(r.DistinctPredicateObjects))
	for _, item := range r.PredicateObjects {
		po := predicateObject{Predicate: item.Predicate, Object: item.Object}
		if ratio == 1 || consumeRange.Contains(partitioning.HashPO(po.Predicate, po.Object)) {
			stats.frequentPOCounts[po] += int(item.Count)
			stats.frequentPOTotal += int(item.Count)
		}
	}
}

// consume will update spStats with data from 'r', filtering down the data in
// the event that consumeRange is smaller than resultsRange.
func (stats *spStats) consume(r *rpc.FactStatsResult, resultsRange space.Range, consumeRange space.Range) {
	if stats.frequentSPCounts == nil {
		stats.frequentSPCounts = make(map[subjectPredicate]int)
	}
	if stats.frequentSCounts == nil {
		stats.frequentSCounts = make(map[uint64]int)
	}
	ratio := rangeSize(consumeRange) / rangeSize(resultsRange)
	stats.spTotalFacts += int(ratio * float64(r.NumFacts))

	stats.distinctS = cmp.MaxInt(stats.distinctS, int(ratio*float64(r.DistinctSubjects)))
	for _, item := range r.Subjects {
		count := int(ratio * float64(item.Count))
		stats.frequentSCounts[item.Subject] += count
		stats.frequentSTotal += count
	}

	stats.distinctSP += int(ratio * float64(r.DistinctSubjectPredicates))
	for _, item := range r.SubjectPredicates {
		sp := subjectPredicate{Subject: item.Subject, Predicate: item.Predicate}
		if ratio == 1 || consumeRange.Contains(partitioning.HashSP(sp.Subject, sp.Predicate)) {
			stats.frequentSPCounts[sp] += int(item.Count)
			stats.frequentSPTotal += int(item.Count)
		}
	}
}

// rangeSize returns the size of the supplied range.
func rangeSize(r space.Range) float64 {
	endInf := space.PointEq(r.End, space.Infinity)
	s := float64(r.Start.(space.Hash64))
	if endInf {
		return float64(1<<64) - s
	}
	e := float64(r.End.(space.Hash64))
	return e - s
}

// completed will finalize the data in the FactStats, it should be called once
// all the calls to consume have completed.
func (stats *FactStats) completed() {
	stats.totalFacts = cmp.MaxInt(stats.poStats.poTotalFacts, stats.spStats.spTotalFacts)

	// This avoids potential issues with divide-by-zero, negative numbers, etc that
	// may be caused by:
	// - Testing with empty datasets
	// - Gathering stats from an inconsistent snapshot
	// - Poor approximations
	stats.totalFacts = cmp.MaxInt(stats.totalFacts,
		stats.frequentPTotal+1,
		stats.frequentPOTotal+1,
		stats.frequentSTotal+1,
		stats.frequentSPTotal+1,
		stats.distinctP+1,
		stats.distinctPO+1,
		stats.distinctS+1,
		stats.distinctSP+1)
	stats.distinctP = cmp.MaxInt(stats.distinctP, len(stats.frequentPCounts)+1)
	stats.distinctPO = cmp.MaxInt(stats.distinctPO, len(stats.frequentPOCounts)+1)
	stats.distinctS = cmp.MaxInt(stats.distinctS, len(stats.frequentSCounts)+1)
	stats.distinctSP = cmp.MaxInt(stats.distinctSP, len(stats.frequentSPCounts)+1)
}

// ToRPCFactStats will generate an rpc fact stats result from FactStats.
func (stats *FactStats) ToRPCFactStats() *rpc.FactStatsResult {
	res := &rpc.FactStatsResult{
		Predicates:        make([]rpc.PredicateStats, 0, len(stats.frequentPCounts)),
		PredicateObjects:  make([]rpc.PredicateObjectStats, 0, len(stats.frequentPOCounts)),
		Subjects:          make([]rpc.SubjectStats, 0, len(stats.frequentSCounts)),
		SubjectPredicates: make([]rpc.SubjectPredicateStats, 0, len(stats.frequentSPCounts)),
	}
	for predicate, count := range stats.frequentPCounts {
		res.Predicates = append(res.Predicates,
			rpc.PredicateStats{Predicate: predicate, Count: uint64(count)})
	}
	for po, count := range stats.frequentPOCounts {
		res.PredicateObjects = append(res.PredicateObjects,
			rpc.PredicateObjectStats{Predicate: po.Predicate, Object: po.Object, Count: uint64(count)})
	}
	for subject, count := range stats.frequentSCounts {
		res.Subjects = append(res.Subjects,
			rpc.SubjectStats{Subject: subject, Count: uint64(count)})
	}
	for sp, count := range stats.frequentSPCounts {
		res.SubjectPredicates = append(res.SubjectPredicates,
			rpc.SubjectPredicateStats{Subject: sp.Subject, Predicate: sp.Predicate, Count: uint64(count)})
	}
	res.NumFacts = uint64(stats.totalFacts)
	res.DistinctPredicates = uint64(stats.distinctP)
	res.DistinctPredicateObjects = uint64(stats.distinctPO)
	res.DistinctSubjects = uint64(stats.distinctS)
	res.DistinctSubjectPredicates = uint64(stats.distinctSP)
	return res
}

// BytesPerFact Implements planner.Stats.BytesPerFact().
func (stats *FactStats) BytesPerFact() int {
	return 100
}

// NumFacts Implements planner.Stats.NumFacts().
func (stats *FactStats) NumFacts() int {
	return stats.totalFacts
}

// NumFactsP Implements planner.Stats.NumFactsP().
func (stats *poStats) NumFactsP(predicate uint64) int {
	if predicate == 0 {
		facts := stats.poTotalFacts
		predicates := stats.distinctP
		return facts / predicates
	}
	count, ok := stats.frequentPCounts[predicate]
	if ok {
		return count
	}
	facts := stats.poTotalFacts - stats.frequentPTotal
	predicates := stats.distinctP - len(stats.frequentPCounts)
	return facts / predicates
}

// NumFactsPO Implements planner.Stats.NumFactsPO().
func (stats *poStats) NumFactsPO(predicate uint64, object rpc.KGObject) int {
	if predicate == 0 || object.IsType(rpc.KtNil) {
		facts := stats.poTotalFacts
		numPO := stats.distinctPO
		return facts / numPO
	}
	po := predicateObject{Predicate: predicate, Object: object}
	count, ok := stats.frequentPOCounts[po]
	if ok {
		return count
	}
	facts := stats.poTotalFacts - stats.frequentPOTotal
	numPO := stats.distinctPO - len(stats.frequentPOCounts)
	return facts / numPO
}

// NumFactsO Implements planner.Stats.NumFactsO().
func (stats *poStats) NumFactsO(object rpc.KGObject) int {
	return 1000
}

// NumFactsS Implements planner.Stats.NumFactsS().
func (stats *spStats) NumFactsS(subject uint64) int {
	if subject == 0 {
		facts := stats.spTotalFacts
		subjects := stats.distinctS
		return facts / subjects
	}
	count, ok := stats.frequentSCounts[subject]
	if ok {
		return count
	}
	facts := stats.spTotalFacts - stats.frequentSTotal
	subjects := stats.distinctS - len(stats.frequentSCounts)
	return facts / subjects
}

// NumFactsSP Implements planner.Stats.NumFactsSP().
func (stats *spStats) NumFactsSP(subject uint64, predicate uint64) int {
	if subject == 0 || predicate == 0 {
		facts := stats.spTotalFacts
		numSP := stats.distinctSP
		return facts / numSP
	}
	sp := subjectPredicate{Subject: subject, Predicate: predicate}
	count, ok := stats.frequentSPCounts[sp]
	if ok {
		return count
	}
	facts := stats.spTotalFacts - stats.frequentSPTotal
	numSP := stats.distinctSP - len(stats.frequentSPCounts)
	return facts / numSP
}

// NumFactsSO Implements planner.Stats.NumFactsSO().
func (stats *FactStats) NumFactsSO(subject uint64, object rpc.KGObject) int {
	s := stats.NumFactsS(subject)
	o := stats.NumFactsO(object)
	if s < o {
		return s
	}
	return o
}
