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

package exec

import (
	"context"
	"fmt"

	"github.com/ebay/akutan/query/parser"
	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/util/parallel"
	"github.com/sirupsen/logrus"
)

// newHashJoin returns a new operator for the hashJoin operation. It will
// start the execution of both inputs, calculate the join value for all the left
// values. Once that is complete it'll process the values from the right
// outputting a joined FactSet where needed.
//
// For a join with a required match it checks for an equal join value on both
// sides. For an optional match, the output will either be the left value, or
// the left value + right value.
//
// If asked to perform a bulk join, it will execute each join serially
func newHashJoin(op *plandef.HashJoin, inputNodes []queryOperator) operator {
	if len(inputNodes) != 2 {
		panic(fmt.Sprintf("hashJoin operation with unexpected inputs: %v", len(inputNodes)))
	}
	panicOnInvalidSpecificity(op.Specificity)
	columns, joiner := joinedColumns(inputNodes[0].columns(), inputNodes[1].columns())

	return &bulkWrapper{singleRowOp: &hashJoin{
		def:    op,
		left:   inputNodes[0],
		right:  inputNodes[1],
		output: columns,
		joiner: joiner,
	}}
}

type hashJoin struct {
	def    *plandef.HashJoin
	left   queryOperator
	right  queryOperator
	output Columns
	joiner func(left, right []Value) []Value
}

func (h *hashJoin) operator() plandef.Operator {
	return h.def
}

func (h *hashJoin) columns() Columns {
	return h.output
}

func (h *hashJoin) execute(ctx context.Context, binder valueBinder, res results) error {
	// key is the identityKey of all the variables in joinVars concat'd in that order
	type joinValues map[string][]rowValue

	// we need to hash all the left items before we can start on the right side
	// this channel is used to pass the calculated left join values to the
	// right side function when they're they're all completed
	leftValuesCh := make(chan joinValues)

	fnLeft := func(ctx context.Context) error {
		leftResCh := make(chan ResultChunk, 4)
		leftJoinColumnIndexes := h.left.columns().MustIndexesOf(h.def.Variables)
		leftJoinValues := make(joinValues)
		wait := parallel.Go(func() {
			for chunk := range leftResCh {
				for i := range chunk.offsets {
					row := chunk.Row(i)
					joinIdentityKey := chunk.identityKeysOf(i, leftJoinColumnIndexes)
					// while hashJoin isn't bulkified, offset will always be zero
					rv := rowValue{offset: 0, fact: chunk.Facts[i], vals: row}
					// don't hoist the string(k) out of the map calls, go will optimize away
					// the string allocation in this case, but not if the string is constructed
					// outside of the map index function
					leftJoinValues[string(joinIdentityKey)] = append(leftJoinValues[string(joinIdentityKey)], rv)
				}
			}
		})
		err := h.left.run(ctx, binder, leftResCh)
		wait()
		if err == nil {
			leftValuesCh <- leftJoinValues
		}
		close(leftValuesCh)
		return err
	}

	fnRight := func(ctx context.Context) error {
		rightResCh := make(chan ResultChunk, 4)
		wait := parallel.Go(func() {
			leftJoinValues, open := <-leftValuesCh
			if !open {
				return
			}
			joiner := hashJoiner{
				leftJoinValues: leftJoinValues,
				joinVars:       h.def.Variables,
				rightColumns:   h.right.columns(),
				rightResCh:     rightResCh,
				outputTo:       res,
				joiner:         h.joiner,
			}
			switch h.def.Specificity {
			case parser.MatchRequired:
				joiner.eqJoin(ctx)
			case parser.MatchOptional:
				joiner.leftJoin(ctx)
			default:
				logrus.Panicf("Unexpected value for Join Specificity %v", h.def.Specificity)
			}
		})
		err := h.right.run(ctx, binder, rightResCh)
		wait()
		return err
	}
	return parallel.Invoke(ctx, fnLeft, fnRight)
}

// panicOnInvalidSpecificity panics with a relevant error message in the event
// it is called with an unexpected value of Specificity.
func panicOnInvalidSpecificity(s parser.MatchSpecificity) {
	switch s {
	case parser.MatchRequired:
		return
	case parser.MatchOptional:
		return
	default:
		logrus.Panicf("Unexpected value for Join Specificity %v", s)
	}
}

// hashJoiner can perform hash based joins given the starting set of left
// values. Each resulting joined value is passed to outputTo results instance.
type hashJoiner struct {
	leftJoinValues map[string][]rowValue
	joinVars       plandef.VarSet
	rightColumns   Columns
	rightResCh     <-chan ResultChunk
	outputTo       results
	joiner         func(left, right []Value) []Value
}

// eqJoin processes entries from the right input channel generating joined
// results until the channel is exhausted and closed. There must be a matching
// left row and right row (based on the values of joinVars) for there to be an
// output row. When this function returns the right channel has been fully
// processed.
func (hj *hashJoiner) eqJoin(ctx context.Context) {
	hj.runEqJoin(func(_ string, offset uint32, fs FactSet, rowValues []Value) {
		hj.outputTo.add(ctx, offset, fs, rowValues)
	})
}

// leftJoin processes rows from the right side of the join and generates
// leftJoin results. This performs a regular eq-join, keeping track of all the
// identity keys that have generated results. Once the right side is fully
// processed it will generate the left join output for all the left rows that
// never got a matching right row. When this function returns the right channel
// has been fully processed.
func (hj *hashJoiner) leftJoin(ctx context.Context) {
	leftIdentityKeysUsed := make(map[string]struct{})

	hj.runEqJoin(func(identityKey string, offset uint32, fs FactSet, rowValues []Value) {
		leftIdentityKeysUsed[identityKey] = struct{}{}
		hj.outputTo.add(ctx, offset, fs, rowValues)
	})

	for key, factsets := range hj.leftJoinValues {
		if _, exists := leftIdentityKeysUsed[key]; !exists {
			// this list of FactSets from the left wasn't joined to any
			// right factSets, so emit the left join version of them
			for _, left := range factsets {
				hj.outputTo.add(ctx, left.offset, left.fact, hj.joiner(left.vals, nil))
			}
		}
	}
}

// runEqJoin processes entries from the right input channel until its
// exhausted and closed. It will perform callbacks to the result function for
// every created eq-join result. When this function returns the right channel
// has been fully processed. This is a helper function, users of hashJoiner
// should be calling eqJoin or leftJoin instead.
func (hj *hashJoiner) runEqJoin(result func(identityKey string, offset uint32, fs FactSet, rowValues []Value)) {
	rightJoinColumnIndexes := hj.rightColumns.MustIndexesOf(hj.joinVars)
	for chunk := range hj.rightResCh {
		for i := range chunk.offsets {
			joinIdentityKey := chunk.identityKeysOf(i, rightJoinColumnIndexes)
			leftRows := hj.leftJoinValues[string(joinIdentityKey)]
			// create new factsSets for left[0] + right[i], left[1] + right[i], etc.
			for _, left := range leftRows {
				result(joinIdentityKey,
					left.offset,
					joinFactSets(left.fact, chunk.Facts[i]),
					hj.joiner(left.vals, chunk.Row(i)))
			}
		}
	}
}

// rowValue contains the values for a single row from the left side input.
type rowValue struct {
	offset uint32
	fact   FactSet
	vals   []Value
}
