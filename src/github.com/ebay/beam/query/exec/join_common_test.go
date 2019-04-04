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
	"errors"
	"testing"

	"github.com/ebay/beam/facts/cache"
	"github.com/ebay/beam/query/parser"
	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/rpc"
	"github.com/stretchr/testify/assert"
)

// This tests the LoopJoiner & HashJoiner types which perform the actual join
// for the 2 different join operators.
func Test_Joiners(t *testing.T) {
	left := ResultChunk{
		Columns: Columns{varS},
		Values: []Value{
			Value{KGObject: rpc.AKID(10)},
			Value{KGObject: rpc.AKID(20)},
			Value{KGObject: rpc.AKID(30)},
		},
		offsets: []uint32{0, 0, 1},
		Facts:   []FactSet{fs(1), fs(2), fs(3)},
	}
	// the left has 3 rows, the right returns 2 matching facts for the first
	// left row, and one matching fact for the last left row. There are no
	// matches for the middle left row. so the 3 expected results are
	//		left[0] + right[0],
	//		left[0] + right[1],
	//		left[2] + right[2]
	right := []ResultChunk{{
		Columns: Columns{varP, varS},
		Values: []Value{
			{KGObject: rpc.AKID(101)}, {KGObject: rpc.AKID(10)},
			{KGObject: rpc.AKID(103)}, {KGObject: rpc.AKID(10)},
		},
		offsets: []uint32{0, 0},
		Facts:   []FactSet{fs(10), fs(11)},
	}, {
		Columns: Columns{varP, varS},
		Values: []Value{
			{KGObject: rpc.AKID(201)}, {KGObject: rpc.AKID(30)},
		},
		offsets: []uint32{2},
		Facts:   []FactSet{fs(20)},
	}}
	expectedInnerJoinResult := ResultChunk{
		Columns: Columns{varS, varP},
		Values: []Value{
			Value{KGObject: rpc.AKID(10)}, {KGObject: rpc.AKID(101)},
			Value{KGObject: rpc.AKID(10)}, {KGObject: rpc.AKID(103)},
			Value{KGObject: rpc.AKID(30)}, {KGObject: rpc.AKID(201)},
		},
		offsets: []uint32{0, 0, 1},
		Facts: []FactSet{
			fs(1, 10), fs(1, 11), fs(3, 20),
		},
	}
	// in the left join case, the left middle row, which got no results from
	// the right should get generated as a result as well.
	expectedLeftJoinResults := ResultChunk{
		Columns: expectedInnerJoinResult.Columns,
		Values:  make([]Value, len(expectedInnerJoinResult.Values)),
		offsets: make([]uint32, len(expectedInnerJoinResult.offsets)),
		Facts:   make([]FactSet, len(expectedInnerJoinResult.Facts)),
	}
	copy(expectedLeftJoinResults.Values, expectedInnerJoinResult.Values)
	copy(expectedLeftJoinResults.offsets, expectedInnerJoinResult.offsets)
	copy(expectedLeftJoinResults.Facts, expectedInnerJoinResult.Facts)

	expectedLeftJoinResults.Values = append(expectedLeftJoinResults.Values,
		Value{KGObject: rpc.AKID(20)}, Value{},
	)
	expectedLeftJoinResults.Facts = append(expectedLeftJoinResults.Facts, fs(2))
	expectedLeftJoinResults.offsets = append(expectedLeftJoinResults.offsets, 0)

	loopJoinerTest := func(t *testing.T, isInnerJoin bool, expectedRes ResultChunk) {
		resCh := make(chan ResultChunk, 10)
		cols, joiner := joinedColumns(Columns{varS}, Columns{varP, varS})
		rb := newChunkBuilder(resCh, cols)
		lj := loopJoiner{
			outputTo: rb,
			joiner:   joiner,
		}
		rightResCh := make(chan ResultChunk, 4)
		go func() {
			if isInnerJoin {
				lj.eqJoin(ctx, &left, rightResCh)
			} else {
				lj.leftJoin(ctx, &left, rightResCh)
			}
			rb.flush(ctx)
			close(resCh)
		}()
		for _, c := range right {
			rightResCh <- c
		}
		close(rightResCh)
		result := <-resCh
		assert.Empty(t, resCh, "Should be no more results")

		assert.Equal(t, expectedRes, result)
	}

	t.Run("LoopJoin.innerJoin", func(t *testing.T) {
		loopJoinerTest(t, true, expectedInnerJoinResult)
	})
	t.Run("LoopJoin.leftJoin", func(t *testing.T) {
		loopJoinerTest(t, false, expectedLeftJoinResults)
	})

	hashJoinerTest := func(t *testing.T, isInnerJoin bool, expectedRes ResultChunk) {
		resCh := make(chan ResultChunk, 10)
		rightResCh := make(chan ResultChunk, 4)
		cols, joiner := joinedColumns(Columns{varS}, Columns{varP, varS})
		rb := newChunkBuilder(resCh, cols)

		hj := hashJoiner{
			leftJoinValues: make(map[string][]rowValue),
			joinVars:       plandef.VarSet{varS},
			outputTo:       rb,
			rightColumns:   right[0].Columns,
			rightResCh:     rightResCh,
			joiner:         joiner,
		}

		leftColumnIndexes := left.Columns.MustIndexesOf(Columns{varS})
		for i := range left.offsets {
			k := left.identityKeysOf(i, leftColumnIndexes)
			row := rowValue{
				offset: left.offsets[i],
				fact:   left.Facts[i],
				vals:   left.Row(i),
			}
			hj.leftJoinValues[string(k)] = append(hj.leftJoinValues[string(k)], row)
		}
		go func() {
			if isInnerJoin {
				hj.eqJoin(ctx)
			} else {
				hj.leftJoin(ctx)
			}
			rb.flush(ctx)
			close(resCh)
		}()
		for _, c := range right {
			rightResCh <- c
		}
		close(rightResCh)
		result := <-resCh
		assert.Empty(t, resCh, "Should be no more results")

		assert.Equal(t, expectedRes, result)
	}

	t.Run("HashJoin.innerJoin", func(t *testing.T) {
		hashJoinerTest(t, true, expectedInnerJoinResult)
	})
	t.Run("HashJoin.leftJoin", func(t *testing.T) {
		hashJoinerTest(t, false, expectedLeftJoinResults)
	})
}

func Test_JoinInputErrors(t *testing.T) {
	queryOpErr := errors.New("it broke")
	errorOp := makeInputOpWithChunks(nil, queryOpErr, Columns{varS})
	chunk := ResultChunk{
		Columns: Columns{varS},
		Values:  []Value{kidVal(10), kidVal(11)},
		offsets: []uint32{0, 0},
		Facts:   []FactSet{fs(1), fs(2)},
	}
	dataOp := makeInputOpWithChunks([]ResultChunk{chunk}, nil, Columns{varS})

	hashJoinBuilder := func(inputs ...queryOperator) operator {
		hashJoinPlan := &plandef.HashJoin{
			Variables: plandef.VarSet{varS},
		}
		return newHashJoin(hashJoinPlan, inputs)
	}
	loopJoinBuilder := func(inputs ...queryOperator) operator {
		loopJoinPlan := &plandef.LoopJoin{
			Variables: plandef.VarSet{varS},
		}
		return newLoopJoin(loopJoinPlan, inputs)
	}
	test := func(t *testing.T, queryOpBuilder func(inputs ...queryOperator) operator) {
		_, err := executeOp(t, queryOpBuilder(errorOp, dataOp), new(defaultBinder))
		assert.Equal(t, queryOpErr, err)
		_, err = executeOp(t, queryOpBuilder(dataOp, errorOp), new(defaultBinder))
		assert.Equal(t, queryOpErr, err)
	}

	t.Run("hashJoin", func(t *testing.T) { test(t, hashJoinBuilder) })
	t.Run("loopJoin", func(t *testing.T) { test(t, loopJoinBuilder) })
}

func Test_OpsVerifyInputCount(t *testing.T) {
	noopQueryOp := makeInputOpWithChunks(nil, nil, Columns{varS})
	inputs := []queryOperator{noopQueryOp, noopQueryOp, noopQueryOp}

	hjPlan := &plandef.HashJoin{}
	ljPlan := &plandef.LoopJoin{}
	assert.Panics(t, func() { newHashJoin(hjPlan, nil) })
	assert.Panics(t, func() { newHashJoin(hjPlan, inputs[:1]) })
	assert.Panics(t, func() { newHashJoin(hjPlan, inputs[:3]) })
	assert.Panics(t, func() { newLoopJoin(ljPlan, nil) })
	assert.Panics(t, func() { newLoopJoin(ljPlan, inputs[:1]) })
	assert.Panics(t, func() { newLoopJoin(ljPlan, inputs[:3]) })

	lsPlan := &plandef.LookupS{}
	lspPlan := &plandef.LookupSP{}
	lspoPlan := &plandef.LookupSPO{}
	lpoPlan := &plandef.LookupPO{}
	lpocPlan := &plandef.LookupPOCmp{}
	assert.Panics(t, func() { newLookupS(1, nil, lsPlan, inputs[:1]) })
	assert.Panics(t, func() { newLookupSP(1, nil, lspPlan, inputs[:1]) })
	assert.Panics(t, func() { newLookupSPO(1, nil, lspoPlan, inputs[:1]) })
	assert.Panics(t, func() { newLookupPO(1, nil, lpoPlan, inputs[:1]) })
	assert.Panics(t, func() { newLookupPOCmp(1, nil, lpocPlan, inputs[:1]) })

	ispPlan := &plandef.InferSP{}
	ispoPlan := &plandef.InferSPO{}
	ipoPlan := &plandef.InferPO{}
	assert.Panics(t, func() { newInferSP(1, nil, ispPlan, inputs[:1]) })
	assert.Panics(t, func() { newInferSPO(1, nil, cache.New(), ispoPlan, inputs[:1]) })
	assert.Panics(t, func() { newInferPO(1, nil, ipoPlan, inputs[:1]) })

	selPlan := &plandef.SelectLit{}
	assert.Panics(t, func() { newSelectLitOp(selPlan, nil) })
	assert.Panics(t, func() { newSelectLitOp(selPlan, inputs[:2]) })
	assert.Panics(t, func() { newSelectLitOp(selPlan, inputs) })
}

// joinDataSet contains test data for testing join execution. Test_LoopJoin and
// Test_HashJoin use this to drive their tests.
type joinDataSet struct {
	name string
	// joinVars is the set of variables to join left & right with. They are used
	// to construct a join operator
	joinVars plandef.VarSet
	// joinType indicates the type of join to execute.
	joinType parser.MatchSpecificity
	// The join is executed using this binder
	inputBinder valueBinder
	// left contains the input data to the left side of the join operator. For
	// hash joins, one chunk from this is returned for each execution. As
	// loopJoin supports bulk execution, all chunks are returned on the first
	// execution for loopJoin tests.
	left []ResultChunk
	// right contains data like a hash join was performed, i.e. it may
	// contain rows that aren't joined. For hash joins one chunk from this is
	// returned for each execution. Offsets in these chunks should be 0
	// as each hashjoin is executed serially. LoopJoin tests will apply the
	// binding filters to generate their right output from this starting data.
	right []ResultChunk
	// The expected results of an inner join.
	expResults ResultChunk
	// The additional (in addition to the expResults) expected results for a
	// left join. No need to set columns here as they should be the same as
	// expResults columns.
	expLeftJoin ResultChunk
}

func joinDataSets() []joinDataSet {
	ds1 := joinDataSet{
		name:        "bulk_binder",
		joinVars:    plandef.VarSet{varS},
		joinType:    parser.MatchRequired,
		inputBinder: newTestBinder(varO, 1, 2),
		left: []ResultChunk{{
			Columns: Columns{varS, varO},
			Values: []Value{
				kidVal(2), kidVal(1),
				kidVal(4), kidVal(1),
				kidVal(3), kidVal(1),
				kidVal(2), kidVal(1),
			},
			offsets: []uint32{0, 0, 0, 0},
			Facts:   []FactSet{fs(11), fs(12), fs(13), fs(14)},
		}, {
			Columns: Columns{varS, varO},
			Values: []Value{
				kidVal(22), kidVal(2),
				kidVal(23), kidVal(2),
			},
			offsets: []uint32{1, 1},
			Facts:   []FactSet{fs(21), fs(22)},
		}},
		right: []ResultChunk{{
			Columns: Columns{varS, varP},
			Values: []Value{
				kidVal(2), kidVal(1001),
				kidVal(3), kidVal(1002),
				kidVal(2), kidVal(1003),
				kidVal(12), kidVal(1004),
			},
			offsets: []uint32{0, 0, 0, 0},
			Facts:   []FactSet{fs(101), fs(102), fs(103), fs(104)},
		}, {
			Columns: Columns{varS, varP},
			Values: []Value{
				kidVal(23), kidVal(1005),
			},
			offsets: []uint32{1},
			Facts:   []FactSet{fs(202)},
		}},
		expResults: ResultChunk{
			Columns: Columns{varS, varO, varP},
			Values: []Value{
				kidVal(2), kidVal(1), kidVal(1001),
				kidVal(2), kidVal(1), kidVal(1003),
				kidVal(3), kidVal(1), kidVal(1002),
				kidVal(2), kidVal(1), kidVal(1001),
				kidVal(2), kidVal(1), kidVal(1003),
				kidVal(23), kidVal(2), kidVal(1005),
			},
			offsets: []uint32{0, 0, 0, 0, 0, 1},
			Facts: []FactSet{
				fs(11, 101), fs(14, 101), fs(13, 102),
				fs(11, 103), fs(14, 103), fs(22, 202),
			},
		},
		expLeftJoin: ResultChunk{
			Values: []Value{
				kidVal(4), kidVal(1), {},
				kidVal(22), kidVal(2), {},
			},
			offsets: []uint32{0, 1},
			Facts:   []FactSet{fs(12), fs(21)},
		},
	}
	ds2 := joinDataSet{
		name:        "2_vars",
		joinVars:    plandef.VarSet{varS, varP},
		joinType:    parser.MatchRequired,
		inputBinder: new(defaultBinder),
		left: []ResultChunk{{
			Columns: Columns{varS, varP},
			Values: []Value{
				kidVal(2), kidVal(3),
				kidVal(4), kidVal(3),
				kidVal(3), kidVal(3),
				kidVal(2), kidVal(3),
			},
			offsets: []uint32{0, 0, 0, 0},
			Facts:   []FactSet{fs(1), fs(2), fs(3), fs(4)},
		}},
		right: []ResultChunk{{
			Columns: Columns{varS, varP, varO},
			Values: []Value{
				kidVal(2), kidVal(3), kidVal(11),
				kidVal(3), kidVal(3), kidVal(12),
				kidVal(2), kidVal(10), kidVal(13),
				kidVal(12), kidVal(10), kidVal(14),
				kidVal(3), kidVal(3), kidVal(15),
			},
			offsets: []uint32{0, 0, 0, 0, 0},
			Facts:   []FactSet{fs(11), fs(12), fs(13), fs(14), fs(15)},
		}},
		expResults: ResultChunk{
			Columns: Columns{varS, varP, varO},
			Values: []Value{
				kidVal(2), kidVal(3), kidVal(11),
				kidVal(2), kidVal(3), kidVal(11),
				kidVal(3), kidVal(3), kidVal(12),
				kidVal(3), kidVal(3), kidVal(15),
			},
			offsets: []uint32{0, 0, 0, 0},
			Facts:   []FactSet{fs(1, 11), fs(4, 11), fs(3, 12), fs(3, 15)},
		},
		expLeftJoin: ResultChunk{
			Values: []Value{
				kidVal(4), kidVal(3), {},
			},
			offsets: []uint32{0},
			Facts:   []FactSet{fs(2)},
		},
	}
	return []joinDataSet{ds1, ds2,
		ds1.reversedInnerJoin(), ds2.reversedInnerJoin(),
		ds1.leftJoin(), ds2.leftJoin()}
}

// leftJoin returns a version of this dataset for the left join case.
func (ds *joinDataSet) leftJoin() joinDataSet {
	exp := ResultChunk{
		Columns: ds.expResults.Columns,
		Values:  append(ds.expResults.Values, ds.expLeftJoin.Values...),
		offsets: append(ds.expResults.offsets, ds.expLeftJoin.offsets...),
		Facts:   append(ds.expResults.Facts, ds.expLeftJoin.Facts...),
	}
	left := joinDataSet{
		name:        ds.name + "_left",
		joinVars:    ds.joinVars,
		joinType:    parser.MatchOptional,
		left:        ds.left,
		right:       ds.right,
		inputBinder: ds.inputBinder,
		expResults:  exp,
	}
	return left
}

// reversedInnerJoin returns a new joinDataSet that has left and right swapped.
// For inner join, join(a,b) and join(b,a) should generate the same logical
// results. The actual ResultChunks may be different because join(a,b) may
// produce its output columns in a different order to join(b,a). This difference
// is reflected in the new data sets expected results.
func (ds *joinDataSet) reversedInnerJoin() joinDataSet {
	exp := ds.expResults
	exp.Columns, _ = joinedColumns(ds.right[0].Columns, ds.left[0].Columns)
	exp.Values = make([]Value, len(ds.expResults.Values))
	for srcColIdx, srcCol := range ds.expResults.Columns {
		dest := exp.Columns.MustIndexOf(srcCol)
		src := srcColIdx
		for range exp.offsets {
			exp.Values[dest] = ds.expResults.Values[src]
			dest += len(exp.Columns)
			src += len(exp.Columns)
		}
	}
	swapped := joinDataSet{
		name:        ds.name + "_reversed",
		joinVars:    ds.joinVars,
		joinType:    parser.MatchRequired,
		left:        ds.right,
		right:       ds.left,
		inputBinder: ds.inputBinder,
		expResults:  exp,
	}
	return swapped
}
