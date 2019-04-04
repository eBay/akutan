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
	"math/rand"
	"testing"

	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/query/planner/search"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/random"
	"github.com/stretchr/testify/assert"
)

func Test_SelectLit(t *testing.T) {
	// inputFacts0 are sent as offset[0] for tests
	var inputFacts0 []rpc.Fact
	// inputFacts1 are sent as offset[1] for bulk tests
	inputFacts1 := []rpc.Fact{
		{Id: 101, Index: 2, Subject: 10, Predicate: 4, Object: rpc.AString("arrow", 0)},
		{Id: 102, Index: 2, Subject: 11, Predicate: 4, Object: rpc.AString("belt", 0)},
		{Id: 103, Index: 2, Subject: 12, Predicate: 4, Object: rpc.AString("car", 0)},
		{Id: 104, Index: 2, Subject: 13, Predicate: 4, Object: rpc.AString("zebra", 0)},
		{Id: 105, Index: 2, Subject: 20, Predicate: 4, Object: rpc.AInt64(12, 0)},
		{Id: 106, Index: 2, Subject: 21, Predicate: 4, Object: rpc.AInt64(43, 0)},
		{Id: 107, Index: 2, Subject: 22, Predicate: 4, Object: rpc.AInt64(44, 0)},
		{Id: 108, Index: 2, Subject: 23, Predicate: 4, Object: rpc.AInt64(123, 0)},
	}

	// useful plan builder functions
	mkPlan := func(comp rpc.Operator, l1 rpc.KGObject) *plandef.SelectLit {
		return &plandef.SelectLit{
			Test: varO,
			Clauses: []plandef.SelectClause{{
				Comparison: comp,
				Literal1:   &plandef.Literal{Value: l1},
			}},
		}
	}
	mkPlanRange := func(comp rpc.Operator, l1, l2 rpc.KGObject) *plandef.SelectLit {
		return &plandef.SelectLit{
			Test: varO,
			Clauses: []plandef.SelectClause{{
				Comparison: comp,
				Literal1:   &plandef.Literal{Value: l1},
				Literal2:   &plandef.Literal{Value: l2},
			}},
		}
	}
	var selectTests []selectTest
	// pass 4 rows of the same KGObject type that are in ascending order and this
	// will generate tests for each of the comparison operators and add the facts
	// to the inputFacts0 list.
	// we want each test to run with a mix of input fact types to help verify we
	// don't get any weird cross KGObject type problems, which is why we accumulate
	// the test facts up into a single list reused for every test
	mkComparisonsTests := func(rows []rpc.Fact) {
		if len(rows) != 5 {
			t.Fatal("mkComparisonsTests should be passed at exactly 5 rows of facts")
		}
		baseIndex := len(inputFacts0)
		tLess := selectTest{
			planOp:           mkPlan(rpc.OpLess, rows[3].Object),
			expResultIndexes: [][]int{{baseIndex, baseIndex + 1, baseIndex + 2}},
		}
		tLessOrEqual := selectTest{
			planOp:           mkPlan(rpc.OpLessOrEqual, rows[1].Object),
			expResultIndexes: [][]int{{baseIndex, baseIndex + 1}},
		}
		tGreater := selectTest{
			planOp:           mkPlan(rpc.OpGreater, rows[1].Object),
			expResultIndexes: [][]int{{baseIndex + 2, baseIndex + 3}},
		}
		tGreaterOrEqual := selectTest{
			planOp:           mkPlan(rpc.OpGreaterOrEqual, rows[1].Object),
			expResultIndexes: [][]int{{baseIndex + 1, baseIndex + 2, baseIndex + 3}},
		}
		tIncInc := selectTest{
			planOp:           mkPlanRange(rpc.OpRangeIncInc, rows[1].Object, rows[3].Object),
			expResultIndexes: [][]int{{baseIndex + 1, baseIndex + 2, baseIndex + 3}},
		}
		tIncExc := selectTest{
			planOp:           mkPlanRange(rpc.OpRangeIncExc, rows[1].Object, rows[3].Object),
			expResultIndexes: [][]int{{baseIndex + 1, baseIndex + 2}},
		}
		tExcExc := selectTest{
			planOp:           mkPlanRange(rpc.OpRangeExcExc, rows[1].Object, rows[3].Object),
			expResultIndexes: [][]int{{baseIndex + 2}},
		}
		tExcInc := selectTest{
			planOp:           mkPlanRange(rpc.OpRangeExcInc, rows[1].Object, rows[3].Object),
			expResultIndexes: [][]int{{baseIndex + 2, baseIndex + 3}},
		}
		selectTests = append(selectTests, tLess, tLessOrEqual, tGreater, tGreaterOrEqual, tIncInc, tIncExc, tExcExc, tExcInc)
		inputFacts0 = append(inputFacts0, rows...)
	}

	mkComparisonsTests([]rpc.Fact{
		{Id: 1, Index: 2, Subject: 10, Predicate: 4, Object: rpc.AString("alice", 0)},
		{Id: 2, Index: 2, Subject: 11, Predicate: 4, Object: rpc.AString("bob", 0)},
		{Id: 3, Index: 2, Subject: 12, Predicate: 4, Object: rpc.AString("cat", 0)},
		{Id: 4, Index: 2, Subject: 13, Predicate: 4, Object: rpc.AString("eve", 0)},
		{Id: 20, Index: 2, Subject: 10, Predicate: 4, Object: rpc.AString("alice", 1)},
	})
	mkComparisonsTests([]rpc.Fact{
		{Id: 5, Index: 2, Subject: 20, Predicate: 4, Object: rpc.AInt64(1, 0)},
		{Id: 6, Index: 2, Subject: 21, Predicate: 4, Object: rpc.AInt64(11, 0)},
		{Id: 7, Index: 2, Subject: 22, Predicate: 4, Object: rpc.AInt64(12, 0)},
		{Id: 8, Index: 2, Subject: 23, Predicate: 4, Object: rpc.AInt64(111, 0)},
		{Id: 21, Index: 2, Subject: 20, Predicate: 4, Object: rpc.AInt64(1, 1)},
	})
	mkComparisonsTests([]rpc.Fact{
		{Id: 10, Index: 2, Subject: 30, Predicate: 4, Object: rpc.AFloat64(1, 0)},
		{Id: 11, Index: 2, Subject: 31, Predicate: 4, Object: rpc.AFloat64(11, 0)},
		{Id: 12, Index: 2, Subject: 32, Predicate: 4, Object: rpc.AFloat64(12, 0)},
		{Id: 13, Index: 2, Subject: 33, Predicate: 4, Object: rpc.AFloat64(111, 0)},
		{Id: 22, Index: 2, Subject: 30, Predicate: 4, Object: rpc.AFloat64(1, 1)},
	})
	mkComparisonsTests([]rpc.Fact{
		{Id: 14, Index: 2, Subject: 40, Predicate: 4, Object: rpc.ATimestampYMD(2018, 5, 5, 0)},
		{Id: 15, Index: 2, Subject: 41, Predicate: 4, Object: rpc.ATimestampYMD(2018, 5, 6, 0)},
		{Id: 16, Index: 2, Subject: 42, Predicate: 4, Object: rpc.ATimestampYMD(2018, 6, 1, 0)},
		{Id: 17, Index: 2, Subject: 43, Predicate: 4, Object: rpc.ATimestampYMD(2018, 6, 3, 0)},
		{Id: 23, Index: 2, Subject: 40, Predicate: 4, Object: rpc.ATimestampYMD(2018, 5, 5, 1)},
	})
	// we don't have any range tests for bools because its not really clear that they're needed or even make any sense
	boolFacts := []rpc.Fact{
		{Id: 18, Index: 2, Subject: 50, Predicate: 5, Object: rpc.ABool(false, 0)},
		{Id: 19, Index: 2, Subject: 50, Predicate: 5, Object: rpc.ABool(true, 0)},
		{Id: 20, Index: 2, Subject: 50, Predicate: 5, Object: rpc.ABool(true, 1)},
	}
	inputFacts0 = append(inputFacts0, boolFacts...)
	kidFacts := []rpc.Fact{
		{Id: 18, Index: 2, Subject: 50, Predicate: 5, Object: rpc.AKID(10)},
		{Id: 19, Index: 2, Subject: 51, Predicate: 5, Object: rpc.AKID(5)},
		{Id: 20, Index: 2, Subject: 52, Predicate: 5, Object: rpc.AKID(55)},
	}
	// returns the offsets into inputFacts0 of the supplied factIDs
	offsetsOf := func(factIDs ...uint64) []int {
		res := make([]int, len(factIDs))
		for i, id := range factIDs {
			for offset, f := range inputFacts0 {
				if f.Id == id {
					res[i] = offset
					break
				}
			}
		}
		return res
	}
	otherTests := []selectTest{{
		planOp:           mkPlan(rpc.OpEqual, rpc.AString("cat", 0)),
		expResultIndexes: [][]int{offsetsOf(3)},
	}, {
		planOp:           mkPlan(rpc.OpEqual, rpc.AString("Cat", 0)),
		expResultIndexes: [][]int{},
	}, {
		planOp:           mkPlan(rpc.OpEqual, rpc.AString("cat", 2)),
		expResultIndexes: [][]int{},
	}, {
		planOp:           mkPlan(rpc.OpNotEqual, rpc.AString("car", 0)),
		input:            [][]rpc.Fact{inputFacts1},
		expResultIndexes: [][]int{{0, 1, 3, 4, 5, 6, 7}},
	}, {
		planOp:           mkPlan(rpc.OpNotEqual, rpc.AInt64(43, 0)),
		input:            [][]rpc.Fact{inputFacts1},
		expResultIndexes: [][]int{{0, 1, 2, 3, 4, 6, 7}},
	}, {
		planOp:           mkPlan(rpc.OpNotEqual, rpc.AInt64(1443, 0)),
		input:            [][]rpc.Fact{inputFacts1},
		expResultIndexes: [][]int{{0, 1, 2, 3, 4, 5, 6, 7}},
	}, {
		planOp:           mkPlan(rpc.OpNotEqual, rpc.AKID(5)),
		input:            [][]rpc.Fact{kidFacts},
		expResultIndexes: [][]int{{0, 2}},
	}, {
		planOp:           mkPlan(rpc.OpPrefix, rpc.AString("c", 0)),
		expResultIndexes: [][]int{offsetsOf(3)},
	}, {
		planOp:           mkPlan(rpc.OpGreater, rpc.ABool(false, 0)),
		expResultIndexes: [][]int{offsetsOf(19)},
	}, {
		planOp:           mkPlan(rpc.OpGreaterOrEqual, rpc.ABool(false, 0)),
		expResultIndexes: [][]int{offsetsOf(18, 19)},
	}, {
		planOp:           mkPlan(rpc.OpGreaterOrEqual, rpc.ABool(true, 0)),
		expResultIndexes: [][]int{offsetsOf(19)},
	}, {
		planOp:           mkPlan(rpc.OpLess, rpc.ABool(true, 0)),
		expResultIndexes: [][]int{offsetsOf(18)},
	}, {
		planOp:           mkPlan(rpc.OpLessOrEqual, rpc.ABool(true, 0)),
		expResultIndexes: [][]int{offsetsOf(18, 19)},
	}, {
		planOp:           mkPlan(rpc.OpLessOrEqual, rpc.ABool(false, 0)),
		expResultIndexes: [][]int{offsetsOf(18)},
	}, {
		planOp:           mkPlan(rpc.OpGreater, rpc.AInt64(20, 0)),
		binder:           newTestBinder(varS, 0, 0), // bind 2 rows, the Op will see data from inputFacts0 & inputFacts1
		expResultIndexes: [][]int{offsetsOf(8), {5, 6, 7}},
	}}
	selectTests = append(selectTests, otherTests...)

	// multiple comparator test
	mcTest := selectTest{
		planOp: &plandef.SelectLit{
			Test: varO,
			Clauses: []plandef.SelectClause{
				{Comparison: rpc.OpGreater, Literal1: &plandef.Literal{Value: rpc.AInt64(10, 0)}},
				{Comparison: rpc.OpLess, Literal1: &plandef.Literal{Value: rpc.AInt64(20, 0)}},
			},
		},
		expResultIndexes: [][]int{offsetsOf(6, 7)},
	}
	selectTests = append(selectTests, mcTest)

	inputFacts := [][]rpc.Fact{inputFacts0, inputFacts1}

	for _, test := range selectTests {
		test.variables = variableSet{nil, nil, nil, varO}
		if test.input == nil {
			test.input = inputFacts
		}
		test.run(t, func(op search.Operator, input queryOperator) operator {
			return newSelectLitOp(op.(*plandef.SelectLit), []queryOperator{input})
		})
	}
}

func Test_SelectVar(t *testing.T) {
	inputFacts := [][]rpc.Fact{
		{
			{Subject: 4, Predicate: 4, Object: rpc.AKID(5)},
			{Subject: 14, Predicate: 15, Object: rpc.AKID(15)},
		}, {
			{Subject: 10, Predicate: 11, Object: rpc.AKID(12)},
			{Subject: 12, Predicate: 12, Object: rpc.AKID(13)},
		}, {
			{Subject: 100, Predicate: 11, Object: rpc.AKID(12)},
			{Subject: 120, Predicate: 12, Object: rpc.AKID(13)},
			{Subject: 130, Predicate: 12, Object: rpc.AKID(13)},
		},
	}
	tests := []selectTest{{
		planOp:    &plandef.SelectVar{Left: varS, Operator: rpc.OpNotEqual, Right: varP},
		binder:    newTestBinder(varI, 1, 2, 3),
		variables: variableSet{nil, varS, varP, nil},
		input:     inputFacts,
		expResultIndexes: [][]int{
			{1}, {0}, {0, 1, 2},
		},
	}, {
		planOp:    &plandef.SelectVar{Left: varS, Operator: rpc.OpEqual, Right: varP},
		binder:    newTestBinder(varI, 1, 2, 3),
		variables: variableSet{nil, varS, varP, nil},
		input:     inputFacts,
		expResultIndexes: [][]int{
			{0}, {1},
		},
	}}

	for _, test := range tests {
		test.run(t, func(op search.Operator, input queryOperator) operator {
			return newSelectVarOp(op.(*plandef.SelectVar), []queryOperator{input})
		})
	}
}

func Test_selectVarCompareOp(t *testing.T) {
	cols := Columns{varI, varS, varP}
	fsEqSP := []Value{kidVal(99), kidVal(142), kidVal(142)}
	fsNotEqSP := []Value{kidVal(333), kidVal(111), kidVal(333)}
	fsInts := []Value{kidVal(33), {KGObject: rpc.AInt64(12, 13)}, {KGObject: rpc.AInt64(12, 13)}}
	fsMixed := []Value{kidVal(42), {KGObject: rpc.AString("Bob", 1)}, {KGObject: rpc.AInt64(42, 2)}}

	op := &plandef.SelectVar{Left: varS, Operator: rpc.OpNotEqual, Right: varP}
	compareOp := selectVarCompareOp(op, cols)
	assert.True(t, compareOp(fsNotEqSP))
	assert.False(t, compareOp(fsEqSP))
	assert.False(t, compareOp(fsInts))
	assert.True(t, compareOp(fsMixed))

	op = &plandef.SelectVar{Left: varP, Operator: rpc.OpEqual, Right: varI}
	compareOp = selectVarCompareOp(op, cols)
	assert.False(t, compareOp(fsEqSP))
	assert.False(t, compareOp(fsInts))
	assert.True(t, compareOp(fsNotEqSP))
	assert.False(t, compareOp(fsMixed))

	op = &plandef.SelectVar{Left: varS, Operator: rpc.OpEqual, Right: varP}
	compareOp = selectVarCompareOp(op, cols)
	assert.True(t, compareOp(fsEqSP))
	assert.False(t, compareOp(fsNotEqSP))
	assert.True(t, compareOp(fsInts))
	assert.False(t, compareOp(fsMixed))

	op = &plandef.SelectVar{Left: varS, Operator: rpc.OpIn, Right: varP}
	assert.PanicsWithValue(t, "selectVarCompareOp operator 'in' not supported",
		func() {
			selectVarCompareOp(op, cols)
		})
}

type selectTest struct {
	// The plan operator to execute, its input will be generated from the facts in 'input'.
	planOp search.Operator
	// Test will use defaultBinder if binder is not explicitly set.
	// The len() of binder will trim the offsets of inputFacts appropriately.
	binder valueBinder
	// The test will run with this set of facts as the input to the op.
	input [][]rpc.Fact
	// The input Op created will be configured with this variableSet.
	variables variableSet
	// The results are expected to contain the facts at these indexes into input
	// the outer array is for each bulk offset.
	expResultIndexes [][]int
}

func (test *selectTest) run(t *testing.T, opBuilder func(op search.Operator, input queryOperator) operator) {
	binder := test.binder
	if binder == nil {
		binder = new(defaultBinder)
	}
	name := test.planOp.String()
	if binder.len() > 1 {
		name += " [bulk]"
	}
	t.Run(name, func(t *testing.T) {
		// the order that facts appear from the child op shouldn't matter
		// to the select op, so we'll make a copy and randomly shuffle
		// each list of facts to feed into the selectOp
		// we copy them because the expected results are described in terms
		// of offsets into test.input
		inputOp := makeInputOp(copyAndShuffle(test.input), test.variables)
		op := opBuilder(test.planOp, inputOp)
		// build expected results from supplied indexes into input
		exp := ResultChunk{
			Columns: test.variables.columns(),
		}
		for offset, expResultIndexes := range test.expResultIndexes {
			for _, resultFactIdx := range expResultIndexes {
				fact := test.input[offset][resultFactIdx]
				fs := FactSet{Facts: []rpc.Fact{fact}}
				exp.offsets = append(exp.offsets, uint32(offset))
				exp.Facts = append(exp.Facts, fs)
				exp.Values = append(exp.Values, test.variables.resultsOf(&fact)...)
			}
		}
		res, err := executeOp(t, op, binder)
		assert.NoError(t, err)
		assertResultChunkEqual(t, exp.sorted(t), res.sorted(t))
	})
}

func init() {
	random.SeedMath()
}

func copyAndShuffle(s [][]rpc.Fact) [][]rpc.Fact {
	res := make([][]rpc.Fact, len(s))
	for i := range s {
		n := make([]rpc.Fact, len(s[i]))
		copy(n, s[i])
		rand.Shuffle(len(n), func(i, j int) {
			n[i], n[j] = n[j], n[i]
		})
		res[i] = n
	}
	return res
}
