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
	"testing"

	"github.com/ebay/beam/query/parser"
	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/rpc"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_LoopJoin(t *testing.T) {
	for _, test := range joinDataSets() {
		t.Run(test.name, func(t *testing.T) {
			loopJoinPlan := &plandef.LoopJoin{
				Variables:   test.joinVars,
				Specificity: test.joinType,
			}
			leftOp := makeInputOpWithChunks(test.left, nil, test.left[0].Columns)
			rightOp := &mockQueryOp{
				cols: test.right[0].Columns,
				exec: func(ctx context.Context, binder valueBinder, resCh chan<- ResultChunk) error {
					// we'll need to manually evaluate what the subset of results should be
					// based on the input binder.
					bindings := make([]*plandef.Binding, len(test.joinVars))
					for i, v := range test.joinVars {
						bindings[i] = &plandef.Binding{Var: v}
					}
					rb := newChunkBuilder(resCh, test.right[0].Columns)
					for bulkIndex := uint32(0); bulkIndex < binder.len(); bulkIndex++ {
						for _, chunk := range test.right {
							for i := range chunk.offsets {
								row := chunk.Row(i)
								match := true
								for _, b := range bindings {
									bindVal := binder.bind(bulkIndex, b)
									colIdx, exists := chunk.Columns.IndexOf(b.Var)
									assert.True(t, exists, "variable %s should be in columns %s", b.Var, chunk.Columns)
									match = match && bindVal.KGObject.Equal(row[colIdx].KGObject)
								}
								if match {
									rb.add(ctx, bulkIndex, chunk.Facts[i], row)
								}
							}
						}
					}
					rb.flush(ctx)
					close(resCh)
					return nil
				},
			}
			joinOp := newLoopJoin(loopJoinPlan, []queryOperator{leftOp, rightOp})
			res, err := executeOp(t, joinOp, test.inputBinder)
			assert.NoError(t, err)
			assertResultChunkEqual(t, test.expResults.sorted(t), res.sorted(t))
		})
	}
}

func Test_LoopJoinPanicsOnBogusSpecificity(t *testing.T) {
	loopJoinOp := plandef.LoopJoin{
		Variables:   plandef.VarSet{varS},
		Specificity: parser.MatchSpecificity(42),
	}
	defer func() {
		// logrus.Panic panics with a logrus.Entry as the panic value
		// which includes an embedded timestamp. This makes it impossible to
		// test with assert.PanicsWithValue
		p := recover()
		assert.NotNil(t, p)
		exp := "Unexpected value for Join Specificity MatchSpecificity(42)"
		msg, err := p.(*logrus.Entry).String()
		assert.NoError(t, err)
		assert.Contains(t, msg, exp)
	}()
	newLoopJoin(&loopJoinOp, []queryOperator{nil, nil})
}

func Test_LoopJoinParentBindings(t *testing.T) {
	// test that a binding value from a loopJoin at the top of the execution tree
	// is available multiple levels lower.
	rootLoopJoin := &plandef.LoopJoin{
		Variables: plandef.VarSet{varP},
	}
	rootLeftOp := makeInputOp([][]rpc.Fact{
		{{Index: 1, Subject: 2, Predicate: 3, Object: rpc.AKID(4)}}}, variableSet{nil, nil, varP, nil},
	)
	childLoopJoin := &plandef.LoopJoin{
		Variables: plandef.VarSet{varO},
	}
	childLeftOp := makeInputOp([][]rpc.Fact{
		{{Index: 1, Subject: 2, Predicate: 3, Object: rpc.AKID(4)}}}, variableSet{nil, nil, varP, varO},
	)
	childRightOpCalled := false
	childRightOp := &mockQueryOp{
		exec: func(ctx context.Context, binder valueBinder, resCh chan<- ResultChunk) error {
			childRightOpCalled = true
			vr := binder.bind(0, &plandef.Binding{Var: varP})
			if assert.NotNil(t, vr) {
				assert.Equal(t, uint64(3), vr.KGObject.ValKID())
			}
			close(resCh)
			return nil
		},
	}
	childJoinQueryOp := &decoratedOp{events: ignoreEvents{}, op: newLoopJoin(childLoopJoin, []queryOperator{childLeftOp, childRightOp})}
	rootJoinQueryOp := newLoopJoin(rootLoopJoin, []queryOperator{rootLeftOp, childJoinQueryOp})
	res := resultsCollector{}
	err := rootJoinQueryOp.execute(context.Background(), new(defaultBinder), &res)
	assert.NoError(t, err)
	assert.True(t, childRightOpCalled)
}

func Test_JoinedColumns(t *testing.T) {
	l := Columns{varS, varP, varO}
	r := Columns{varS, varI}
	cols, joiner := joinedColumns(l, r)
	assert.Equal(t, Columns{varS, varP, varO, varI}, cols)

	lv := []Value{kidVal(10), kidVal(11), kidVal(12)}
	rv := []Value{kidVal(10), kidVal(20)}
	outRow := joiner(lv, rv)
	assert.Equal(t, []Value{kidVal(10), kidVal(11), kidVal(12), kidVal(20)}, outRow)

	outRow = joiner(lv, nil)
	assert.Equal(t, []Value{kidVal(10), kidVal(11), kidVal(12), Value{}}, outRow)

	assert.Panics(t, func() {
		joiner([]Value{kidVal(10)}, rv)
	})
	assert.Panics(t, func() {
		joiner(lv, []Value{kidVal(10)})
	})
}

func Test_JoinedColumnsNoNewVars(t *testing.T) {
	l := Columns{varS, varP}
	r := Columns{varP}
	cols, joiner := joinedColumns(l, r)
	assert.Equal(t, Columns{varS, varP}, cols)

	lv := []Value{kidVal(10), kidVal(11)}
	rv := []Value{kidVal(11)}
	outRow := joiner(lv, rv)
	assert.Equal(t, lv, outRow)

	outRow = joiner(lv, nil)
	assert.Equal(t, lv, outRow)
}
