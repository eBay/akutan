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
	"testing"

	"github.com/ebay/beam/facts/cache"
	"github.com/ebay/beam/query/parser"
	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/clocks"
	"github.com/ebay/beam/viewclient/lookups/mocklookups"
	"github.com/stretchr/testify/assert"
)

// This padding is added to channel's buffer to avoid the test wedging if the
// actual number of items sent on the channel is a bit higher than expected.
const chanPadding = 8

// Tests enumerateOp in isolation when the Output is a variable.
func Test_Enumerate_unbound(t *testing.T) {
	runTest := func(t *testing.T, binder valueBinder, expectedRepeat int) {
		assert := assert.New(t)
		fooVar := &plandef.Variable{Name: "foo"}
		opDef := &plandef.Enumerate{
			Output: fooVar,
			Values: []plandef.FixedTerm{
				&plandef.OID{Value: 10},
				&plandef.OID{Value: 20},
				// Normally these values have to be the same type, but this gets coverage of the
				// KGObject case.
				&plandef.Literal{Value: rpc.AString("Jamaica", 12)},
			},
		}
		op := &decoratedOp{events: ignoreEvents{}, op: &enumerateOp{opDef}}
		resCh := make(chan ResultChunk, 4)
		err := op.run(context.Background(), binder, resCh)
		assert.NoError(err)
		if assert.True(len(resCh) > 0) {
			res := <-resCh
			if assert.Equal(expectedRepeat*3, res.NumRows()) { // 3 because there were 3 values in the plan
				assert.Equal(Columns{fooVar}, res.Columns)
				for offset, p := 0, 0; offset < expectedRepeat; offset, p = offset+1, p+3 {
					assert.Empty(res.Facts[p])
					assert.Equal("#10", res.Values[p].String())
					assert.Equal(offset, int(res.offsets[p]), "incorrect bulkOffset")

					assert.Empty(res.Facts[p+1])
					assert.Equal("#20", res.Values[p+1].String())
					assert.Equal(offset, int(res.offsets[p+1]), "incorrect bulkOffset")

					assert.Empty(res.Facts[p+2])
					assert.Equal(`"Jamaica"@#12`, res.Values[p+2].String())
					assert.Equal(offset, int(res.offsets[p+2]), "incorrect bulkOffset")
				}
			}
		}
	}
	t.Run("defaultBinder", func(t *testing.T) {
		runTest(t, new(defaultBinder), 1)
	})
	t.Run("lenTwoBinder", func(t *testing.T) {
		binder := newTestBinder(varS, 1, 2)
		runTest(t, binder, 2)
	})
}

// Tests enumerateOp in isolation when the Output is a binding.
// Also provides sufficient testing for containsValue.
func Test_Enumerate_bound(t *testing.T) {
	assert := assert.New(t)
	fooVar := &plandef.Variable{Name: "foo"}
	fooBinding := &plandef.Binding{Var: fooVar}
	opDef := &plandef.Enumerate{
		Output: fooBinding,
		Values: []plandef.FixedTerm{
			&plandef.OID{Value: 10},
			&plandef.OID{Value: 20},
			// Normally these values have to be the same type, but this gets coverage of the
			// KGObject case.
			&plandef.Literal{Value: rpc.AString("Jamaica", 12)},
			&plandef.Literal{Value: rpc.AString("Cuba", 19)},
		},
	}
	op := &enumerateOp{opDef}
	res := resultsCollector{}
	binder := newTestObjBinder(fooVar,
		rpc.AKID(20),
		rpc.AKID(15),
		rpc.AKID(10),
		rpc.AString("Jamaica", 16),
		rpc.AString("Jamaica", 12))

	err := op.execute(context.Background(), binder, &res)
	assert.NoError(err)
	if assert.True(len(res.rows) > 0) {
		assert.Equal(0, int(res.offsets[0]))
		assert.Empty(res.facts[0])
		assert.Equal("[#20]", fmt.Sprintf("%v", res.rows[0]))
	}
	if assert.True(len(res.rows) > 1) {
		assert.Equal(2, int(res.offsets[1]))
		assert.Empty(res.facts[1])
		assert.Equal("[#10]", fmt.Sprintf("%v", res.rows[1]))
	}
	if assert.True(len(res.rows) > 2) {
		assert.Equal(4, int(res.offsets[2]))
		assert.Empty(res.facts[2])
		assert.Equal(`["Jamaica"@#12]`, fmt.Sprintf("%v", res.rows[2]))
		assert.Equal(uint64(12), res.rows[2][0].KGObject.LangID())
	}
	assert.Len(res.rows, 3, "Unexpected number of results in output")
}

// Tests enumerateOp's behavior in the context of a HashJoin.
func Test_Enumerate_unbound_hashJoin(t *testing.T) {
	fooVar := &plandef.Variable{Name: "foo"}
	plan := &plandef.Plan{
		Operator: &plandef.HashJoin{
			Variables:   plandef.VarSet{fooVar},
			Specificity: parser.MatchRequired,
		},
		Variables: plandef.VarSet{fooVar},
		Inputs: []*plandef.Plan{
			{
				Operator: &plandef.Enumerate{
					Output: fooVar,
					Values: []plandef.FixedTerm{
						&plandef.OID{Value: 10},
						&plandef.OID{Value: 20},
						&plandef.OID{Value: 30},
					},
				},
				Variables: plandef.VarSet{fooVar},
			},
			{
				Operator: &plandef.LookupPO{
					ID:        new(plandef.DontCare),
					Subject:   fooVar,
					Predicate: &plandef.OID{Value: 999},
					Object:    &plandef.OID{Value: 888},
				},
				Variables: plandef.VarSet{fooVar},
			},
		},
	}
	enumerateTestHelper(t, fooVar, plan)
}

// Tests enumerateOp's behavior in the context of the right side of a LoopJoin.
func Test_Enumerate_bound_loopJoin(t *testing.T) {
	fooVar := &plandef.Variable{Name: "foo"}
	plan := &plandef.Plan{
		Operator: &plandef.LoopJoin{
			Variables:   plandef.VarSet{fooVar},
			Specificity: parser.MatchRequired,
		},
		Variables: plandef.VarSet{fooVar},
		Inputs: []*plandef.Plan{
			{
				Operator: &plandef.LookupPO{
					ID:        new(plandef.DontCare),
					Subject:   fooVar,
					Predicate: &plandef.OID{Value: 999},
					Object:    &plandef.OID{Value: 888},
				},
				Variables: plandef.VarSet{fooVar},
			},
			{
				Operator: &plandef.Enumerate{
					Output: &plandef.Binding{Var: fooVar},
					Values: []plandef.FixedTerm{
						&plandef.OID{Value: 10},
						&plandef.OID{Value: 20},
						&plandef.OID{Value: 30},
					},
				},
				Variables: plandef.VarSet{fooVar},
			},
		},
	}
	enumerateTestHelper(t, fooVar, plan)
}

// Used in Test_Enumerate_unbound_hashJoin and Test_Enumerate_bound_loopJoin.
func enumerateTestHelper(t *testing.T, outColumn *plandef.Variable, plan *plandef.Plan) {
	assert := assert.New(t)
	t.Helper()
	lookups, assertDone := mocklookups.New(t,
		mocklookups.OK(
			&rpc.LookupPORequest{
				Index: 10,
				Lookups: []rpc.LookupPORequest_Item{{
					Predicate: 999,
					Object:    rpc.AKID(888),
				}},
			},
			rpc.LookupChunk{
				Facts: []rpc.LookupChunk_Fact{
					{
						Lookup: 0,
						Fact:   rpc.Fact{Index: 3, Id: 3001, Subject: 20, Predicate: 999, Object: rpc.AKID(888)},
					},
					{
						Lookup: 0,
						Fact:   rpc.Fact{Index: 10, Id: 10001, Subject: 80, Predicate: 999, Object: rpc.AKID(888)},
					},
				},
			},
			rpc.LookupChunk{
				Facts: []rpc.LookupChunk_Fact{
					{
						Lookup: 0,
						Fact:   rpc.Fact{Index: 8, Id: 8001, Subject: 30, Predicate: 999, Object: rpc.AKID(888)},
					},
				},
			}))
	events := captureEvents{clock: clocks.Wall}
	op := buildOperator(&events, 10, cache.New(), lookups, plan)
	resultCh := make(chan ResultChunk, 1+chanPadding)
	err := op.run(context.Background(), new(defaultBinder), resultCh)
	assert.NoError(err)
	assert.Len(resultCh, 1)
	chunk := <-resultCh
	assert.True(isClosed(resultCh))
	if assert.Equal(2, chunk.NumRows()) {
		factSet := chunk.Facts[0]
		assert.Equal("[{idx:3 id:3001 s:20 p:999 o:#888}]", fmt.Sprint(factSet.Facts))
		assert.Equal("#20", chunk.Values[0].String())

		factSet = chunk.Facts[1]
		assert.Equal("[{idx:8 id:8001 s:30 p:999 o:#888}]", fmt.Sprint(factSet.Facts))
		assert.Equal("#30", chunk.Values[1].String())
		assert.Equal(Columns{outColumn}, chunk.Columns)
	}
	assertDone()
}
