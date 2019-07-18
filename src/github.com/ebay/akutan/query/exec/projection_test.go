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
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/ebay/akutan/msg/facts"
	"github.com/ebay/akutan/query/parser"
	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/rpc"
	"github.com/stretchr/testify/assert"
)

func Test_ProjectionOp(t *testing.T) {
	type test struct {
		name       string
		input      []ResultChunk
		inputCols  Columns
		inputError error
		op         plandef.Projection
		expResults ResultChunk
		expError   error
	}
	tests := []test{{
		name: "select_2var",
		input: []ResultChunk{{
			Columns: Columns{varS, varP, varO},
			Values: []Value{
				kidVal(10), kidVal(11), kidVal(12),
				kidVal(20), kidVal(21), kidVal(22),
			},
			offsets: []uint32{0, 0},
			Facts:   []FactSet{fs(1), fs(2)},
			FinalStatistics: FinalStatistics{
				TotalResultSize: 10,
			},
		}},
		inputCols: Columns{varS, varP, varO},
		op: plandef.Projection{
			Select: []plandef.ExprBinding{
				{Expr: varP, Out: varP}, {Expr: varO, Out: varO},
			},
			Variables: plandef.VarSet{varO, varP},
		},
		expResults: ResultChunk{
			Columns: Columns{varP, varO},
			Values: []Value{
				kidVal(11), kidVal(12),
				kidVal(21), kidVal(22),
			},
			offsets: []uint32{0, 0},
			Facts:   []FactSet{{}, {}},
			FinalStatistics: FinalStatistics{
				TotalResultSize: 10,
			},
		},
	}, {
		name: "agg_*",
		input: []ResultChunk{{
			Columns: Columns{varS, varP},
			Values: []Value{
				kidVal(10), kidVal(11),
				kidVal(20), kidVal(21),
			},
			offsets: []uint32{0, 0},
			Facts:   []FactSet{fs(1), fs(2)},
		}},
		inputCols: Columns{varS, varP},
		op: plandef.Projection{
			Select: []plandef.ExprBinding{{
				Expr: &plandef.AggregateExpr{
					Func: parser.AggCount,
					Of:   plandef.WildcardExpr{},
				},
				Out: varO,
			}},
			Variables: plandef.VarSet{varO},
		},
		expResults: ResultChunk{
			Columns: Columns{varO},
			Values:  []Value{{KGObject: rpc.AInt64(2, 0)}},
			offsets: []uint32{0},
			Facts:   []FactSet{{}},
		},
	}, {
		name: "agg_vars",
		input: []ResultChunk{{
			Columns: Columns{varS, varP},
			Values: []Value{
				kidVal(10), kidVal(11),
				kidVal(20), {},
			},
			offsets: []uint32{0, 0},
			Facts:   []FactSet{fs(1), fs(2)},
		}},
		inputCols: Columns{varS, varP},
		op: plandef.Projection{
			Select: []plandef.ExprBinding{
				{Expr: &plandef.AggregateExpr{Func: parser.AggCount, Of: varS}, Out: varO},
				{Expr: &plandef.AggregateExpr{Func: parser.AggCount, Of: varP}, Out: varI},
			},
			Variables: plandef.VarSet{varI, varO},
		},
		expResults: ResultChunk{
			Columns: Columns{varO, varI},
			Values: []Value{
				{KGObject: rpc.AInt64(2, 0)}, {KGObject: rpc.AInt64(1, 0)},
			},
			offsets: []uint32{0},
			Facts:   []FactSet{{}},
		},
	}, {
		name: "agg_no_input_rows",
		input: []ResultChunk{{
			Columns: Columns{varS, varP},
		}},
		inputCols: Columns{varS, varP},
		op: plandef.Projection{
			Select: []plandef.ExprBinding{
				{Expr: &plandef.AggregateExpr{Func: parser.AggCount, Of: varP}, Out: varO},
			},
			Variables: plandef.VarSet{varO},
		},
		expResults: ResultChunk{
			Columns: Columns{varO},
			Values:  []Value{{KGObject: rpc.AInt64(0, 0)}},
			offsets: []uint32{0},
			Facts:   []FactSet{{}},
		},
	}, {
		name:       "input_error",
		inputCols:  Columns{varP, varS},
		inputError: fmt.Errorf("unable to lookup FOO"),
		op: plandef.Projection{
			Select:    []plandef.ExprBinding{{Expr: varS, Out: varS}},
			Variables: plandef.VarSet{varS},
		},
		expError: fmt.Errorf("unable to lookup FOO"),
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := makeInputOpWithChunks(test.input, test.inputError, test.inputCols)
			projectionOp := newProjection(&test.op, []queryOperator{input})
			assert.Equal(t, &test.op, projectionOp.operator())
			res, err := executeOp(t, projectionOp, new(defaultBinder))
			assert.Equal(t, test.expError, err)
			if test.expError == nil {
				assertResultChunkEqual(t, test.expResults, res)
			}
		})
	}
}

func Test_Projection_ChecksInputCount(t *testing.T) {
	assert.PanicsWithValue(t, "projection operation with unexpected number of inputs: 2", func() {
		newProjection(&plandef.Projection{}, []queryOperator{&mockQueryOp{}, &mockQueryOp{}})
	})
}

func Test_Projection_NoBindingAllowed(t *testing.T) {
	def := &plandef.Projection{
		Select:    []plandef.ExprBinding{{Expr: varS, Out: varS}},
		Variables: plandef.VarSet{varS},
	}
	op := newProjection(def, []queryOperator{&mockQueryOp{}})
	res := resultsCollector{}
	assert.PanicsWithValue(t, "projection operator Project ?s unexpectedly bulk bound to 2 rows", func() {
		op.execute(context.Background(), newTestBinder(varS, 10, 20), &res)
	})
}

func Test_Ask(t *testing.T) {
	type test struct {
		name       string
		input      []ResultChunk
		inputCols  Columns
		inputError error
		op         plandef.Ask
		expColumns Columns
		expResult  Value
		expError   error
	}
	tests := []test{{
		name: "some input results",
		input: []ResultChunk{{
			Columns: Columns{varS, varP},
			Values: []Value{
				kidVal(10), kidVal(11),
				kidVal(12), kidVal(13),
			},
			offsets: []uint32{0, 0},
			Facts:   []FactSet{fs(1), fs(2)},
		}},
		inputCols:  Columns{varS, varP},
		op:         plandef.Ask{Out: varO},
		expColumns: Columns{varO},
		expResult:  Value{KGObject: rpc.ABool(true, 0)},
	}, {
		name:       "no input results",
		input:      []ResultChunk{},
		inputCols:  Columns{varS, varP},
		op:         plandef.Ask{Out: varO},
		expColumns: Columns{varO},
		expResult:  Value{KGObject: rpc.ABool(false, 0)},
	}, {
		name:       "input error",
		input:      []ResultChunk{},
		inputCols:  Columns{varS, varP},
		inputError: errors.New("lookup failed"),
		op:         plandef.Ask{Out: varO},
		expColumns: Columns{varO},
		expError:   errors.New("lookup failed"),
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := makeInputOpWithChunks(test.input, test.inputError, test.inputCols)
			askOp := newAsk(&test.op, []queryOperator{input})
			assert.Equal(t, &test.op, askOp.operator())
			exp := ResultChunk{
				Columns: test.expColumns,
				Values:  []Value{test.expResult},
				offsets: []uint32{0},
				Facts:   []FactSet{{}},
			}
			res, err := executeOp(t, askOp, new(defaultBinder))
			assert.Equal(t, test.expError, err)
			if test.expError == nil {
				assertResultChunkEqual(t, exp, res)
			}
		})
	}
}

func Test_AskCancelsInputOp(t *testing.T) {
	count := 0
	cancelled := false
	inputOp := &mockQueryOp{
		cols: Columns{varS},
		exec: func(ctx context.Context, _ valueBinder, resCh chan<- ResultChunk) error {
			defer close(resCh)
			out := ResultChunk{
				Columns: Columns{varS},
				Values:  []Value{kidVal(42)},
				Facts:   []FactSet{fs(1)},
				offsets: []uint32{0},
			}
			for count < 10 {
				select {
				case resCh <- out:
					count++
				case <-ctx.Done():
					cancelled = true
					return ctx.Err()
				}
			}
			return nil
		},
	}
	res := resultsCollector{t: t}
	askOp := newAsk(&plandef.Ask{Out: inputOp.columns()[0]}, []queryOperator{inputOp})
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err := askOp.execute(ctx, new(defaultBinder), &res)
	assert.NoError(t, err, "AskOp performed its own cancellation, this shouldn't be reported as the askOp result error")
	assert.Equal(t, []Value{{KGObject: rpc.ABool(true, 0)}}, res.asChunk().Values)
	assert.Equal(t, 1, count, "Ask should only allow at most 1 resultChunk to be generated")
	assert.True(t, cancelled, "Ask should have cancelled the inputOp")
	assert.Nil(t, ctx.Err(), "Timed out waiting for AskOp to cancel inputOp")
}

// If the context is cancelled by the AskOp caller before any results have been
// generated then the returned error should be the context error.
func Test_AskCancelledErr(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	inputOp := &mockQueryOp{
		cols: Columns{varS},
		exec: func(ctx context.Context, _ valueBinder, resCh chan<- ResultChunk) error {
			// once ask op is executing the input op, we cause the root context to be cancelled
			cancel()
			close(resCh)
			return ctx.Err()
		},
	}
	res := resultsCollector{t: t}
	askOp := newAsk(&plandef.Ask{Out: inputOp.columns()[0]}, []queryOperator{inputOp})
	err := askOp.execute(ctx, new(defaultBinder), &res)
	assert.Equal(t, context.Canceled, err, "AskOp was cancelled by caller, AskOp should report cancelled as the error")
}

// If the input op to Ask generates some results, and then returns an error
// that's not the cancellation error, that should be reported as the error out
// of askOp.
func Test_AskInputErrorAfterResult(t *testing.T) {
	inputOp := &mockQueryOp{
		cols: Columns{varS},
		exec: func(_ context.Context, _ valueBinder, resCh chan<- ResultChunk) error {
			resCh <- ResultChunk{
				Columns: Columns{varS},
				Values:  []Value{kidVal(10)},
				offsets: []uint32{0},
				Facts:   []FactSet{fs(1)},
			}
			close(resCh)
			return errors.New("rpc failed")
		},
	}
	res := resultsCollector{t: t}
	askOp := newAsk(&plandef.Ask{Out: inputOp.columns()[0]}, []queryOperator{inputOp})
	err := askOp.execute(ctx, new(defaultBinder), &res)
	assert.EqualError(t, err, "rpc failed")
}

func Test_Ask_ChecksInputCount(t *testing.T) {
	assert.PanicsWithValue(t, "ask operation with unexpected number of inputs: 2", func() {
		newAsk(&plandef.Ask{}, []queryOperator{&mockQueryOp{}, &mockQueryOp{}})
	})
}

func Test_ExternalIDs(t *testing.T) {
	type test struct {
		name              string
		op                plandef.ExternalIDs
		input             []ResultChunk
		subjectIDs        map[uint64]string
		expSubjectLookups [][]uint64
		expOutput         ResultChunk
	}
	tests := []test{{
		name: "repeated units",
		input: []ResultChunk{{
			Columns: Columns{varS, varP},
			Values: []Value{
				{KGObject: rpc.AKID(42)}, {KGObject: rpc.AInt64(10, 100)},
				{KGObject: rpc.AKID(43)}, {KGObject: rpc.AInt64(11, 100)},
			},
			offsets: []uint32{1, 2},
			Facts:   []FactSet{fs(1), fs(2)},
			FinalStatistics: FinalStatistics{
				TotalResultSize: 42,
			},
		}},
		subjectIDs: map[uint64]string{
			42:  "car",
			43:  "truck",
			100: "inch",
		},
		expSubjectLookups: [][]uint64{{42, 43, 100}},
		expOutput: ResultChunk{
			Columns: Columns{varS, varP},
			Values: []Value{
				{KGObject: rpc.AKID(42), ExtID: "<car>"}, {KGObject: rpc.AInt64(10, 100), UnitExtID: "<inch>"},
				{KGObject: rpc.AKID(43), ExtID: "<truck>"}, {KGObject: rpc.AInt64(11, 100), UnitExtID: "<inch>"},
			},
			offsets: []uint32{1, 2},
			Facts:   []FactSet{fs(1), fs(2)},
			FinalStatistics: FinalStatistics{
				TotalResultSize: 42,
			},
		},
	}, {
		name: "multiple chunks",
		input: []ResultChunk{{
			Columns: Columns{varS, varP},
			Values: []Value{
				{KGObject: rpc.AKID(42)}, {KGObject: rpc.AString("A", 101)},
			},
			offsets: []uint32{0},
			Facts:   []FactSet{fs(1)},
		}, {
			Columns: Columns{varS, varP},
			Values: []Value{
				{KGObject: rpc.AKID(43)}, {KGObject: rpc.AString("B", 101)},
			},
			offsets: []uint32{1},
			Facts:   []FactSet{fs(2)},
		}},
		subjectIDs: map[uint64]string{
			42:  "car",
			43:  "truck",
			101: "en",
		},
		expSubjectLookups: [][]uint64{{42, 101}, {43}},
		expOutput: ResultChunk{
			Columns: Columns{varS, varP},
			Values: []Value{
				{KGObject: rpc.AKID(42), ExtID: "<car>"}, {KGObject: rpc.AString("A", 101), LangExtID: "en"},
				{KGObject: rpc.AKID(43), ExtID: "<truck>"}, {KGObject: rpc.AString("B", 101), LangExtID: "en"},
			},
			offsets: []uint32{0, 1},
			Facts:   []FactSet{fs(1), fs(2)},
		},
	}, {
		name: "2nd chunk needs no lookups",
		input: []ResultChunk{{
			Columns: Columns{varS, varP},
			Values: []Value{
				{KGObject: rpc.AKID(42)}, {KGObject: rpc.AString("A", 101)},
			},
			offsets: []uint32{0},
			Facts:   []FactSet{fs(1)},
		}, {
			Columns: Columns{varS, varP},
			Values: []Value{
				{KGObject: rpc.AKID(42)}, {KGObject: rpc.AString("B", 101)},
			},
			offsets: []uint32{1},
			Facts:   []FactSet{fs(2)},
		}},
		subjectIDs: map[uint64]string{
			42:  "car",
			43:  "truck",
			101: "en",
		},
		expSubjectLookups: [][]uint64{{42, 101}},
		expOutput: ResultChunk{
			Columns: Columns{varS, varP},
			Values: []Value{
				{KGObject: rpc.AKID(42), ExtID: "<car>"}, {KGObject: rpc.AString("A", 101), LangExtID: "en"},
				{KGObject: rpc.AKID(42), ExtID: "<car>"}, {KGObject: rpc.AString("B", 101), LangExtID: "en"},
			},
			offsets: []uint32{0, 1},
			Facts:   []FactSet{fs(1), fs(2)},
		},
	}, {
		name: "chunk needs no lookups",
		input: []ResultChunk{{
			Columns: Columns{varS, varP},
			Values: []Value{
				{KGObject: rpc.AInt64(42, 0)}, {KGObject: rpc.AString("A", 0)},
				{KGObject: rpc.AFloat64(42.0, 0)}, {KGObject: rpc.AString("B", 0)},
			},
			offsets: []uint32{1, 2},
			Facts:   []FactSet{fs(1), fs(2)},
		}},
		subjectIDs:        nil,
		expSubjectLookups: nil,
		expOutput: ResultChunk{
			Columns: Columns{varS, varP},
			Values: []Value{
				{KGObject: rpc.AInt64(42, 0)}, {KGObject: rpc.AString("A", 0)},
				{KGObject: rpc.AFloat64(42.0, 0)}, {KGObject: rpc.AString("B", 0)},
			},
			offsets: []uint32{1, 2},
			Facts:   []FactSet{fs(1), fs(2)},
		},
	}, {
		name: "missing ExtID fact",
		input: []ResultChunk{{
			Columns: Columns{varS},
			Values: []Value{
				{KGObject: rpc.AKID(42)},
				{KGObject: rpc.AKID(43)},
				{KGObject: rpc.AKID(44)},
			},
			offsets: []uint32{1, 2, 3},
			Facts:   []FactSet{fs(1), fs(2), fs(3)},
			FinalStatistics: FinalStatistics{
				TotalResultSize: 42,
			},
		}},
		subjectIDs: map[uint64]string{
			42: "car",
			// 43 is missing
			44: "truck",
		},
		expSubjectLookups: [][]uint64{{42, 43, 44}},
		expOutput: ResultChunk{
			Columns: Columns{varS},
			Values: []Value{
				{KGObject: rpc.AKID(42), ExtID: "<car>"},
				{KGObject: rpc.AKID(43)},
				{KGObject: rpc.AKID(44), ExtID: "<truck>"},
			},
			offsets: []uint32{1, 2, 3},
			Facts:   []FactSet{fs(1), fs(2), fs(3)},
			FinalStatistics: FinalStatistics{
				TotalResultSize: 42,
			},
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inputOp := makeInputOpWithChunks(test.input, nil, test.input[0].Columns)
			lookups := extIdsMockLookup{t: t, ids: test.subjectIDs}
			execOp := newExternalIDs(1, &lookups, &test.op, []queryOperator{inputOp})
			res, err := executeOp(t, execOp, new(defaultBinder))
			assert.NoError(t, err)
			assertResultChunkEqual(t, test.expOutput, res)
			assert.Equal(t, test.expSubjectLookups, lookups.requestedSubjects)
		})
	}
}

func Test_ExternalIDResolver(t *testing.T) {
	lookup := extIdsMockLookup{
		t: t,
		ids: map[uint64]string{
			42: "bob",
			43: "alice",
			44: "eve",
		},
	}
	r := newExternalIDResolver(1)
	var bob, alice, eve mockCallback
	r.resolveID(42, bob.cb)
	r.resolveID(42, bob.cb)
	assert.Equal(t, 0, bob.count)
	assert.NoError(t, r.fetchPending(context.Background(), &lookup))
	assert.Equal(t, mockCallback{extID: "bob", count: 2}, bob)

	r.resolveID(43, alice.cb)
	assert.Equal(t, 0, alice.count)
	r.resolveID(42, bob.cb)
	assert.Equal(t, 3, bob.count, "42 has already been resolved to 'bob' should of gotten callback straight away")
	r.resolveID(44, eve.cb)
	assert.NoError(t, r.fetchPending(context.Background(), &lookup))
	assert.Equal(t, mockCallback{extID: "eve", count: 1}, eve)
	assert.Equal(t, mockCallback{extID: "alice", count: 1}, alice)

	if assert.Equal(t, [][]uint64{{42}, {43, 44}}, lookup.requestedSubjects, "Unexpected subjectIDs in LookupSP RPC") {
		assert.NoError(t, r.fetchPending(context.Background(), &lookup))
		assert.Equal(t, [][]uint64{{42}, {43, 44}}, lookup.requestedSubjects, "Should of been no more LookupSP RPCs made")
	}
}

func Test_EmptyResultOp_ChildResult(t *testing.T) {
	c := ResultChunk{
		Columns: Columns{varS, varP},
		Values:  []Value{kidVal(1), kidVal(2)},
		offsets: []uint32{0},
		Facts:   []FactSet{fs(1)},
	}
	input := makeInputOpWithChunks([]ResultChunk{c}, nil, c.Columns)
	emptyOp := &emptyResultOp{input}
	resCh := make(chan ResultChunk, 4)
	err := emptyOp.run(context.Background(), new(defaultBinder), resCh)
	assert.NoError(t, err)
	if assert.Len(t, resCh, 1) {
		res := <-resCh
		assert.Equal(t, c, res)
		assert.True(t, isClosed(resCh))
	}
}

func Test_EmptyResultOp_ChildNoResults(t *testing.T) {
	input := makeInputOpWithChunks(nil, nil, Columns{varS, varP})
	emptyOp := emptyResultOp{input}
	resCh := make(chan ResultChunk, 4)
	err := emptyOp.run(context.Background(), new(defaultBinder), resCh)
	assert.NoError(t, err)
	if assert.Len(t, resCh, 1) {
		res := <-resCh
		assert.Equal(t, Columns{varS, varP}, res.Columns)
		assert.Empty(t, res.Facts)
		assert.Empty(t, res.Values)
		assert.Zero(t, res.NumRows())
	}
}

type mockCallback struct {
	extID string
	count int
}

func (m *mockCallback) cb(id string) {
	m.extID = id
	m.count++
}

type extIdsMockLookup struct {
	t                 *testing.T
	requestedSubjects [][]uint64
	ids               map[uint64]string
}

func (m *extIdsMockLookup) LookupSP(_ context.Context, req *rpc.LookupSPRequest, resCh chan *rpc.LookupChunk) error {
	result := rpc.LookupChunk{}
	reqSubjects := make(map[uint64]bool, len(req.Lookups))
	// process the lookups, build the results, and verify that there aren't
	// duplicate subjects in the request.
	for lookupIdx, l := range req.Lookups {
		assert.Equal(m.t, facts.HasExternalID, l.Predicate)
		if reqSubjects[l.Subject] {
			m.t.Errorf("Subject ID %d was requested more than once", l.Subject)
		}
		reqSubjects[l.Subject] = true
		if externalID, exists := m.ids[l.Subject]; exists {
			result.Facts = append(result.Facts,
				rpc.LookupChunk_Fact{
					Lookup: uint32(lookupIdx),
					Fact:   rpc.Fact{Subject: l.Subject, Predicate: l.Predicate, Object: rpc.AString(externalID, 0)},
				})
		}
	}
	resCh <- &result
	close(resCh)

	// collect up the sorted list of subjects that were request
	// for later verification
	subjectIDs := make([]uint64, 0, len(reqSubjects))
	for s := range reqSubjects {
		subjectIDs = append(subjectIDs, s)
	}
	sort.Slice(subjectIDs, func(i, j int) bool {
		return subjectIDs[i] < subjectIDs[j]
	})
	m.requestedSubjects = append(m.requestedSubjects, subjectIDs)
	return nil
}
