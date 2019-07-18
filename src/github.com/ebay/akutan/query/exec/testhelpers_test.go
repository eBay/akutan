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
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/clocks"
	"github.com/ebay/akutan/util/parallel"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	varI = &plandef.Variable{Name: "id"}
	varS = &plandef.Variable{Name: "s"}
	varP = &plandef.Variable{Name: "p"}
	varO = &plandef.Variable{Name: "o"}
	ctx  = context.Background()
)

func Test_FactsEqual(t *testing.T) {
	a := rpc.Fact{Index: 1, Id: 2, Subject: 3, Predicate: 4, Object: rpc.AKID(5)}
	a1 := rpc.Fact{Index: 1, Id: 2, Subject: 3, Predicate: 4, Object: rpc.AKID(5)}
	b := rpc.Fact{Index: 1, Id: 2, Subject: 3, Predicate: 4, Object: rpc.AKID(7)}
	assert.True(t, a.Equal(a))
	assert.True(t, a.Equal(a1))
	assert.True(t, a1.Equal(a))
	assert.False(t, a.Equal(b))
}

func Test_FactSetsAreEqual(t *testing.T) {
	fs1 := fs(1, 2, 3)
	fs2 := fs(3, 2, 4)
	fs3 := fs(3, 2, 1)
	assert.True(t, factSetsAreEqual(&fs1, &fs1))
	assert.True(t, factSetsAreEqual(&fs2, &fs2))
	assert.True(t, factSetsAreEqual(&fs1, &fs3))
	assert.False(t, factSetsAreEqual(&fs1, &fs2))
}

func Test_AssertFactSetsEqual(t *testing.T) {
	fs1 := []FactSet{fs(5, 6, 10), fs(1)}
	fs2 := []FactSet{fs(1), fs(6, 10, 5)}
	assertFactSetsAreEqual(t, fs1, fs2)
}

// executeOp executes the supplied operator, collecting the results into a
// single ResultChunk and returns them. It returns an error if the operator
// returns an error.
func executeOp(t *testing.T, op operator, binder valueBinder) (ResultChunk, error) {
	queryOp := &decoratedOp{events: ignoreEvents{}, op: op}
	// Default the columns from the queryOp, all the received chunks should have
	// the same columns. resultsCollector verifies that.
	actual := resultsCollector{
		t:       t,
		columns: queryOp.columns(),
	}
	resCh := make(chan ResultChunk, 4)
	wait := parallel.Go(func() {
		// Accumulate up all the results
		actual.accumulateFrom(resCh)
	})
	err := queryOp.run(context.Background(), binder, resCh)
	wait()
	return actual.asChunk(), err
}

func assertResultChunkEqual(t *testing.T, exp ResultChunk, act ResultChunk) {
	// don't treat empty slice and nil as different
	if len(exp.Columns) > 0 || len(act.Columns) > 0 {
		assert.Equal(t, exp.Columns, act.Columns, "columns don't match")
	}
	if len(exp.Values) > 0 || len(act.Values) > 0 {
		assert.Equal(t, exp.Values, act.Values, "row values don't match")
	}
	if len(exp.offsets) > 0 || len(act.offsets) > 0 {
		assert.Equal(t, exp.offsets, act.offsets, "offsets don't match")
	}
	assertFactSetsAreEqual(t, exp.Facts, act.Facts)
	assert.Equal(t, exp.FinalStatistics, act.FinalStatistics, "final stats don't match")
}

// sorted returns a copy of this resultChunk with the facts, offsets & rows
// sorted by the row column values.
func (r *ResultChunk) sorted(t *testing.T) ResultChunk {
	c := ResultChunk{
		Columns:         r.Columns,
		Facts:           r.Facts,
		offsets:         r.offsets,
		Values:          r.Values,
		FinalStatistics: r.FinalStatistics,
	}
	// this copies r into c, preserving the distinction between nil and empty
	// slice. This distinction is generally not important, but is for assert.Equal
	// which is used below to try and spot when ResultChunk is updated without
	// updating this func.
	if len(r.Columns) > 0 {
		c.Columns = append(Columns(nil), r.Columns...)
	}
	if len(r.Facts) > 0 {
		c.Facts = append([]FactSet(nil), r.Facts...)
	}
	if len(r.offsets) > 0 {
		c.offsets = append([]uint32(nil), r.offsets...)
	}
	if len(r.Values) > 0 {
		c.Values = append([]Value(nil), r.Values...)
	}
	assert.Equal(t, r, &c, "ResultChunk likely updated with new fields that need adding to sorted()")
	sorter := resChunkSort{c: &c}
	sort.Sort(&sorter)
	return c
}

// resChunkSort will sort the rows in the result chunk, by col[0], col[1], etc.
type resChunkSort struct {
	c *ResultChunk
}

func (s *resChunkSort) Len() int {
	return len(s.c.offsets)
}

func (s *resChunkSort) Less(i, j int) bool {
	a := s.c.Row(i)
	b := s.c.Row(j)
	for col := range a {
		if a[col].KGObject.Less(b[col].KGObject) {
			return true
		}
		if b[col].KGObject.Less(a[col].KGObject) {
			return false
		}
	}
	return false
}

func (s *resChunkSort) Swap(i, j int) {
	s.c.Facts[i], s.c.Facts[j] = s.c.Facts[j], s.c.Facts[i]
	s.c.offsets[i], s.c.offsets[j] = s.c.offsets[j], s.c.offsets[i]
	a := i * len(s.c.Columns)
	b := j * len(s.c.Columns)
	for col := range s.c.Columns {
		s.c.Values[a+col], s.c.Values[b+col] = s.c.Values[b+col], s.c.Values[a+col]
	}
}

// assertFactSetsAreEqual asserts that the 2 FactSets are logically equivalent.
// It allows for different orders in the slices of facts.
func assertFactSetsAreEqual(t *testing.T, exp []FactSet, act []FactSet) {
	t.Helper()
	assert.Equal(t, len(exp), len(act), "Different number of FactSets, expecting %v\nreceived %v\n", exp, act)
	actConsumed := make([]bool, len(act))

	for i := range exp {
		found := false
		for j := range act {
			if !actConsumed[j] && factSetsAreEqual(&exp[i], &act[j]) {
				actConsumed[j] = true
				found = true
				break
			}
		}
		assert.True(t, found, "Expected FactSet %v not found in actual results\n%s", exp[i], prettyPrintFactSets(act))
	}
}

func prettyPrintFactSets(fs []FactSet) string {
	b := strings.Builder{}
	b.WriteString("[\n")
	for _, f := range fs {
		b.WriteByte('\t')
		b.WriteString(f.String())
		b.WriteByte('\n')
	}
	b.WriteString("]\n")
	return b.String()
}

// factSetsAreEqual returns true if the 2 Factsets are logically equivalent. It
// allows for different orders in the slices of facts.
func factSetsAreEqual(a, b *FactSet) bool {
	if len(a.Facts) != len(b.Facts) {
		return false
	}
	hasFact := func(facts []rpc.Fact, f rpc.Fact) bool {
		for _, fact := range facts {
			if fact.Equal(f) {
				return true
			}
		}
		return false
	}
	for _, af := range a.Facts {
		if !hasFact(b.Facts, af) {
			return false
		}
	}
	return true
}

// isClosed returns true if ch has been closed or false if it's still open.
// In the event that the channel is still open and a value is ready, this will
// read and discard the value.
func isClosed(ch <-chan ResultChunk) bool {
	select {
	case v, open := <-ch:
		if open {
			logrus.WithFields(logrus.Fields{
				"value": v,
			}).Warn("isClosed discarded value")
			return false
		}
		return true
	default:
		return false
	}
}

func makeInputOp(facts [][]rpc.Fact, vars variableSet) queryOperator {
	return &mockQueryOp{
		cols: vars.columns(),
		exec: func(ctx context.Context, binder valueBinder, resCh chan<- ResultChunk) error {
			rb := newChunkBuilder(resCh, vars.columns())
			for offset := uint32(0); offset < binder.len(); offset++ {
				for _, f := range facts[offset] {
					fs := FactSet{Facts: []rpc.Fact{f}}
					rb.add(ctx, offset, fs, vars.resultsOf(&f))
				}
			}
			rb.flush(ctx)
			close(resCh)
			return nil
		},
	}
}

// makeInputOpWithChunks returns a new queryOperator that sends the supplied
// list of ResultChunks when executed, and then return 'err'
func makeInputOpWithChunks(chunks []ResultChunk, err error, cols Columns) queryOperator {
	return &mockQueryOp{
		cols: cols,
		exec: func(ctx context.Context, binder valueBinder, resCh chan<- ResultChunk) error {
			for _, c := range chunks {
				resCh <- c
			}
			close(resCh)
			return err
		},
	}
}

// ignoreEvents should impl the Events interface
var _ Events = ignoreEvents{}

// captureEvents implements Events and captures the calls, it assumes there's
// only one Op in the test
type captureEvents struct {
	lock   sync.Mutex
	clock  clocks.Source
	events []OpCompletedEvent
}

func (c *captureEvents) OpCompleted(event OpCompletedEvent) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.events = append(c.events, event)
}

func (c *captureEvents) Clock() clocks.Source {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.clock
}

// resultsCollector implements the results interface and appends all received
// results to rows
type resultsCollector struct {
	t               *testing.T
	columns         Columns
	facts           []FactSet
	offsets         []uint32
	rows            [][]Value
	finalStatistics FinalStatistics
}

func (c *resultsCollector) asChunk() ResultChunk {
	res := ResultChunk{
		Columns:         c.columns,
		Facts:           c.facts,
		offsets:         c.offsets,
		FinalStatistics: c.finalStatistics,
		Values:          make([]Value, 0, len(c.columns)*len(c.rows)),
	}
	for _, r := range c.rows {
		res.Values = append(res.Values, r...)
	}
	return res
}

func (c *resultsCollector) accumulateFrom(ch chan ResultChunk) {
	for r := range ch {
		if len(c.columns) == 0 {
			// copy the columns into our result chunk from the first result
			// we see.
			c.columns = r.Columns
		} else {
			// all chunks should have the same columns.
			require.Equal(c.t, c.columns, r.Columns)
		}
		assert.True(c.t, c.finalStatistics == FinalStatistics{} ||
			c.finalStatistics == r.FinalStatistics,
			"FinalStatistics changed after being originally set")
		c.finalStatistics = r.FinalStatistics
		c.facts = append(c.facts, r.Facts...)
		c.offsets = append(c.offsets, r.offsets...)
		for i := range r.offsets {
			c.rows = append(c.rows, r.Row(i))
		}
	}
}

func (c *resultsCollector) add(ctx context.Context, offset uint32, f FactSet, rowValues []Value) {
	c.facts = append(c.facts, f)
	c.offsets = append(c.offsets, offset)
	c.rows = append(c.rows, rowValues)
}

func (c *resultsCollector) setFinalStatistics(stats FinalStatistics) {
	c.finalStatistics = stats
}

// mockOp provides a mock implementation of the operator interface.
type mockOp struct {
	def  plandef.Operator
	exec func(context.Context, valueBinder, results) error
	cols Columns
}

func (m *mockOp) columns() Columns {
	return m.cols
}

func (m *mockOp) operator() plandef.Operator {
	return m.def
}

func (m *mockOp) execute(ctx context.Context, binder valueBinder, res results) error {
	return m.exec(ctx, binder, res)
}

// mockQueryOp provides a mock implementation of the queryOperator interface.
type mockQueryOp struct {
	exec func(context.Context, valueBinder, chan<- ResultChunk) error
	cols Columns
}

func (m *mockQueryOp) run(ctx context.Context, binder valueBinder, res chan<- ResultChunk) error {
	return m.exec(ctx, binder, res)
}

func (m *mockQueryOp) columns() Columns {
	return m.cols
}

// kidVal returns a Value instance for the supplied KID value.
func kidVal(kid uint64) Value {
	return Value{KGObject: rpc.AKID(kid)}
}

func factIDValuesOf(facts []rpc.Fact) []Value {
	res := make([]Value, len(facts))
	for i, f := range facts {
		res[i] = Value{KGObject: rpc.AKID(f.Id)}
	}
	return res
}

func subjectValuesOf(facts []rpc.Fact) []Value {
	res := make([]Value, len(facts))
	for i, f := range facts {
		res[i] = Value{KGObject: rpc.AKID(f.Subject)}
	}
	return res
}

func predicateValuesOf(facts []rpc.Fact) []Value {
	res := make([]Value, len(facts))
	for i, f := range facts {
		res[i] = Value{KGObject: rpc.AKID(f.Predicate)}
	}
	return res
}

func objectValuesOf(facts []rpc.Fact) []Value {
	res := make([]Value, len(facts))
	for i, f := range facts {
		res[i] = Value{KGObject: f.Object}
	}
	return res
}

// zipValues returns a table of values from the provided list of column values.
// It assumes all the columns provided have the same number of items.
//
// i.e., given a := [a,b,c] and b:= [1,2,3], zipValue(a,b) returns a table from
// the 2 columns:
//  [ a, 1,
//    b, 2,
//    c, 3 ]
func zipValues(cols ...[]Value) []Value {
	if len(cols) == 0 {
		return []Value{}
	}
	out := make([]Value, 0, len(cols)*len(cols[0]))
	for r := range cols[0] {
		for c := range cols {
			out = append(out, cols[c][r])
		}
	}
	return out
}
