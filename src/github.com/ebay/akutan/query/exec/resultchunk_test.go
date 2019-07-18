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
	"strings"
	"testing"
	"time"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ResultChunk(t *testing.T) {
	c := ResultChunk{}
	assert.Zero(t, c.len())
	c = ResultChunk{
		Columns: Columns{varP, varO},
		Values: []Value{
			{KGObject: rpc.AKID(100)}, {KGObject: rpc.AInt64(2, 2)},
			{KGObject: rpc.AKID(101)}, {KGObject: rpc.AInt64(4, 2)},
		},
		offsets: []uint32{0, 0},
		Facts:   []FactSet{fs(0), fs(1)},
	}
	assert.Equal(t, uint32(2), c.len())
	assert.Equal(t, 2, c.NumRows())
	assert.Equal(t, c.Values[0:2], c.Row(0))
	assert.Equal(t, c.Values[2:], c.Row(1))

	bindDef := &plandef.Binding{Var: varP}
	assert.Equal(t, uint64(100), c.bind(0, bindDef).KGObject.ValKID())
	assert.Equal(t, uint64(101), c.bind(1, bindDef).KGObject.ValKID())

	bindDef = &plandef.Binding{Var: varS} // not in ResultChunk
	assert.Nil(t, c.bind(0, bindDef))
	assert.Nil(t, c.bind(1, bindDef))
}

func Test_ResultChunk_IdentityKey(t *testing.T) {
	assert := assert.New(t)
	c1 := ResultChunk{
		Columns: Columns{varS, varP},
		Values: []Value{
			kidVal(123), kidVal(321),
			kidVal(111), kidVal(222),
		},
	}
	c2 := ResultChunk{
		Columns: Columns{varP, varS}, // note different order to c1
		Values: []Value{
			kidVal(123), kidVal(321),
			kidVal(222), kidVal(111),
		},
	}

	c1ColumnsVarS := c1.Columns.MustIndexesOf(Columns{varS})
	c1ColumnsVarPS := c1.Columns.MustIndexesOf(Columns{varP, varS})

	c2ColumnsVarS := c2.Columns.MustIndexesOf(Columns{varS})
	c2ColumnsVarPS := c2.Columns.MustIndexesOf(Columns{varP, varS})

	assert.NotEqual(c1.identityKeysOf(0, c1ColumnsVarS), c1.identityKeysOf(1, c1ColumnsVarS))
	assert.NotEqual(c1.identityKeysOf(0, c1ColumnsVarS), c1.identityKeysOf(0, c1ColumnsVarPS))
	assert.Equal(c1.identityKeysOf(1, c1ColumnsVarPS), c1.identityKeysOf(1, c1ColumnsVarPS))
	assert.Equal(c1.identityKeysOf(1, c1ColumnsVarS), c2.identityKeysOf(1, c2ColumnsVarS))
	assert.Equal(c1.identityKeysOf(1, c1ColumnsVarPS), c2.identityKeysOf(1, c2ColumnsVarPS))

	// Should panic when column indexes are out of bounds for the row.
	assert.Panics(func() {
		c1.identityKeysOf(0, []int{0, 1, 2, 3})
	})
}

func Benchmark_ResultChunk_identityKeysOf(t *testing.B) {
	c1 := ResultChunk{
		Columns: Columns{varS, varP, varO},
		Values: []Value{
			kidVal(100), kidVal(101), {KGObject: rpc.AInt64(1, 2)},
			kidVal(200), kidVal(201), {KGObject: rpc.AInt64(2, 2)},
			kidVal(300), kidVal(301), {KGObject: rpc.AInt64(3, 2)},
		},
	}
	vars := Columns{varS, varP, varO}
	columns := c1.Columns.MustIndexesOf(vars)
	for i := 0; i < t.N; i++ {
		c1.identityKeysOf(0, columns)
	}
}

func Test_ResultChunk_ToTable(t *testing.T) {
	c := ResultChunk{
		Columns: Columns{varS, varO},
		Values: []Value{
			kidVal(10), {KGObject: rpc.AString("Bob", 0)},
			kidVal(12), {KGObject: rpc.AString("Alice", 11), LangExtID: "en"},
			kidVal(14), {KGObject: rpc.AString("Eve", 11), LangExtID: "en"},
		},
	}
	w := strings.Builder{}
	c.ToTable(&w)
	assert.Equal(t, `
Chunk with 2 columns, 3 rows
 s   | o              |
 --- | -------------- |
 #10 | "Bob"          |
 #12 | "Alice"@#11 en |
 #14 | "Eve"@#11 en   |
`, "\n"+w.String())
}

func Test_ResultChunk_ChecksRowLen(t *testing.T) {
	out := Columns{varS, varP, varO}
	b := newChunkBuilder(make(chan ResultChunk, 4), out)
	assert.Panics(t, func() {
		b.add(ctx, 0, FactSet{}, []Value{kidVal(1)})
	})
}

func Test_ResultChunkBuilder_Flush(t *testing.T) {
	resCh := make(chan ResultChunk, 4)
	rb := newChunkBuilder(resCh, Columns{varS})
	rb.flushAtSize = 3
	rb.flush(ctx)
	assert.Zero(t, len(resCh))
	rb.flush(ctx)
	assert.Zero(t, len(resCh))
	rb.add(ctx, 1, fs(1), []Value{kidVal(1)})
	assert.Zero(t, len(resCh))
	rb.flush(ctx)
	require.Equal(t, 1, len(resCh), "Expecting 1 Chunk to have been flushed to the channel")
	exp := ResultChunk{
		Columns: Columns{varS},
		Values:  []Value{kidVal(1)},
		offsets: []uint32{1},
		Facts:   []FactSet{fs(1)},
	}
	assert.Equal(t, exp, <-resCh)
	rb.flush(ctx)
	assert.Zero(t, len(resCh))
}

func Test_ResultChunkBuilder_SetFinalStatistics(t *testing.T) {
	resCh := make(chan ResultChunk, 4)
	rb := newChunkBuilder(resCh, Columns{varS})
	rb.flushAtSize = 2
	// Noop when set a zero value stats.
	rb.setFinalStatistics(FinalStatistics{TotalResultSize: 0})
	rb.flush(ctx)
	assert.Equal(t, 0, len(resCh)) // nothing flushed.
	rb.add(ctx, 1, fs(1), []Value{kidVal(1)})
	stats := FinalStatistics{TotalResultSize: 10}
	rb.setFinalStatistics(stats)
	rb.add(ctx, 1, fs(2), []Value{kidVal(2)})
	rb.flush(ctx)
	assert.Equal(t, 1, len(resCh))
	exp := ResultChunk{
		Columns:         Columns{varS},
		Values:          []Value{kidVal(1), kidVal(2)},
		offsets:         []uint32{1, 1},
		Facts:           []FactSet{fs(1), fs(2)},
		FinalStatistics: FinalStatistics{TotalResultSize: 10},
	}
	assert.Equal(t, exp, <-resCh)
	// Noop when set stats with the same value.
	rb.setFinalStatistics(FinalStatistics{TotalResultSize: 10})
	rb.flush(ctx)
	assert.Equal(t, 0, len(resCh)) // nothing flushed.
	// Should panic when attempt to overwrite the stats with a zero value.
	assert.Panics(t, func() {
		rb.setFinalStatistics(FinalStatistics{})
	})
	// Should panic when attempt to overwrite the stats with a new value.
	assert.Panics(t, func() {
		rb.setFinalStatistics(FinalStatistics{TotalResultSize: 20})
	})

	// Case: set stats when zero records to flush.
	resCh = make(chan ResultChunk, 4)
	rb = newChunkBuilder(resCh, Columns{varS})
	rb.flushAtSize = 2
	stats = FinalStatistics{TotalResultSize: 10}
	rb.setFinalStatistics(stats)
	rb.flush(ctx)
	assert.Equal(t, 1, len(resCh)) // should flush 1 chunk.
	exp = ResultChunk{
		Columns:         Columns{varS},
		FinalStatistics: FinalStatistics{TotalResultSize: 10},
	}
	assert.Equal(t, exp, <-resCh)
}

// Test that when the context passed to add is cancelled, add returns even if it
// was blocked trying to write to resCh.
func Test_ResultChunkBuilder_Ctx(t *testing.T) {
	resCh := make(chan ResultChunk)
	b := newChunkBuilder(resCh, Columns{varS})
	b.flushAtSize = 2
	addCh := make(chan bool)
	testTimeoutCtx, toCancel := context.WithTimeout(context.Background(), time.Second)
	defer toCancel()
	ctx, cancel := context.WithCancel(testTimeoutCtx)
	defer cancel()
	go func() {
		b.add(ctx, 0, FactSet{}, []Value{kidVal(12)})
		addCh <- true
		b.add(ctx, 0, FactSet{}, []Value{kidVal(12)})
		addCh <- true
	}()
	// wait for first add to have completed
	assert.True(t, <-addCh)
	select {
	case <-addCh:
		assert.Fail(t, "resultChunkBuilder.add returned without flushing chunk")
	case <-time.After(time.Millisecond):
	}
	// cancel the context, the call to add that is blocked on writing to resCh should unblock
	cancel()
	select {
	case <-testTimeoutCtx.Done():
		assert.Fail(t, "Timed out waiting for resultChunkBuilder.add to unblock")
	case <-addCh:
		// 2nd call to add returned, so we're all good.
	}
}

func fs(factIndexes ...blog.Index) FactSet {
	f := FactSet{
		Facts: make([]rpc.Fact, len(factIndexes)),
	}
	for i, factIdx := range factIndexes {
		f.Facts[i].Id = factIdx
	}
	return f
}

func Test_Columns_IndexOf(t *testing.T) {
	c := Columns{varS, varP, varO}
	assertIndexOf := func(c Columns, v *plandef.Variable, expIndex int, expExists bool) {
		actualIdx, actualExists := c.IndexOf(v)
		assert.Equal(t, expIndex, actualIdx, "returned index wrong for [%s].IndexOf(%s)", c, v)
		assert.Equal(t, expExists, actualExists, "returned exists wrong for [%s].IndexOf(%s)", c, v)
	}
	assertIndexOf(c, varS, 0, true)
	assertIndexOf(c, varP, 1, true)
	assertIndexOf(c, varO, 2, true)
	assertIndexOf(c, varI, 0, false)
}

func Test_Columns_MustIndexOf(t *testing.T) {
	c := Columns{varP, varO}
	assert.Equal(t, 0, c.MustIndexOf(varP))
	assert.Equal(t, 1, c.MustIndexOf(varO))
	assert.Panics(t, func() {
		c.MustIndexOf(varS)
	})
}

func Test_Columns_IndexesOf(t *testing.T) {
	tests := []struct {
		name       string
		inputVars  []*plandef.Variable
		expIndexes []int
		expError   error
		expPanic   bool
	}{
		{
			name:       "varS",
			inputVars:  []*plandef.Variable{varS},
			expIndexes: []int{0},
		},
		{
			name:       "varP",
			inputVars:  []*plandef.Variable{varP},
			expIndexes: []int{1},
		},
		{
			name:       "varO",
			inputVars:  []*plandef.Variable{varO},
			expIndexes: []int{2},
		},
		{
			name:       "varS_varP",
			inputVars:  []*plandef.Variable{varS, varP},
			expIndexes: []int{0, 1},
		},
		{
			name:       "varP_varS",
			inputVars:  []*plandef.Variable{varP, varS},
			expIndexes: []int{1, 0},
		},
		{
			name:       "varS_varP_varO",
			inputVars:  []*plandef.Variable{varS, varP, varO},
			expIndexes: []int{0, 1, 2},
		},
		{
			name:       "varO_varP_varS",
			inputVars:  []*plandef.Variable{varO, varP, varS},
			expIndexes: []int{2, 1, 0},
		},
		{
			name:      "varI_VarS",
			inputVars: []*plandef.Variable{varI, varS},
			expError:  errors.New("variable ?id is not found in the Columns (it has [?s ?p ?o])"),
			expPanic:  true,
		},
	}

	c := Columns{varS, varP, varO}
	for _, test := range tests {
		// IndexesOf test
		t.Run(test.name, func(t *testing.T) {
			actualIdexes, actualErr := c.IndexesOf(test.inputVars)
			if test.expError != nil {
				assert.Equal(t, test.expError, actualErr,
					"returned wrong error for [%s].IndexOf(%s)", c, test.inputVars)
				return
			}
			assert.Equal(t, test.expIndexes, actualIdexes,
				"returned wrong indexes for [%s].IndexOf(%s)", c, test.inputVars)
		})
		// MustIndexesOf test
		t.Run("MustIndexesOf_"+test.name, func(t *testing.T) {
			if test.expPanic {
				assert.Panics(t, func() {
					c.MustIndexesOf(test.inputVars)
				})
				return
			}
			actualIdexes := c.MustIndexesOf(test.inputVars)
			assert.Equal(t, test.expIndexes, actualIdexes,
				"returned wrong indexes for [%s].MustIndexesOf(%s)", c, test.inputVars)
		})
	}
}

func Test_Columns_String(t *testing.T) {
	assert.Equal(t, "[?s ?p ?id]", Columns{varS, varP, varI}.String())
	assert.Equal(t, "[?s]", Columns{varS}.String())
	assert.Equal(t, "[]", Columns{}.String())
}
