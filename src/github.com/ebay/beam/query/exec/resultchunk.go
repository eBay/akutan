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
	"io"
	"strings"
	"sync"

	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/util/table"
)

// ResultChunk contains a slice of rows from a table of a single QueryOperator's
// output. The overall results for a QueryOperator consists of 0..N of these.
// The final execution tree is rooted by emptyResultOp which generates a
// ResultChunk with 0 rows in the event the query doesn't generate any results.
// This is required to be able to pass the column info to the API which it needs
// to meet its semantics.
type ResultChunk struct {
	// The columns that this result chunk contains.
	Columns Columns
	// Values contains the values for the rows, these are in the order described
	// by columns. This contains len(columns) values for each row, repeated for
	// each row. For example if there are 4 columns and 5 rows, this will
	// contain 20 values. Although the external API is columnar oriented, while
	// executing the query everything needs row oriented values.
	Values []Value
	// The binder offset that generated the result at row[x]. The results in a
	// single ResultChunk may be for many different bulkOffsets.
	offsets []uint32
	// Although the FactSets are in a slice the order is not relevant.
	Facts []FactSet
	// The last chunk may contain the final statistics value. However, if it's
	// found in any arbitrary chunk, then all of the subsequent chunks received
	// from a channel will contain the same final statistics value.
	FinalStatistics FinalStatistics
}

// ToTable writes a human readable version of the chunk results as a table to
// the supplied Writer.
func (r *ResultChunk) ToTable(w io.Writer) {
	fmt.Fprintf(w, "Chunk with %d columns, %d rows\n", len(r.Columns), r.NumRows())
	t := make([][]string, 1, len(r.offsets)+1)
	for i := range r.Columns {
		t[0] = append(t[0], r.Columns[i].Name)
	}
	row := make([]string, 0, len(r.Columns))
	for _, v := range r.Values {
		row = append(row, v.String())
		if len(row) == len(t[0]) {
			t = append(t, row)
			row = make([]string, 0, len(r.Columns))
		}
	}
	table.PrettyPrint(w, t, table.HeaderRow)
}

// len implements valueBinder for the ResultChunk
func (r *ResultChunk) len() uint32 {
	return uint32(len(r.offsets))
}

// bind implements valueBinder for the ResultChunk
func (r *ResultChunk) bind(index uint32, b *plandef.Binding) *Value {
	col, exists := r.Columns.IndexOf(b.Var)
	if !exists {
		return nil
	}
	return &r.Values[int(index)*len(r.Columns)+col]
}

// Row returns the values for the indicated row number. The values are in the
// order indicated in ResultChunk.Columns
func (r *ResultChunk) Row(row int) []Value {
	rowOffset := row * len(r.Columns)
	return r.Values[rowOffset : rowOffset+len(r.Columns)]
}

// NumRows returns the number of rows in this chunk.
func (r *ResultChunk) NumRows() int {
	if len(r.Columns) == 0 {
		return 0
	}
	return len(r.Values) / len(r.Columns)
}

// identityKeysOf returns a single key value that includes the serialization of
// each value for the supplied list of columns. This is typically used in
// evaluating join expressions.
func (r *ResultChunk) identityKeysOf(rowNum int, columns []int) string {
	row := r.Row(rowNum)
	size := 0
	for _, colIdx := range columns {
		size += row[colIdx].KGObject.Size() + 1
	}
	var b strings.Builder
	b.Grow(size)
	for _, colIdx := range columns {
		b.WriteString(row[colIdx].KGObject.AsString())
		b.WriteByte('.')
	}
	return b.String()
}

// preferredResultChunkSize indicates the ideal number of FactSets in a
// ResultChunk that the overall query execution package would prefer.
const preferredResultChunkSize = 2048

// resultChunkBuilder accumulates results into a ResultChunk, flushing them out
// as the they reach flushAtSize size. The builder may choose to produce chunks
// smaller or larger than the target flushAtSize. Its safe to call add/flush
// concurrently.
type resultChunkBuilder struct {
	lock   sync.Mutex
	locked struct {
		chunk                     ResultChunk
		stats                     StreamStats
		needToSendFinalStatistics bool
	}
	// flushAtSize indicates how many rows should be accumulated in the ResultChunk before
	// its flushed to the result channel.
	flushAtSize uint32
	resCh       chan<- ResultChunk
}

// newChunkBuilder returns a default resultChunkBuilder that sends results to
// the provided channel. After construction but before calling add you can
// update the flushAtSize configuration if you need to override the default.
// Changing flushAtSize is primarily for testing.
//
// 'out' contains the list of columns to generate in the output
// ResultChunks. All generated chunks will have these columns.
func newChunkBuilder(resCh chan<- ResultChunk, out Columns) *resultChunkBuilder {
	rb := resultChunkBuilder{
		flushAtSize: preferredResultChunkSize,
		resCh:       resCh,
	}
	rb.locked.chunk = ResultChunk{
		Columns: out,
	}
	return &rb
}

// add appends the new Row to the current chunk, flushing it if needed.
// 'rowValues' should contain the values for the row in the same order as the
// columns that were specified in the newChunkBuilder call. This may block if
// the output channel is full.
func (b *resultChunkBuilder) add(ctx context.Context, offset uint32, f FactSet, rowValues []Value) {
	b.lock.Lock()
	defer b.lock.Unlock()

	c := &b.locked.chunk
	if len(c.Columns) != len(rowValues) {
		panic("add called with a different number of values, to the number of columns")
	}
	c.Facts = append(c.Facts, f)
	c.offsets = append(c.offsets, offset)
	c.Values = append(c.Values, rowValues...)
	if c.len() >= b.flushAtSize {
		b.lockedFlush(ctx)
	}
}

// flush sends any buffered data to the result channel. You can continue to call
// add after performing a flush. It returns the current stats reflecting the
// flushed item. Returns once the chunk is accepted by the result channel, or
// the context is cancelled.
func (b *resultChunkBuilder) flush(ctx context.Context) StreamStats {
	b.lock.Lock()
	s := b.lockedFlush(ctx)
	b.lock.Unlock()
	return s
}

// lockedFlush sends any buffered data to the result channel. The lock should
// already be taken before calling this. It is expected that only other code in
// resultChunkBuilder should call this.
func (b *resultChunkBuilder) lockedFlush(ctx context.Context) StreamStats {
	if len(b.locked.chunk.offsets) > 0 || b.locked.needToSendFinalStatistics {
		b.locked.stats.NumChunks++
		b.locked.stats.NumFactSets += len(b.locked.chunk.offsets)
		b.locked.needToSendFinalStatistics = false
		select {
		case b.resCh <- b.locked.chunk:
		case <-ctx.Done():
		}
		b.locked.chunk = ResultChunk{
			Columns:         b.locked.chunk.Columns,
			FinalStatistics: b.locked.chunk.FinalStatistics,
		}
	}
	return b.locked.stats
}

// setFinalStatistics sets the supplied stats to the current chunk which will be
// sent to the output channel during next flush/lockedFlush call. The stats will
// be included in to the newly created chunks as well. It's safe to call
// setFinalStatistics in any order along with add. Overwriting previously set
// stats with a new value causes panic.
func (b *resultChunkBuilder) setFinalStatistics(stats FinalStatistics) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.locked.chunk.FinalStatistics == stats {
		return
	}
	zeroVal := FinalStatistics{}
	if b.locked.chunk.FinalStatistics != zeroVal {
		panic("setFinalStatistics called to overwrite final stats")
	}
	b.locked.chunk.FinalStatistics = stats
	b.locked.stats.FinalStatistics = stats
	b.locked.needToSendFinalStatistics = true
}

// Columns describes the ordered list of columns that is generated by an
// operator.
type Columns []*plandef.Variable

// IndexOf returns the index into Columns that the variable 'v' appears
// and the value true, or (0,false) if its not in the columns.
func (c Columns) IndexOf(v *plandef.Variable) (int, bool) {
	for i, x := range c {
		if x == v {
			return i, true
		}
	}
	return 0, false
}

// MustIndexOf returns the index into Columns that the variable 'v' appears. It
// panics if it can't find v. This should only be used when it's a programmer
// error for v to not be in the columns.
func (c Columns) MustIndexOf(v *plandef.Variable) int {
	idx, exists := c.IndexOf(v)
	if !exists {
		panic(fmt.Sprintf("Didn't find expected variable %s in columns %s", v, c))
	}
	return idx
}

// IndexesOf returns the indexes that the variables 'vars' appear at in
// Columns. Returns (nil, error) if any of the variables is not found in
// Columns.
func (c Columns) IndexesOf(vars []*plandef.Variable) ([]int, error) {
	columns := make([]int, len(vars))
	for i, v := range vars {
		colIdx, exists := c.IndexOf(v)
		if !exists {
			return nil, fmt.Errorf("variable %s is not found in the Columns (it has %s)", v, c)
		}
		columns[i] = colIdx
	}
	return columns, nil
}

// MustIndexesOf returns the indexes that the variables 'vars' appears at in
// Columns. It panics if it can't find any of the variable in Columns.
func (c Columns) MustIndexesOf(vars []*plandef.Variable) []int {
	columns, err := c.IndexesOf(vars)
	if err != nil {
		panic(err)
	}
	return columns
}

func (c Columns) String() string {
	s := new(strings.Builder)
	s.WriteByte('[')
	for i, col := range c {
		if i > 0 {
			s.WriteByte(' ')
		}
		s.WriteString(col.String())
	}
	s.WriteByte(']')
	return s.String()
}

// FinalStatistics contains statistics that a QueryOperator can output.
type FinalStatistics struct {
	// For queries that specify a LIMIT or OFFSET clause, this field is
	// populated with the overall resultset size that would have been returned
	// without the clause.
	TotalResultSize uint64
}
