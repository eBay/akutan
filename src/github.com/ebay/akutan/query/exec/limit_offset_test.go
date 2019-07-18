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
	"math"
	"testing"

	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/stretchr/testify/assert"
)

func Test_LimitAndOffsetOp(t *testing.T) {
	values := []Value{
		kidVal(20),
		kidVal(52),
		kidVal(60),
		kidVal(61),
		kidVal(21),
		kidVal(82),
		kidVal(10),
		kidVal(11),
		kidVal(12),
		kidVal(22),
		kidVal(50),
		kidVal(51),
		kidVal(60),
	}

	// numChunks needs to be greater than 1.
	assertLimitOffset := func(numChunks int, vs []Value, limit uint64, offset uint64, expected []Value) {
		columns := Columns{varS}
		chunks := make([]ResultChunk, 0, numChunks)

		valuesForChunk := func(chunkIdx int) []Value {
			rowsPerChunk := len(vs) / numChunks
			start, end := chunkIdx*rowsPerChunk, (chunkIdx*rowsPerChunk)+rowsPerChunk
			if start > len(vs) {
				start = len(vs)
			}
			if end > len(vs) {
				end = len(vs)
			}
			// last chunk will contain remaining rows, if any.
			if chunkIdx+1 == numChunks && len(vs)%numChunks > 0 {
				end += len(vs) % numChunks
			}
			return vs[start:end]
		}

		for i := 0; i < numChunks; i++ {
			c := ResultChunk{
				Columns: columns,
				Values:  valuesForChunk(i),
				offsets: []uint32{},
				Facts:   []FactSet{},
			}
			for v := range c.Values {
				c.offsets = append(c.offsets, 0)
				c.Facts = append(c.Facts, fs(uint64(v)))
			}
			chunks = append(chunks, c)
		}
		op := &plandef.LimitAndOffsetOp{
			Paging: plandef.LimitOffset{
				Limit:  &limit,
				Offset: &offset,
			},
		}
		input := makeInputOpWithChunks(chunks, nil, columns)
		limitAndOffsetOp := newLimitAndOffsetOp(op, []queryOperator{input})
		expResults := ResultChunk{
			Columns: Columns{varS},
			Values:  expected,
			FinalStatistics: FinalStatistics{
				TotalResultSize: uint64(len(vs)),
			},
		}
		expResults.offsets = make([]uint32, expResults.NumRows())
		expResults.Facts = make([]FactSet, expResults.NumRows())
		res, err := executeOp(t, limitAndOffsetOp, new(defaultBinder))
		assert.NoError(t, err)
		assertResultChunkEqual(t, expResults, res)
	}

	for numChunks := 1; numChunks <= len(values); numChunks++ {
		for offset := 0; offset < len(values)+10; offset++ {
			for limit := 0; limit < len(values)+10; limit++ {
				var start, end = offset, offset + limit
				if start > len(values) {
					start = len(values)
				}
				if end > len(values) {
					end = len(values)
				}
				expected := values[start:end]
				assertLimitOffset(numChunks, values, uint64(limit), uint64(offset), expected)
			}
		}
	}

	assertLimitOffset(1, values, uint64(math.MaxUint64), uint64(0), values)
	assertLimitOffset(1, values, uint64(0), uint64(math.MaxUint64), []Value{})
	assertLimitOffset(1, values, uint64(math.MaxUint64), uint64(math.MaxUint64), []Value{})
}
