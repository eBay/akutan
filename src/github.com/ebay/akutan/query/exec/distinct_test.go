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

	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/rpc"
	"github.com/stretchr/testify/assert"
)

func Test_DistinctOp(t *testing.T) {
	type test struct {
		name       string
		input      []ResultChunk
		inputCols  Columns
		expResults ResultChunk
	}
	tests := []test{
		{
			name: "distinct_single_column",
			input: []ResultChunk{
				{
					Columns: Columns{varO},
					Values: []Value{
						{KGObject: rpc.AString("Bob", 100)},
						{KGObject: rpc.AString("Alice", 100)},
					},
					offsets: []uint32{0, 0},
					Facts:   []FactSet{fs(1), fs(2)},
				},
				{
					Columns: Columns{varO},
					Values: []Value{
						{KGObject: rpc.AString("Alice", 200)},
						{KGObject: rpc.AString("Bob", 100)},
						{KGObject: rpc.AString("Charlie", 100)},
					},
					offsets: []uint32{0, 0, 0},
					Facts:   []FactSet{fs(3), fs(4), fs(5)},
				}},
			inputCols: Columns{varO},
			expResults: ResultChunk{
				Columns: Columns{varO},
				Values: []Value{
					{KGObject: rpc.AString("Bob", 100)},
					{KGObject: rpc.AString("Alice", 100)},
					{KGObject: rpc.AString("Alice", 200)},
					{KGObject: rpc.AString("Charlie", 100)},
				},
				offsets: []uint32{0, 0, 0, 0},
				Facts:   []FactSet{fs(1), fs(2), fs(3), fs(5)},
			},
		},
		{
			name: "distinct_multi_columns",
			input: []ResultChunk{{
				Columns: Columns{varP, varO},
				Values: []Value{
					{KGObject: rpc.AString("Charlie", 100)}, {KGObject: rpc.AInt64(40, 100)},
					{KGObject: rpc.AString("Bob", 100)}, {KGObject: rpc.AInt64(20, 100)},
					{KGObject: rpc.AString("Charlie", 100)}, {KGObject: rpc.AInt64(40, 100)},
					{KGObject: rpc.AString("Bob", 100)}, {KGObject: rpc.AInt64(20, 100)},
					{KGObject: rpc.AString("Alice", 100)}, {KGObject: rpc.AInt64(90, 100)},
					{KGObject: rpc.AString("Dave", 10)}, {KGObject: rpc.AInt64(10, 100)},
					{KGObject: rpc.AString("Dave", 10)}, {KGObject: rpc.AInt64(20, 100)},
				},
				offsets: []uint32{0, 0, 0, 0, 0, 0, 0},
				Facts:   []FactSet{fs(1), fs(2), fs(3), fs(4), fs(5), fs(6), fs(7)},
			}},
			inputCols: Columns{varP, varO},
			expResults: ResultChunk{
				Columns: Columns{varP, varO},
				Values: []Value{
					{KGObject: rpc.AString("Charlie", 100)}, {KGObject: rpc.AInt64(40, 100)},
					{KGObject: rpc.AString("Bob", 100)}, {KGObject: rpc.AInt64(20, 100)},
					{KGObject: rpc.AString("Alice", 100)}, {KGObject: rpc.AInt64(90, 100)},
					{KGObject: rpc.AString("Dave", 10)}, {KGObject: rpc.AInt64(10, 100)},
					{KGObject: rpc.AString("Dave", 10)}, {KGObject: rpc.AInt64(20, 100)},
				},
				offsets: []uint32{0, 0, 0, 0, 0},
				Facts:   []FactSet{fs(1), fs(2), fs(5), fs(6), fs(7)},
			},
		},
		{
			// ExternalIDs are excluded by the distinct op.
			name: "distinct_kid_with_externalIDs",
			input: []ResultChunk{{
				Columns: Columns{varO},
				Values: []Value{
					{KGObject: rpc.AKID(1), ExtID: "<inch>"},
					{KGObject: rpc.AKID(1), ExtID: "<ounce>"},
					{KGObject: rpc.AKID(3), ExtID: "<inch>"},
					{KGObject: rpc.AKID(4), ExtID: "<inch>"},
					{KGObject: rpc.AKID(5), ExtID: "<feet>"},
				},
				offsets: []uint32{0, 0, 0, 0, 0},
				Facts:   []FactSet{fs(1), fs(2), fs(3), fs(4), fs(5)},
			}},
			inputCols: Columns{varO},
			expResults: ResultChunk{
				Columns: Columns{varO},
				Values: []Value{
					{KGObject: rpc.AKID(1), ExtID: "<inch>"},
					{KGObject: rpc.AKID(3), ExtID: "<inch>"},
					{KGObject: rpc.AKID(4), ExtID: "<inch>"},
					{KGObject: rpc.AKID(5), ExtID: "<feet>"},
				},
				offsets: []uint32{0, 0, 0, 0},
				Facts:   []FactSet{fs(1), fs(3), fs(4), fs(5)},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := makeInputOpWithChunks(test.input, nil, test.inputCols)
			distinctOp := newDistinctOp(&plandef.DistinctOp{}, []queryOperator{input})
			res, err := executeOp(t, distinctOp, new(defaultBinder))
			assert.NoError(t, err)
			assertResultChunkEqual(t, test.expResults, res)
		})
	}
}

func Test_DistinctOp_all_values(t *testing.T) {
	values := testValues()
	for i1, v1 := range values {
		for i2, v2 := range values {
			inputChunks := []ResultChunk{{
				Columns: Columns{varS},
				Values:  []Value{v1, v2},
				offsets: []uint32{0, 1},
				Facts:   []FactSet{fs(1), fs(2)},
			}}
			input := makeInputOpWithChunks(inputChunks, nil, Columns{varS})
			res := resultsCollector{t: t}
			distinctOp := newDistinctOp(&plandef.DistinctOp{}, []queryOperator{input})
			distinctOp.execute(context.Background(), new(defaultBinder), &res)
			resChunks := res.asChunk()
			if i1 == i2 {
				assert.Equal(t, 1, len(resChunks.Values))
				assert.Equal(t, []Value{v1}, resChunks.Values)
				continue
			}
			assert.Equal(t, 2, len(resChunks.Values))
			assert.Equal(t, []Value{v1, v2}, resChunks.Values)
		}
	}
}
