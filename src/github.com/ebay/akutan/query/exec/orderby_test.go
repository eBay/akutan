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
	"testing"
	"time"

	"github.com/ebay/akutan/logentry"
	"github.com/ebay/akutan/query/parser"
	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/rpc"
	"github.com/stretchr/testify/assert"
)

func Test_OrderByOp(t *testing.T) {
	type test struct {
		name       string
		input      []ResultChunk
		inputCols  Columns
		op         plandef.OrderByOp
		expResults []Value
	}
	tests := []test{
		{
			name: "orderby_string_asc",
			input: []ResultChunk{{
				Columns: Columns{varO},
				Values: []Value{
					{KGObject: rpc.AString("Bob", 100), LangExtID: "en"},
					{KGObject: rpc.AString("Alice", 100), LangExtID: "en"},
					{KGObject: rpc.AString("Charlie", 100), LangExtID: "en"},
					{KGObject: rpc.AString("Carly", 100), LangExtID: "en"},
				},
				offsets: []uint32{0, 1, 2, 3},
				Facts:   []FactSet{fs(1), fs(2), fs(3), fs(4)},
			}},
			inputCols: Columns{varO},
			op: plandef.OrderByOp{
				OrderBy: []plandef.OrderCondition{
					plandef.OrderCondition{
						Direction: parser.SortAsc,
						On:        varO,
					},
				},
			},
			expResults: []Value{
				{KGObject: rpc.AString("Alice", 100), LangExtID: "en"},
				{KGObject: rpc.AString("Bob", 100), LangExtID: "en"},
				{KGObject: rpc.AString("Carly", 100), LangExtID: "en"},
				{KGObject: rpc.AString("Charlie", 100), LangExtID: "en"},
			},
		},
		{
			name: "orderby_single_column_only",
			input: []ResultChunk{{
				Columns: Columns{varP, varO},
				Values: []Value{
					{KGObject: rpc.AString("Bob", 100), LangExtID: "en"}, {KGObject: rpc.AInt64(30, 100), UnitExtID: "<age>"},
					{KGObject: rpc.AString("Charlie", 100), LangExtID: "en"}, {KGObject: rpc.AInt64(20, 100), UnitExtID: "<age>"},
					{KGObject: rpc.AString("Bob", 100), LangExtID: "en"}, {KGObject: rpc.AInt64(10, 100), UnitExtID: "<age>"},
				},
				offsets: []uint32{0, 1, 3},
				Facts:   []FactSet{fs(1), fs(2), fs(3)},
			}},
			inputCols: Columns{varP, varO},
			op: plandef.OrderByOp{
				OrderBy: []plandef.OrderCondition{
					plandef.OrderCondition{
						Direction: parser.SortAsc,
						On:        varP,
					},
				},
			},
			expResults: []Value{
				{KGObject: rpc.AString("Bob", 100), LangExtID: "en"}, {KGObject: rpc.AInt64(30, 100), UnitExtID: "<age>"},
				{KGObject: rpc.AString("Bob", 100), LangExtID: "en"}, {KGObject: rpc.AInt64(10, 100), UnitExtID: "<age>"},
				{KGObject: rpc.AString("Charlie", 100), LangExtID: "en"}, {KGObject: rpc.AInt64(20, 100), UnitExtID: "<age>"},
			},
		},
		{
			name: "orderby_string_asc_and_int64_desc",
			input: []ResultChunk{{
				Columns: Columns{varP, varO},
				Values: []Value{
					{KGObject: rpc.AString("Charlie", 100), LangExtID: "en"}, {KGObject: rpc.AInt64(40, 100), UnitExtID: "<age>"},
					{KGObject: rpc.AString("Bob", 100), LangExtID: "en"}, {KGObject: rpc.AInt64(20, 100), UnitExtID: "<age>"},
					{KGObject: rpc.AString("Charlie", 100), LangExtID: "en"}, {KGObject: rpc.AInt64(20, 100), UnitExtID: "<age>"},
					{KGObject: rpc.AString("Bob", 100), LangExtID: "en"}, {KGObject: rpc.AInt64(10, 100), UnitExtID: "<age>"},
					{KGObject: rpc.AString("Alice", 100), LangExtID: "en"}, {KGObject: rpc.AInt64(90, 100), UnitExtID: "<age>"},
				},
				offsets: []uint32{0, 1, 2, 3, 4},
				Facts:   []FactSet{fs(1), fs(2), fs(3), fs(4), fs(5)},
			}},
			inputCols: Columns{varP, varO},
			op: plandef.OrderByOp{
				OrderBy: []plandef.OrderCondition{
					plandef.OrderCondition{
						Direction: parser.SortAsc,
						On:        varP,
					},
					plandef.OrderCondition{
						Direction: parser.SortDesc,
						On:        varO,
					},
				},
			},
			expResults: []Value{
				{KGObject: rpc.AString("Alice", 100), LangExtID: "en"}, {KGObject: rpc.AInt64(90, 100), UnitExtID: "<age>"},
				{KGObject: rpc.AString("Bob", 100), LangExtID: "en"}, {KGObject: rpc.AInt64(20, 100), UnitExtID: "<age>"},
				{KGObject: rpc.AString("Bob", 100), LangExtID: "en"}, {KGObject: rpc.AInt64(10, 100), UnitExtID: "<age>"},
				{KGObject: rpc.AString("Charlie", 100), LangExtID: "en"}, {KGObject: rpc.AInt64(40, 100), UnitExtID: "<age>"},
				{KGObject: rpc.AString("Charlie", 100), LangExtID: "en"}, {KGObject: rpc.AInt64(20, 100), UnitExtID: "<age>"},
			},
		},
		{
			name: "orderby_single_column_with_two_types_asc",
			input: []ResultChunk{{
				Columns: Columns{varO},
				Values: []Value{
					{KGObject: rpc.AString("Bob", 100), LangExtID: "en"},
					{KGObject: rpc.AInt64(20, 100), UnitExtID: "<inch>"},
					{KGObject: rpc.AString("Alice", 100), LangExtID: "en"},
					{KGObject: rpc.AString("Carly", 100), LangExtID: "en"},
					{KGObject: rpc.AInt64(10, 100), UnitExtID: "<inch>"},
				},
				offsets: []uint32{0, 1, 2, 3, 4},
				Facts:   []FactSet{fs(1), fs(2), fs(3), fs(4), fs(5)},
			}},
			inputCols: Columns{varO},
			op: plandef.OrderByOp{
				OrderBy: []plandef.OrderCondition{
					plandef.OrderCondition{
						Direction: parser.SortAsc,
						On:        varO,
					},
				},
			},
			expResults: []Value{
				{KGObject: rpc.AString("Alice", 100), LangExtID: "en"},
				{KGObject: rpc.AString("Bob", 100), LangExtID: "en"},
				{KGObject: rpc.AString("Carly", 100), LangExtID: "en"},
				{KGObject: rpc.AInt64(10, 100), UnitExtID: "<inch>"},
				{KGObject: rpc.AInt64(20, 100), UnitExtID: "<inch>"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exp := ResultChunk{
				Columns: test.inputCols,
				Values:  test.expResults,
			}
			exp.Facts = make([]FactSet, exp.NumRows())
			exp.offsets = make([]uint32, exp.NumRows())

			input := makeInputOpWithChunks(test.input, nil, test.inputCols)
			orderByOp := newOrderByOp(&test.op, []queryOperator{input})
			res, err := executeOp(t, orderByOp, new(defaultBinder))
			assert.NoError(t, err)
			assertResultChunkEqual(t, exp, res)
		})
	}
}

func Test_CompareValues(t *testing.T) {
	assert := assert.New(t)
	compareInts := func(a, b int) int {
		if a == b {
			return 0
		} else if a < b {
			return -1
		}
		return 1
	}
	values := testValues()
	for i1, v1 := range values {
		for i2, v2 := range values {
			assert.Equal(compareInts(i1, i2), compareValues(&v1, &v2),
				"should be equal (i1: %v, v1: %v), (i2: %v, v2: %v)", i1, v1, i2, v2)
		}
	}
}

// testValues returns distinct values that are ordered by it's type and value.
func testValues() []Value {
	return []Value{
		{KGObject: rpc.KGObject{}},
		{KGObject: rpc.AString("Adrian", 0)},
		{KGObject: rpc.AString("Alice", 100), LangExtID: "en"},
		{KGObject: rpc.AString("Bob", 0)},
		{KGObject: rpc.AString("Bob", 100), LangExtID: "en"},
		{KGObject: rpc.AString("Bob", 99), LangExtID: "fr"},
		{KGObject: rpc.AString("Charles", 100), LangExtID: "en"},
		{KGObject: rpc.AString("Charlie", 99), LangExtID: "fr"},
		{KGObject: rpc.AString("David", 99), LangExtID: "fr"},
		{KGObject: rpc.AFloat64(20.50, 0)},
		{KGObject: rpc.AFloat64(21.50, 0)},
		{KGObject: rpc.AFloat64(10.00, 100), UnitExtID: "<centimeter>"},
		{KGObject: rpc.AFloat64(10.25, 100), UnitExtID: "<centimeter>"},
		{KGObject: rpc.AFloat64(5.01, 99), UnitExtID: "<inch>"},
		{KGObject: rpc.AFloat64(5.34, 99), UnitExtID: "<inch>"},
		{KGObject: rpc.AFloat64(12.00, 99), UnitExtID: "<inch>"},
		{KGObject: rpc.AInt64(25, 0)},
		{KGObject: rpc.AInt64(26, 0)},
		{KGObject: rpc.AInt64(15, 100), UnitExtID: "<centimeter>"},
		{KGObject: rpc.AInt64(18, 100), UnitExtID: "<centimeter>"},
		{KGObject: rpc.AInt64(10, 99), UnitExtID: "<inch>"},
		{KGObject: rpc.AInt64(12, 99), UnitExtID: "<inch>"},
		{KGObject: rpc.ATimestampY(2019, 0)},
		{KGObject: rpc.ATimestampY(2020, 0)},
		{KGObject: rpc.ATimestamp(time.Date(2018, 9, 14, 14, 24, 10, 0, time.UTC), logentry.Second, 4), UnitExtID: "<epoch>"},
		{KGObject: rpc.ATimestamp(time.Date(2019, 9, 14, 14, 24, 1, 0, time.UTC), logentry.Second, 4), UnitExtID: "<epoch>"},
		{KGObject: rpc.ATimestampY(2017, 1), UnitExtID: "<new_year>"},
		{KGObject: rpc.ATimestampY(2018, 1), UnitExtID: "<new_year>"},
		{KGObject: rpc.ABool(false, 0)},
		{KGObject: rpc.ABool(true, 0)},
		{KGObject: rpc.ABool(false, 100), UnitExtID: "<is_color_blue>"},
		{KGObject: rpc.ABool(true, 100), UnitExtID: "<is_color_blue>"},
		{KGObject: rpc.ABool(false, 99), UnitExtID: "<is_color_green>"},
		{KGObject: rpc.ABool(true, 99), UnitExtID: "<is_color_green>"},
		{KGObject: rpc.AKID(1), ExtID: "Alice"},
		{KGObject: rpc.AKID(2), ExtID: "Dave"},
		{KGObject: rpc.AKID(5), ExtID: "Ken"},
	}
}
