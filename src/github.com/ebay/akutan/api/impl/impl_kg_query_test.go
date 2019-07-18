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

package impl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ebay/akutan/api"
	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/config"
	"github.com/ebay/akutan/query"
	"github.com/ebay/akutan/query/exec"
	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/rpc"
	"github.com/stretchr/testify/assert"
)

type queryTest struct {
	name        string
	input       []query.ResultChunk
	inputErr    error
	sendErr     error
	expected    []outputChunk
	expectedErr string
}

type outputChunk [][]string // [col][row]string. row[0] should have the column name in it.

func Test_Query(t *testing.T) {
	vehicle := &plandef.Variable{Name: "vehicle"}
	rating := &plandef.Variable{Name: "rating"}
	tests := []queryTest{{
		name: "2 chunks, 2 cols",
		input: []query.ResultChunk{{
			Columns: exec.Columns{vehicle, rating},
			Values: []exec.Value{
				{KGObject: rpc.AKID(42), ExtID: "ford:bronco"},
				{KGObject: rpc.AInt64(5, 0)},
				{KGObject: rpc.AKID(44), ExtID: "ford:focus"},
				{KGObject: rpc.AInt64(3, 0)},
			},
		}, {
			Columns: exec.Columns{vehicle, rating},
			Values: []exec.Value{
				{KGObject: rpc.AKID(45), ExtID: "ford:mustang"},
				{KGObject: rpc.AInt64(6, 0)},
			},
		}},
		expected: []outputChunk{{
			{"vehicle", "ford:bronco", "ford:focus"},
			{"rating", "5", "3"},
		}, {
			{"vehicle", "ford:mustang"},
			{"rating", "6"},
		}},
	}, {
		name: "no results",
		input: []query.ResultChunk{{
			Columns: exec.Columns{vehicle, rating},
		}},
		expected: []outputChunk{{
			{"vehicle"},
			{"rating"},
		}},
	}, {
		name:        "query error",
		inputErr:    fmt.Errorf("Failed to execute query"),
		expectedErr: "Failed to execute query",
	}, {
		name: "send error",
		input: []query.ResultChunk{{
			Columns: exec.Columns{vehicle},
			Values: []exec.Value{
				{KGObject: rpc.AKID(42), ExtID: "ford:bronco"},
			},
		},
		},
		sendErr:     fmt.Errorf("Failed to send chunk"),
		expectedErr: "Failed to send chunk",
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queryEngine := mockQuery{results: test.input, err: test.inputErr}
			s := Server{
				queryEngine: &queryEngine,
				cfg: &config.Akutan{
					API: &config.API{},
				},
			}
			req := api.QueryRequest{
				Index: api.LogIndex{Constraint: api.LogIndexExact, Index: 100},
				// As we're faking out the query engine, it doesn't matter what the query is
				Query: "SELECT * FROM { ... }",
			}
			out := []api.QueryResult{}
			sender := func(r *api.QueryResult) error {
				out = append(out, *r)
				return test.sendErr
			}
			err := s.query(context.Background(), &req, sender)
			if test.expectedErr != "" {
				assert.EqualError(t, err, test.expectedErr)
				return
			}
			assert.NoError(t, err)
			assertQueryResultsEqual(t, out, test.expected)
			assert.Equal(t, req.Index.Index, queryEngine.executedAtIndex)
		})
	}
}

func assertQueryResultsEqual(t *testing.T, actual []api.QueryResult, expChunks []outputChunk) {
	assert.Equal(t, len(expChunks), len(actual), "Different number of result chunks received")
	for i, expChunk := range expChunks {
		act := actual[i]
		assert.Equal(t, len(expChunk), len(act.Columns))
		for colIdx, expCol := range expChunk {
			actCol := []string{act.Columns[colIdx].Name}
			for _, cell := range act.Columns[colIdx].Cells {
				actCol = append(actCol, cell.String())
			}
			assert.Equal(t, expCol, actCol, "Unexpected column results in chunk %d column %s", i, expCol[0])
		}
	}
}

type mockQuery struct {
	results []query.ResultChunk
	err     error

	executedAtIndex blog.Index
}

func (m *mockQuery) Query(ctx context.Context, index blog.Index, rawQuery string,
	opt query.Options, resCh chan<- query.ResultChunk) error {

	for _, r := range m.results {
		resCh <- r
	}
	close(resCh)
	m.executedAtIndex = index
	return m.err
}

func Test_ToKGValue(t *testing.T) {
	type test struct {
		input    exec.Value
		expected api.KGValue
	}
	tests := []test{{
		input:    exec.Value{KGObject: rpc.AKID(42), ExtID: "rdf:type"},
		expected: api.KGValue{Value: &api.KGValue_Node{Node: &api.KGID{QName: "rdf:type", SysId: 42}}},
	}, {
		input:    exec.Value{KGObject: rpc.AInt64(42, 0)},
		expected: api.KGValue{Value: &api.KGValue_Int64{Int64: &api.KGInt64{Value: 42, Unit: nil}}},
	}, {
		input: exec.Value{KGObject: rpc.AInt64(42, 10), UnitExtID: "<inch>"},
		expected: api.KGValue{Value: &api.KGValue_Int64{Int64: &api.KGInt64{
			Value: 42, Unit: &api.KGID{QName: "<inch>", SysId: 10},
		}}},
	}, {
		input:    exec.Value{KGObject: rpc.AFloat64(42, 0)},
		expected: api.KGValue{Value: &api.KGValue_Float64{Float64: &api.KGFloat64{Value: 42, Unit: nil}}},
	}, {
		input: exec.Value{KGObject: rpc.AFloat64(42, 10), UnitExtID: "<inch>"},
		expected: api.KGValue{Value: &api.KGValue_Float64{Float64: &api.KGFloat64{
			Value: 42, Unit: &api.KGID{QName: "<inch>", SysId: 10},
		}}},
	}, {
		input:    exec.Value{KGObject: rpc.ABool(true, 0)},
		expected: api.KGValue{Value: &api.KGValue_Bool{Bool: &api.KGBool{Value: true, Unit: nil}}},
	}, {
		input: exec.Value{KGObject: rpc.ABool(true, 11), UnitExtID: "<inch>"},
		expected: api.KGValue{Value: &api.KGValue_Bool{Bool: &api.KGBool{
			Value: true, Unit: &api.KGID{QName: "<inch>", SysId: 11},
		}}},
	}, {
		input:    exec.Value{KGObject: rpc.AString("Alice", 0)},
		expected: api.KGValue{Value: &api.KGValue_Str{Str: &api.KGString{Value: "Alice", Lang: nil}}},
	}, {
		input: exec.Value{KGObject: rpc.AString("Alice", 11), LangExtID: "en"},
		expected: api.KGValue{Value: &api.KGValue_Str{Str: &api.KGString{
			Value: "Alice", Lang: &api.KGID{QName: "en", SysId: 11},
		}}},
	}, {
		input: exec.Value{KGObject: rpc.ATimestampYMD(2019, 1, 7, 0)},
		expected: api.KGValue{Value: &api.KGValue_Timestamp{
			Timestamp: &api.KGTimestamp{Precision: api.Day, Value: time.Date(2019, 1, 7, 0, 0, 0, 0, time.UTC)}}},
	}, {
		input:    exec.Value{KGObject: rpc.KGObject{}},
		expected: api.KGValue{},
	}}
	for _, testCase := range tests {
		t.Run(testCase.input.String(), func(t *testing.T) {
			assert.Equal(t, testCase.expected, toKGValue(testCase.input))
		})
	}
}
