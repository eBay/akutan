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

package plandef

import (
	"testing"

	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/cmp"
	"github.com/stretchr/testify/assert"
)

func Test_SelectLit(t *testing.T) {
	assert := assert.New(t)
	op := &SelectLit{
		Test: &Variable{Name: "test"},
	}
	assert.Equal("SelectLit ?test", op.String())
	assert.Equal("SelectLit ?test", cmp.GetKey(op))
	op.Clauses = append(op.Clauses, SelectClause{
		Comparison: rpc.OpLess,
		Literal1:   &Literal{Value: rpc.AInt64(30, 0)},
	})
	assert.Equal("SelectLit ?test < 30", op.String())
	assert.Equal("SelectLit ?test < int64:30", cmp.GetKey(op))
	op.Clauses = append(op.Clauses, SelectClause{
		Comparison: rpc.OpRangeIncExc,
		Literal1:   &Literal{Value: rpc.AInt64(10, 0)},
		Literal2:   &Literal{Value: rpc.AInt64(20, 0)},
	})
	assert.Equal("SelectLit ?test < 30 in [10, 20)", op.String())
	assert.Equal("SelectLit ?test < int64:30 rangeIncExc int64:10 int64:20", cmp.GetKey(op))
}

func Test_SelectClause_StringKey(t *testing.T) {
	var (
		ten       = &Literal{Value: rpc.AInt64(10, 0)}
		twenty    = &Literal{Value: rpc.AInt64(20, 0)}
		fourNines = &Literal{Value: rpc.AInt64(4, 9)}
		rodeo     = &Literal{Value: rpc.AString("rodeo", 12)}
	)
	tests := []struct {
		clause    SelectClause
		expString string
		expKey    string
	}{
		{
			clause:    SelectClause{Comparison: rpc.OpRangeIncExc, Literal1: ten, Literal2: twenty},
			expString: "in [10, 20)",
			expKey:    "rangeIncExc int64:10 int64:20",
		},
		{
			clause:    SelectClause{Comparison: rpc.OpRangeIncInc, Literal1: ten, Literal2: twenty},
			expString: "in [10, 20]",
			expKey:    "rangeIncInc int64:10 int64:20",
		},
		{
			clause:    SelectClause{Comparison: rpc.OpRangeExcInc, Literal1: ten, Literal2: twenty},
			expString: "in (10, 20]",
			expKey:    "rangeExcInc int64:10 int64:20",
		},
		{
			clause:    SelectClause{Comparison: rpc.OpRangeExcExc, Literal1: ten, Literal2: twenty},
			expString: "in (10, 20)",
			expKey:    "rangeExcExc int64:10 int64:20",
		},
		{
			clause:    SelectClause{Comparison: rpc.OpRangeExcExc, Literal1: ten, Literal2: fourNines},
			expString: "in (10, 4^^#9)",
			expKey:    "rangeExcExc int64:10 int64:4^^#9",
		},
		{
			clause:    SelectClause{Comparison: rpc.OpLess, Literal1: ten},
			expString: "< 10",
			expKey:    "< int64:10",
		},
		{
			clause:    SelectClause{Comparison: rpc.OpPrefix, Literal1: rodeo},
			expString: `prefix "rodeo"@#12`,
			expKey:    `prefix string:"rodeo"@#12`,
		},
		{
			clause:    SelectClause{Comparison: rpc.Operator(90210), Literal1: ten},
			expString: "unknown(90210) 10",
			expKey:    "unknown(90210) int64:10",
		},
		{
			clause:    SelectClause{Comparison: rpc.Operator(90210), Literal1: ten, Literal2: twenty},
			expString: "unknown(90210) 10 20",
			expKey:    "unknown(90210) int64:10 int64:20",
		},
	}
	for i, test := range tests {
		assert.Equal(t, test.expString, test.clause.String(),
			"test %d", i)
		assert.Equal(t, test.expKey, cmp.GetKey(test.clause),
			"expString: %v, test: %d", test.expString, i)
	}
}

func Test_SelectVar(t *testing.T) {
	assert := assert.New(t)
	op := &SelectVar{
		Left:     &Variable{Name: "left"},
		Right:    &Variable{Name: "right"},
		Operator: rpc.OpPrefix,
	}
	assert.Equal("SelectVar ?left prefix ?right", op.String())
	assert.Equal("SelectVar ?left prefix ?right", cmp.GetKey(op))
	op.Operator = rpc.OpLess
	assert.Equal("SelectVar ?left < ?right", op.String())
	assert.Equal("SelectVar ?left < ?right", cmp.GetKey(op))
}
