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

	"github.com/ebay/beam/query/parser"
	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/rpc"
	"github.com/stretchr/testify/assert"
)

func Test_AggrExprEval(t *testing.T) {
	type test struct {
		def       plandef.AggregateExpr
		inputCols Columns
		input     [][]Value
		expected  int64
	}
	tests := []test{{
		def: plandef.AggregateExpr{
			Func: parser.AggCount,
			Of:   plandef.WildcardExpr{},
		},
		input: [][]Value{
			{{}},
			{{KGObject: rpc.AInt64(10, 1)}},
			{{}},
		},
		expected: 3,
	}, {
		def: plandef.AggregateExpr{
			Func: parser.AggCount,
			Of:   varS,
		},
		inputCols: Columns{varP, varS},
		input: [][]Value{
			{kidVal(1), kidVal(2)},
			{kidVal(3), {}},
			{kidVal(4), kidVal(5)},
		},
		expected: 2,
	}, {
		def: plandef.AggregateExpr{
			Func: parser.AggCount,
			Of:   varP,
		},
		inputCols: Columns{varP, varS},
		input:     [][]Value{},
		expected:  0,
	}}
	for _, test := range tests {
		t.Run(test.def.String(), func(t *testing.T) {
			eval := newAggrExprEvaluator(&test.def, test.inputCols)
			for _, r := range test.input {
				assert.Nil(t, eval.consume(r))
			}
			assert.Equal(t, test.expected, eval.completed().KGObject.ValInt64())
		})
	}
}

func Test_AggrExprEval_CheckFunc(t *testing.T) {
	def := plandef.AggregateExpr{Func: 42, Of: varS}
	assert.PanicsWithValue(t, "Unexpected aggregate function: Unknown AggregateFunction (42)", func() {
		newAggrExprEvaluator(&def, Columns{varS})
	})
}
