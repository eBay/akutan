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
	"fmt"

	"github.com/ebay/akutan/query/parser"
	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/rpc"
)

// exprEvaluator is used to calculate the result of an expression used in a
// Projection. The Projection operator uses these to generate its results.
type exprEvaluator interface {
	// consume is called for each row we receive from the input. Return an
	// output value that is the evaluation of the expression on this row. Return
	// nil if there's no output at all for the input row (like in an aggregate
	// expression)
	consume(in []Value) *Value
	// completed is called once all results have been passed to consume. Return
	// a value for a final output row (e.g. an aggregate expression result), or
	// nil if there is no final value.
	completed() *Value
}

// buildExprEvaluators returns an expression evaluator for each item in the
// projection.
func buildExprEvaluators(op *plandef.Projection, input Columns) []exprEvaluator {
	res := make([]exprEvaluator, len(op.Select))
	for i, exprBinding := range op.Select {
		switch s := exprBinding.Expr.(type) {
		case *plandef.Variable:
			res[i] = &variableExpr{variable: s, inputIndex: input.MustIndexOf(s)}
		case *plandef.AggregateExpr:
			res[i] = newAggrExprEvaluator(s, input)
		default:
			panic(fmt.Sprintf("Unexpected expression type %T %v", exprBinding.Expr, exprBinding.Expr))
		}
	}
	return res
}

// variableExpr is an evaluator for variable expressions. They return the
// current value of the variable
type variableExpr struct {
	variable   *plandef.Variable
	inputIndex int
}

func (v *variableExpr) consume(in []Value) *Value {
	return &in[v.inputIndex]
}

func (v *variableExpr) completed() *Value {
	return nil
}

// newAggrExprEvaluator returns an expression evaluator that calculates the
// aggregate expression defined in 'def'
func newAggrExprEvaluator(def *plandef.AggregateExpr, input Columns) exprEvaluator {
	if def.Func != parser.AggCount {
		panic(fmt.Sprintf("Unexpected aggregate function: %v", def.Func))
	}
	switch exprDef := def.Of.(type) {
	case plandef.WildcardExpr:
		return &aggrExprEvaluator{
			def: def,
			aggregate: func([]Value) int64 {
				return 1
			},
		}
	case *plandef.Variable:
		// ensures that the variable is available before we start executing
		inputColumnIdx := input.MustIndexOf(exprDef)
		agg := aggrExprEvaluator{
			def: def,
			aggregate: func(in []Value) int64 {
				if in[inputColumnIdx].KGObject.IsType(rpc.KtNil) {
					return 0
				}
				return 1
			},
		}
		return &agg
	default:
		panic(fmt.Sprintf("Unexpected aggregate target type %T %v", def.Of, def.Of))
	}
}

type aggrExprEvaluator struct {
	def       *plandef.AggregateExpr
	val       int64
	aggregate func([]Value) int64
}

func (a *aggrExprEvaluator) consume(in []Value) *Value {
	a.val += a.aggregate(in)
	return nil
}

func (a *aggrExprEvaluator) completed() *Value {
	return &Value{KGObject: rpc.AInt64(a.val, 0)}
}
