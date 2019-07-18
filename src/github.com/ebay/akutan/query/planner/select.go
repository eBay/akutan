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

package planner

import (
	"github.com/ebay/akutan/query/parser"
	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/query/planner/search"
	"github.com/ebay/akutan/util/cmp"
)

// pushDownSelectLit and pushDownSelectVar will generate alternatives where a
// select operator has been pushed further down the plan tree. The closer to the
// leaves the select can be placed, the more efficient the execution will be.
//
// For select over inner joins and left joins, applies:
// 	Before: select(x cmp k, join(a, b)) where a and b emit x
// 	After:  join(select(x cmp k, a), select(x cmp k, b))
// or
// 	Before: select(x cmp k, join(a, b)) where a emits x
// 	After:  join(select(x cmp k, a), b)
// or
// 	Before: select(x cmp k, join(a, b)) where b emits x
// 	After:  join(a, select(x cmp k, b))
//
// For select over select, applies:
// 	Before: select(x cmp k, select(y cmp l, a))
// 	After:  select(y cmp l, select(x cmp k, a))
//
// The included case for leftLoopJoin is related to the workaround described in
// preImplementLeftLoopJoin. That work around means there will not be any
// leftJoins in the plan, just optional LoopJoins
//
func pushDownSelectLit(root *search.Expr) []*search.IntoExpr {
	selection, ok := root.Operator.(*plandef.SelectLit)
	if !ok {
		return nil
	}
	selectionVars := plandef.VarSet{selection.Test}
	return pushDownSelectImpl(selectionVars, root.Operator, root.Inputs[0])
}

// pushDownSelectVar generates alternatives for a selectVar where the selection
// has been pushed down the tree. This does the same as pushDownSelectLit, but
// for the SelectVar selection operator. see pushDownSelectLit for details on
// the rules it applies.
func pushDownSelectVar(root *search.Expr) []*search.IntoExpr {
	selection, ok := root.Operator.(*plandef.SelectVar)
	if !ok {
		return nil
	}
	vars := map[string]*plandef.Variable{
		selection.Left.Name:  selection.Left,
		selection.Right.Name: selection.Right,
	}
	selectionVars := plandef.NewVarSet(vars)
	return pushDownSelectImpl(selectionVars, root.Operator, root.Inputs[0])
}

// pushDownSelectImpl does the real work for pushDownSelectLit & pushDownSelectVar.
// It will return alternatives where 'selectOp' has been push down below operators
// where those operators inputs still contain all the variables in 'reqVars'.
func pushDownSelectImpl(reqVars plandef.VarSet, selectOp search.Operator, selectInput *search.Group) []*search.IntoExpr {
	var ret []*search.IntoExpr
	for _, alt := range selectInput.Exprs {
		switch op := alt.Operator.(type) {
		case *innerJoinOperator:
			ret = append(ret,
				selectOverJoinAlternatives(reqVars, selectOp, op, alt.Inputs)...)

		case *leftJoinOperator:
			ret = append(ret,
				selectOverJoinAlternatives(reqVars, selectOp, op, alt.Inputs)...)

		case *plandef.LoopJoin:
			// This is part of the work around described in preImplementLeftLoopJoin.
			// When that is removed, this case can be removed as well.
			if op.Specificity == parser.MatchOptional {
				ret = append(ret,
					selectOverJoinAlternatives(reqVars, selectOp, op, alt.Inputs)...)
			}

		case *plandef.SelectLit:
			ret = append(ret, search.NewExpr(
				alt.Operator,
				search.NewExpr(selectOp, alt.Inputs[0])))

		case *plandef.SelectVar:
			ret = append(ret, search.NewExpr(
				alt.Operator,
				search.NewExpr(selectOp, alt.Inputs[0])))
		}
	}
	return ret
}

// selectOverJoinAlternatives is a helper function used by pushDownSelectImpl to
// generate alternate results for a select over join operation. It will push the
// select down to any input to the join that produces all the variables required
// by the select. Returns the list of generated alternatives.
func selectOverJoinAlternatives(reqVars plandef.VarSet, selectOp search.Operator,
	joinOp search.Operator, joinInputs []*search.Group) []*search.IntoExpr {

	var ret []*search.IntoExpr
	leftVars := joinInputs[0].LogicalProp.(*logicalProperties).variables
	rightVars := joinInputs[1].LogicalProp.(*logicalProperties).variables
	pushLeft := leftVars.ContainsSet(reqVars)
	pushRight := rightVars.ContainsSet(reqVars)
	if pushLeft && pushRight {
		ret = append(ret, search.NewExpr(
			joinOp,
			search.NewExpr(selectOp, joinInputs[0]),
			search.NewExpr(selectOp, joinInputs[1]),
		))
	} else if pushLeft {
		ret = append(ret, search.NewExpr(
			joinOp,
			search.NewExpr(selectOp, joinInputs[0]),
			joinInputs[1],
		))
	} else if pushRight {
		ret = append(ret, search.NewExpr(
			joinOp,
			joinInputs[0],
			search.NewExpr(selectOp, joinInputs[1]),
		))
	}
	return ret
}

func selectionCost(expr *search.Expr, stats Stats) *estCost {
	return &estCost{
		diskBytes: 0,
		diskSeeks: 0,
	}
}

func selectLitLogicalProperties(op *plandef.SelectLit, inputs []*logicalProperties, stats Stats) *logicalProperties {
	return &logicalProperties{
		variables:  inputs[0].variables,
		resultSize: cmp.MaxInt(1, inputs[0].resultSize/2),
	}
}

func selectVarLogicalProperties(op *plandef.SelectVar, inputs []*logicalProperties, stats Stats) *logicalProperties {
	return &logicalProperties{
		variables:  inputs[0].variables,
		resultSize: cmp.MaxInt(1, inputs[0].resultSize/2),
	}
}
