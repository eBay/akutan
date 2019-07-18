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
	"strings"

	"github.com/ebay/akutan/query/parser"
	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/query/planner/search"
	"github.com/ebay/akutan/util/cmp"
	"github.com/sirupsen/logrus"
)

type innerJoinOperator struct {
	// Variables present on both the left- and right-hand sides.
	variables plandef.VarSet
}

func (op *innerJoinOperator) String() string {
	return "InnerJoin " + op.variables.String()
}

// Key implements cmp.Key.
func (op *innerJoinOperator) Key(b *strings.Builder) {
	b.WriteString("InnerJoin ")
	op.variables.Key(b)
}

func joinLogicalProperties(inputs []*logicalProperties, stats Stats) *logicalProperties {
	return &logicalProperties{
		variables:  inputs[0].variables.Union(inputs[1].variables),
		resultSize: joinResultSize(inputs, stats),
	}
}

func joinResultSize(inputs []*logicalProperties, stats Stats) int {
	left := inputs[0].resultSize
	right := inputs[1].resultSize
	if left < right {
		return cmp.MaxInt(1, left/2)
	}
	return cmp.MaxInt(1, right/2)
}

// loopJoinLogicalProperties exists until the workaround that is
// preImplementLeftLoopJoin can be removed. With the work around in place, the
// starting tree can have a LoopJoin operator in it already, so the planner will
// treat it as a logical operator.
func loopJoinLogicalProperties(op *plandef.LoopJoin, inputs []*logicalProperties, stats Stats) *logicalProperties {
	switch op.Specificity {
	case parser.MatchOptional:
		return leftJoinLogicalProperties(inputs, stats)
	case parser.MatchRequired:
		return joinLogicalProperties(inputs, stats)
	}
	logrus.Panicf("loopJoinLogicalProperties received Op with unknown Specificity of %v", op.Specificity)
	return nil // never gets here
}

// leftJoinOperator is a logical operator for a left join. This is like a SQL
// left outer join where the left side is always produced, and the right side is
// included if it matches. This is used to handle optional matches in the query.
type leftJoinOperator struct {
	variables plandef.VarSet
}

func (op *leftJoinOperator) String() string {
	return "LeftJoin " + op.variables.String()
}

// Key implements cmp.Key.
func (op *leftJoinOperator) Key(b *strings.Builder) {
	b.WriteString("LeftJoin ")
	op.variables.Key(b)
}

func leftJoinLogicalProperties(inputs []*logicalProperties, stats Stats) *logicalProperties {
	return &logicalProperties{
		variables:  inputs[0].variables.Union(inputs[1].variables),
		resultSize: inputs[0].resultSize,
	}
}

// flipJoin generates an alternative for a join by swapping the inputs around.
//
// 	Before: join(a, b)
// 	After:  join(b, a)
func flipJoin(root *search.Expr) []*search.IntoExpr {
	op, ok := root.Operator.(*innerJoinOperator)
	if !ok {
		return nil
	}
	return []*search.IntoExpr{
		search.NewExpr(
			&innerJoinOperator{
				variables: op.variables,
			},
			root.Inputs[1], root.Inputs[0]),
	}
}

// reorderJoin generates a different combination of joins where 2 joins
// are related.
//
// 	Before: join(join(a, b), c) where b and c have a variable in common
// 	After:  join(a, join(b, c))
func reorderJoin(root *search.Expr) []*search.IntoExpr {
	_, ok := root.Operator.(*innerJoinOperator)
	if !ok {
		return nil
	}
	var ret []*search.IntoExpr
	for _, alt := range root.Inputs[0].Exprs {
		if _, ok := alt.Operator.(*innerJoinOperator); ok {
			a := alt.Inputs[0]
			b := alt.Inputs[1]
			c := root.Inputs[1]
			aVars := a.LogicalProp.(*logicalProperties).variables
			bVars := b.LogicalProp.(*logicalProperties).variables
			cVars := c.LogicalProp.(*logicalProperties).variables
			bcJoinVars := bVars.Intersect(cVars)
			if len(bcJoinVars) == 0 {
				continue
			}
			ret = append(ret, search.NewExpr(
				&innerJoinOperator{
					variables: aVars.Intersect(bVars.Union(cVars)),
				},
				a,
				search.NewExpr(
					&innerJoinOperator{
						variables: bcJoinVars,
					},
					b, c)))
		}
	}
	return ret
}

func hashJoinCost(expr *search.Expr, stats Stats) *estCost {
	return &estCost{
		diskBytes: 0,
		diskSeeks: 0,
	}
}

// implementHashJoin provides an implementation of joinOperator using
// a physical hash Join operator.
//
// 	before : join(a,b)
// 	after  : HashJoin(a,b,MatchRequired)
func implementHashJoin(root *search.Expr) []*search.IntoExpr {
	op, ok := root.Operator.(*innerJoinOperator)
	if !ok {
		return nil
	}
	return []*search.IntoExpr{
		search.NewExpr(
			&plandef.HashJoin{
				Variables:   op.variables,
				Specificity: parser.MatchRequired,
			},
			root.Inputs[0], root.Inputs[1]),
	}
}

// implementLeftHashJoin provides an implementation of leftJoinOperator
// using a physical hash join operator.
//
// 	before: leftJoin(a,b)
// 	after:  HashJoin(a,b, MatchOptional)
func implementLeftHashJoin(root *search.Expr) []*search.IntoExpr {
	joinOp, ok := root.Operator.(*leftJoinOperator)
	if !ok {
		return nil
	}
	// leftJoin can be implemented by a HashJoin with Match set to Optional
	return []*search.IntoExpr{
		search.NewExpr(
			&plandef.HashJoin{
				Variables:   joinOp.variables,
				Specificity: parser.MatchOptional,
			},
			root.Inputs[0], root.Inputs[1]),
	}
}

func loopJoinCost(expr *search.Expr, stats Stats) *estCost {
	return &estCost{
		diskBytes: 0,
		diskSeeks: 0,
	}
}

func loopJoinCombinedCost(expr *search.Expr) *estCost {
	opCost := expr.LocalCost.(*estCost)
	leftCost := expr.Inputs[0].Best.CombinedCost.(*estCost)
	rightCost := expr.Inputs[1].Best.CombinedCost.(*estCost)
	leftProp := expr.Inputs[0].LogicalProp.(*logicalProperties)
	return &estCost{
		diskSeeks: opCost.diskSeeks + leftCost.diskSeeks + leftProp.resultSize*rightCost.diskSeeks,
		diskBytes: opCost.diskBytes + leftCost.diskBytes + leftProp.resultSize*rightCost.diskBytes,
	}
}

// implementLoopJoin implements a joinOperator using a LoopJoin physical operator
//
// 	before : join (a,b) where b is a lookup or enumeration
// 	after  : LoopJoin (a, bind(b, vars(a)), Required)
//
// bind(b, vars(a)) updates the input b by changes any variables
// available in a to bindings to that variable, for
// variables produced by 'a'
func implementLoopJoin(root *search.Expr) []*search.IntoExpr {
	joinOp, ok := root.Operator.(*innerJoinOperator)
	if !ok {
		return nil
	}
	return buildLoopJoins(root,
		&plandef.LoopJoin{
			Variables:   joinOp.variables,
			Specificity: parser.MatchRequired,
		})
}

// implementLeftLoopJoin provides an implementation of leftJoinOperator
// using a physical LoopJoin operator.
//
// 	before : leftJoin (a,b) where b is a lookup or enumerate leaf
// 	after  : LoopJoin (a, bind(b, vars(a)), Optional)
//
// bind(b, vars(a)) updates the input b by changes any variables
// available in a to bindings to that variable, for
// variables produced by 'a'
func implementLeftLoopJoin(root *search.Expr) []*search.IntoExpr {
	joinOp, ok := root.Operator.(*leftJoinOperator)
	if !ok {
		return nil
	}
	return buildLoopJoins(root,
		&plandef.LoopJoin{
			Variables:   joinOp.variables,
			Specificity: parser.MatchOptional,
		})
}

// buildLoopJoins does almost all the work for implementLoopJoin and
// implementLeftLoopJoin. 'root' is assumed to be some kind of existing join
// operator (and in particular that it has 2 inputs). 'newJoinOp' is the
// replacement LoopJoin operation. It'll build and return replacements
// consisting of newOp with the right side input having any inputs changed into
// bound versions.
func buildLoopJoins(root *search.Expr, newJoinOp *plandef.LoopJoin) []*search.IntoExpr {
	left := root.Inputs[0].LogicalProp.(*logicalProperties).variables
	var ret []*search.IntoExpr
	for _, expr := range root.Inputs[1].Exprs {
		switch op := expr.Operator.(type) {
		case *lookupOperator:
			boundLookup, _ := bindLookup(op, left)
			ret = append(ret,
				search.NewExpr(newJoinOp,
					root.Inputs[0],
					search.NewExpr(boundLookup)))

		case *plandef.Enumerate:
			boundEnum, _ := bindEnumerate(op, left)
			ret = append(ret,
				search.NewExpr(newJoinOp,
					root.Inputs[0],
					search.NewExpr(boundEnum)))
		}
	}
	return ret
}

// bindLookup will create a copy of lookup and convert any variables in it that
// are in 'vars' into bindings. The returned value is the created lookup and a
// bool indicating that at least one variable was converted to a binding.
func bindLookup(lookup *lookupOperator, vars plandef.VarSet) (*lookupOperator, bool) {
	result := *lookup
	wasBound := false
	bind := func(t *plandef.Term) {
		if v, ok := (*t).(*plandef.Variable); ok {
			if vars.Contains(v) {
				*t = &plandef.Binding{Var: v}
				wasBound = true
			}
		}
	}
	bind(&result.id)
	bind(&result.subject)
	bind(&result.predicate)
	bind(&result.object)
	return &result, wasBound
}

// bindEnumerate will return the enumerate op with any variables converted into
// bindings. If the enumerate doesn't use a variable in 'vars' then the input op
// is returned. If the variable was converted to a binding, a new enumerate op
// reflecting this is returned. In either case the inputs are not mutated. In
// addition to the op, it also returns true if the op was bound, or false
// otherwise.
func bindEnumerate(op *plandef.Enumerate, vars plandef.VarSet) (*plandef.Enumerate, bool) {
	if v, isVar := op.Output.(*plandef.Variable); isVar {
		if vars.Contains(v) {
			bound := *op
			bound.Output = &plandef.Binding{Var: v}
			return &bound, true
		}
	}
	return op, false
}
