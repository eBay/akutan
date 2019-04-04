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

package search

import (
	"fmt"
	"io"
	"strings"

	"github.com/ebay/beam/util/cmp"
)

// IntoExpr represents an expression that hasn't yet been integrated into the
// search space. It's used when the logical properties are already known, such
// as when creating an expression that's logically equivalent to another.
// IntoExpr is constructed with NewExpr.
type IntoExpr struct {
	// Logical or physical operator.
	Operator Operator
	// A list of *IntoExpr or *Group inputs.
	Inputs []intoExprInput
}

// intoExprInput is only used in defining IntoExpr.Inputs.
// The only implementations are *IntoExpr and *Group.
type intoExprInput interface {
	isIntoExprInput()
}

func (*IntoExpr) isIntoExprInput() {}
func (*Group) isIntoExprInput()    {}

// NewExpr is a convenience function for building an IntoExpr tree. Permissible
// inputs are either *Group or *IntoExpr.
func NewExpr(operator Operator, inputs ...intoExprInput) *IntoExpr {
	return &IntoExpr{
		Operator: operator,
		Inputs:   inputs,
	}
}

// String returns a multi-line indented human-readable string describing the
// new expression.
func (expr *IntoExpr) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%v\n", expr.Operator)
	for _, input := range expr.Inputs {
		printInput(input, &b, "\t")
	}
	return b.String()
}

// printInput is a helper to IntoExpr.String.
func printInput(expr intoExprInput, w io.Writer, indent string) {
	switch expr := expr.(type) {
	case *IntoExpr:
		fmt.Fprintf(w, "%v%v\n", indent, expr.Operator)
		for _, input := range expr.Inputs {
			printInput(input, w, indent+"\t")
		}
	case *Group:
		fmt.Fprintf(w, "%vGroup %v\n", indent, expr.ID)
	}
}

// Insert will integrate a new expression tree into the plan. The expression at
// the root of 'newExpr' must be logically equivalent to the existing 'from'
// expression. Note that, because of de-duping, the expressions in the newExpr
// tree might not actually add anything new to the plan. Also, the group that
// 'from' was in might merge with another group. Returns the existing or new
// expression in the plan that is identical to 'newExpr'. Exprs obtained from the
// memo before this call is executed may be made stale. They must be checked
// for staleness before continuing to use them after this function returns.
func (space *Space) insertEquivalent(newExpr *IntoExpr, from *Expr) *Expr {
	if space.options.Invariants != nil {
		defer space.options.Invariants(space, space.root)
	}
	if space.options.CheckInternalInvariants {
		defer space.MustCheckInvariants()
	}
	if from.isStale() {
		panic(fmt.Sprintf("insertEquivalent called with stale expr: id:%d ex:%v reason:%s",
			from.id, from, from.staleReason))
	}
	if from.Group.mergedInto != nil {
		panic(fmt.Sprintf("insertEquivalent called with expr from a merged group, expr:%d %v group:%d merged to %d. Expr should of been flagged as stale",
			from.id, from, from.Group.ID, from.Group.mergedInto.ID))
	}
	expr := space.insertInputs(newExpr)
	expr.Group = from.Group

	key := cmp.GetKey(expr)
	existing, found := space.exprHash[key]
	if found {
		if expr.Group != existing.Group {
			space.mergeGroups(expr.Group, existing.Group)
		}
		return existing
	}
	space.exprHash[key] = expr
	expr.Group.Exprs = append(expr.Group.Exprs, expr)
	return expr
}

// insertInputs integrates the inputs of newExpr into the space.
// Returns an Expr that itself is not yet part of the space.
func (space *Space) insertInputs(newExpr *IntoExpr) *Expr {
	expr := &Expr{
		Operator: newExpr.Operator,
		Inputs:   make([]*Group, 0, len(newExpr.Inputs)),
		id:       space.nextExprID,
	}
	space.nextExprID++
	for _, input := range newExpr.Inputs {
		var group *Group
		switch input := input.(type) {
		case *IntoExpr:
			group = space.insertAnywhere(input).Group
		case *Group:
			group = input
			for group.mergedInto != nil {
				group = group.mergedInto
			}
		}
		expr.Inputs = append(expr.Inputs, group)
	}
	return expr
}

// insert finds newExpr in an existing group or creates a new group for it.
func (space *Space) insertAnywhere(newExpr *IntoExpr) *Expr {
	expr := space.insertInputs(newExpr)
	key := cmp.GetKey(expr)
	existing, found := space.exprHash[key]
	if found {
		return existing
	}
	space.exprHash[key] = expr
	expr.Group = &Group{
		ID:    space.nextGroupID,
		Exprs: []*Expr{expr},
	}
	space.nextGroupID++
	inputs := make([]LogicalProperties, len(expr.Inputs))
	for i := range expr.Inputs {
		inputs[i] = expr.Inputs[i].LogicalProp
	}
	expr.Group.LogicalProp = space.def.LogicalProperties(expr.Operator, inputs)
	return expr
}

// mergeGroups is called when 'from' and 'into' are found to be equivalent.
// After mergeGroups returns, 'from' will be empty (its expressions will be part
// of 'into'), and any other group using 'from' as input will be updated. This
// results in affected Exprs to be marked stale. They are then replaced by new
// Expr instances in the memo.
func (space *Space) mergeGroups(from *Group, into *Group) {
	// Copy expressions to new group
	staleReason := fmt.Sprintf("Group %d was merged to %d", from.ID, into.ID)
	for _, expr := range from.Exprs {
		copy := &Expr{
			Operator: expr.Operator,
			Inputs:   expr.Inputs,
			Group:    into,
			id:       expr.id,
		}
		into.Exprs = append(into.Exprs, copy)
		space.exprHash[cmp.GetKey(expr)] = copy
		expr.staleReason = staleReason
	}
	from.Exprs = nil
	from.mergedInto = into

	// Update references in affected groups.
	// The affected groups cannot precede 'from' in the topological sort.
	groups := space.groups()
	for i := 0; i < len(groups); i++ {
		if groups[i] == from {
			groups = groups[i+1:]
			break
		}
	}
	for _, group := range groups {
		groupAffected := false
		for _, expr := range group.Exprs {
			if expr.hasInput(from.ID) {
				groupAffected = true
				break
			}
		}
		if !groupAffected {
			continue
		}
		oldExprs := group.Exprs
		group.Exprs = nil
		for _, expr := range oldExprs {
			delete(space.exprHash, cmp.GetKey(expr))
		}
		for _, expr := range oldExprs {
			// we only need to update exprs that reference the merged
			// group, there may be exprs in this group that don't reference
			// the merged group, and they can be left as is.
			newExpr := expr
			if expr.hasInput(from.ID) {
				expr.staleReason = staleReason
				newExpr = &Expr{
					Group:    group,
					Operator: expr.Operator,
					Inputs:   make([]*Group, len(expr.Inputs)),
					id:       space.nextExprID,
				}
				space.nextExprID++
				for i, input := range expr.Inputs {
					if input.ID == from.ID {
						newExpr.Inputs[i] = into
					} else {
						newExpr.Inputs[i] = input
					}
				}
			}
			key := cmp.GetKey(newExpr)
			_, found := space.exprHash[key]
			if !found {
				space.exprHash[key] = newExpr
				group.Exprs = append(group.Exprs, newExpr)
			}
		}
	}
}
