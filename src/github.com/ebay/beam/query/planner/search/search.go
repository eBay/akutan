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

// Package search implements a generic query optimizer algorithm. It's loosely
// based on the Volcano/Cascades/Columbia line of optimizer frameworks. These
// are rule-driven top-down optimizers that use dynamic programming to avoid
// duplicating effort. This package takes the set of rules (and various other
// things) as input, and applies them to look for a low-cost implementation plan
// for a query.
package search

import (
	"context"
	"fmt"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
)

// Prepare finds the best plan for the given query. The selected plan as well as
// the search space that generated it are returned. The search space is returned
// in error cases as well to allow the caller further introspection to the
// problem if needed.
//
// This is a high-level interface to the query planner; for more control see
// Space. The supplied context is used to generate tracing spans and to allow
// for the planning operation to be canceled before it is completed. There is no
// I/O performed by this function.
func Prepare(ctx context.Context, expr *IntoExpr, def Definition, options Options) (*Space, PlanNode, error) {
	space := NewSpace(expr, def, options)
	execStep := func(name string, step func()) {
		span, cctx := opentracing.StartSpanFromContext(ctx, name)
		step()
		if options.CheckInvariantsAfterMajorSteps {
			cispan, _ := opentracing.StartSpanFromContext(cctx, "CheckInvariants")
			space.MustCheckInvariants()
			cispan.Finish()
		}
		span.Finish()
	}
	execStep("Explore Plan", space.Explore)
	if ctx.Err() != nil {
		return space, nil, ctx.Err()
	}
	execStep("Implement Plan", space.Implement)
	if ctx.Err() != nil {
		return space, nil, ctx.Err()
	}
	execStep("Predict Plan Costs", space.PredictCosts)
	if ctx.Err() != nil {
		return space, nil, ctx.Err()
	}

	// BestPlan doesn't change the space, so no need to re-check the invariants
	span, _ := opentracing.StartSpanFromContext(ctx, "BestPlan")
	res, err := space.BestPlan()
	span.Finish()
	if err != nil {
		return space, nil, err
	}
	return space, res, nil
}

// Options are configuration settings for the Space.
type Options struct {
	// Used in testing: If set, checks the integrity of the Space's internal
	// structure after changes, and panics on errors.
	CheckInternalInvariants bool
	// Used in testing: If set, called to test the validity of expressions in the
	// Space.
	Invariants func(space *Space, root *Group)
	// If set Prepare will check the invariants after each of the major steps, Explore
	// Implement, PredictCosts.
	CheckInvariantsAfterMajorSteps bool
}

// NewSpace creates a search space from the given logical expression tree.
func NewSpace(expr *IntoExpr, def Definition, options Options) *Space {
	space := &Space{
		exprHash:    make(map[string]*Expr),
		nextExprID:  1,
		nextGroupID: 1,
		options:     options,
		def:         def,
	}
	space.root = space.insertAnywhere(expr).Group
	if space.options.Invariants != nil {
		space.options.Invariants(space, space.root)
	}
	return space
}

// Explore looks for equivalent logical plans that would give the same results
// as the original query. One of these new logical plans may turn out to yield a
// lower-cost execution plan.
func (space *Space) Explore() {
	explored := make(map[int]struct{})
	steps := 0

	var exploreExpr func(expr *Expr)
	exploreGroup := func(group *Group) {
		for _, expr := range group.Exprs {
			exploreExpr(expr)
		}
	}
	exploreExpr = func(expr *Expr) {
		_, found := explored[expr.id]
		if found || expr.isStale() {
			return
		}
		explored[expr.id] = struct{}{}
		for _, input := range expr.Inputs {
			exploreGroup(input)
			if expr.isStale() {
				return
			}
		}
		for _, rule := range space.def.ExplorationRules() {
			steps++
			newExprs := rule.Apply(expr)
			for _, newExpr := range newExprs {
				maybeAdded := space.insertEquivalent(newExpr, expr)
				exploreExpr(maybeAdded)
				// If expr becomes stale, then we have to skip the rest of the
				// alternatives. In that event the resulting space may not have
				// been fully explored. This is a limitation of the current
				// recursive structure of Explore()
				if expr.isStale() {
					return
				}
			}
		}
	}

	start := time.Now()
	exploreGroup(space.root)
	elapsed := time.Since(start)
	log.Printf("explore tried to apply rules %v times and took %v", steps, elapsed)
}

// Implement chooses physical execution operators to implement the logical
// operators in the plan space. Note that some logical operators may not be
// implementable; those will be left out of all execution plans.
func (space *Space) Implement() {
	implemented := make(map[*Group]struct{})

	var implementExpr func(expr *Expr)
	implementGroup := func(group *Group) {
		_, found := implemented[group]
		if found {
			return
		}
		implemented[group] = struct{}{}
		for _, expr := range group.Exprs {
			implementExpr(expr)
		}
	}
	implementExpr = func(expr *Expr) {
		for _, input := range expr.Inputs {
			implementGroup(input)
		}
		for _, rule := range space.def.ImplementationRules() {
			newExprs := rule.Apply(expr)
			for _, newExpr := range newExprs {
				space.insertEquivalent(newExpr, expr)
			}
		}
	}
	implementGroup(space.root)
}

// PredictCosts assigns estimated costs to the possible physical execution plans.
func (space *Space) PredictCosts() {
	for _, group := range space.groups() {
		group.Best = nil
	exprs:
		for _, expr := range group.Exprs {
			expr.LocalCost = space.def.LocalCost(expr)
			for _, input := range expr.Inputs {
				if input.Best == nil {
					expr.CombinedCost = input.Exprs[0].CombinedCost
					if !expr.CombinedCost.Infinite() {
						log.Panicf("Expected infinite cost, found %v", expr.CombinedCost)
					}
					continue exprs
				}
			}
			cost := space.def.CombinedCost(expr)
			expr.CombinedCost = cost
			if !cost.Infinite() && (group.Best == nil || cost.Less(group.Best.CombinedCost)) {
				group.Best = expr
			}
		}
	}
}

// BestPlan returns an execution plan with the lowest cost. If multiple plans
// have the same cost, it returns one arbitrarily. Returns an error if no suitable
// execution plan has been found.
func (space *Space) BestPlan() (PlanNode, error) {
	var do func(*Group) (PlanNode, error)
	do = func(root *Group) (PlanNode, error) {
		if root.Best == nil {
			return nil, fmt.Errorf("no physical plan found (or missing/infinite cost predictions)")
		}
		inputs := make([]PlanNode, len(root.Best.Inputs))
		for i, input := range root.Best.Inputs {
			var err error
			inputs[i], err = do(input)
			if err != nil {
				return nil, err
			}
		}
		return space.def.MakePlanNode(
			root.Best.Operator,
			inputs,
			root.LogicalProp,
		), nil
	}
	return do(space.root)
}
