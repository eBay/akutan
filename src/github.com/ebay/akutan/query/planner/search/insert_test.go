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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntoExpr_String(t *testing.T) {
	assert := assert.New(t)
	expr := NewExpr(&testOp{str: "Join"},
		NewExpr(&testOp{str: "Join"},
			NewExpr(&testOp{str: "Scan A"}),  // Existing group (1)
			NewExpr(&testOp{str: "Scan C"})), // New group (5)
		&Group{ID: 5})
	assert.Equal(`
Join
	Join
		Scan A
		Scan C
	Group 5
`, "\n"+expr.String())
}

// Note: these tests lean heavily on checkInvariants(), so they appear light on
// assertions.

// Tests a new expression equivalent to an existing one.
func TestPlan_Insert_basic(t *testing.T) {
	assert := assert.New(t)
	plan := NewTestPlan1(t)
	expr := plan.insertEquivalent(NewExpr(&testOp{str: "root-equiv"}),
		plan.root.Exprs[0])
	assert.Equal("root-equiv", expr.String())
	assert.Equal(plan.root, expr.Group)
}

// Tests a proposed expression that is a perfect duplicate of an existing
// expression.
func TestPlan_Insert_duplicate(t *testing.T) {
	assert := assert.New(t)
	plan := NewTestPlan1(t)
	equiv := plan.groups()[2].Exprs[0]
	expr := plan.insertEquivalent(
		NewExpr(&testOp{str: "Join"},
			NewExpr(&testOp{str: "Scan A"}),
			NewExpr(&testOp{str: "Scan B"})),
		equiv)
	assert.Equal("Join [1 2]", expr.String())
	assert.Equal(equiv, expr)
	assert.Len(equiv.Group.Exprs, 1)
}

// Tests a new expression with one new input and two existing inputs (one hashes
// the same, the other is a group).
func TestPlan_Insert_inputs(t *testing.T) {
	assert := assert.New(t)
	plan := NewTestPlan1(t)
	expr := plan.insertEquivalent(
		NewExpr(&testOp{str: "Join"},
			NewExpr(&testOp{str: "Scan A"}), // Existing group (1)
			NewExpr(&testOp{str: "Scan C"}), // New group (5)
			plan.groups()[1]),               // Existing group (2)
		plan.root.Exprs[0])
	assert.Equal("Join [1 5 2]", expr.String())
	assert.Equal(plan.root, expr.Group)
	assert.Len(plan.groups(), 5)
	assert.Len(expr.Inputs[1].Exprs, 1)
	assert.Equal("Scan A", expr.Inputs[0].Exprs[0].String())
	assert.Equal("Scan C", expr.Inputs[1].Exprs[0].String())
	assert.Equal("Scan B", expr.Inputs[2].Exprs[0].String())
}

func TestPlan_Insert_merge(t *testing.T) {
	// Setup.
	assert := assert.New(t)
	plan := NewTestPlan1(t)
	group1 := plan.root.Exprs[0].Inputs[0]
	group3 := plan.root.Exprs[0].Inputs[1]
	expr := plan.insertEquivalent(
		NewExpr(&testOp{str: "Join"}, // New group 6
			NewExpr(&testOp{str: "Index A"}), // New group 5
			NewExpr(&testOp{str: "Scan B"})), // Existing group 2
		group3.Exprs[0])
	assert.Equal(`
Group 1 []
	Scan A
Group 2 []
	Scan B
Group 5 []
	Index A
Group 3 []
	Join [1 2]
	Join [5 2]
Group 4 []
	Join [1 3]
`, "\n"+plan.String())
	group5 := expr.Inputs[0]
	indexa := group5.Exprs[0]
	assert.Equal("Index A", indexa.String())

	// Merge (5) Index A into (1) Scan A.
	plan.insertEquivalent(NewExpr(&testOp{str: "Scan A"}), indexa)
	assert.Equal(`
Group 1 []
	Scan A
	Index A
Group 2 []
	Scan B
Group 3 []
	Join [1 2]
Group 4 []
	Join [1 3]
`, "\n"+plan.String())

	// Now group5 has mergedInto pointing to group 1. Test that it's still be safe to
	// reference group5 in NewExpr.
	assert.Equal(group1, group5.mergedInto)
	select5 := plan.insertEquivalent(
		NewExpr(&testOp{str: "Select"}, group5),
		plan.root.Exprs[0])
	assert.Equal("Select [1]", select5.String())
}

func Test_Explore_Bug283_Cause(t *testing.T) {
	assert := assert.New(t)
	ex := func(op string, inputs ...intoExprInput) *IntoExpr {
		return NewExpr(&testOp{op}, inputs...)
	}
	start := ex("select", ex("join", ex("a"), ex("join", ex("b"), ex("join", ex("c"), ex("d")))))
	ruleJoin := func(r *Expr) []*IntoExpr {
		var ret []*IntoExpr
		if r.Operator.String() == "join" {
			ret = append(ret,
				ex("join", r.Inputs[1], r.Inputs[0]),
				ex("hashJoin", r.Inputs[0], r.Inputs[1]))

			for _, e := range r.Inputs[1].Exprs {
				if len(e.Operator.String()) == 1 {
					// in the real rules this emits a loopJoin. But changing this to
					// loop join prevents the test from failing. The test scenario is very
					// tricky, you have to have the mergeGroups called to affect the stack
					// in Explore, and then the subsequent usage of the stale exprs need
					// to affect the space in an invalid way (e.g. by bringing an already
					// merged group back to life).
					ret = append(ret, ex("join", r.Inputs[0], ex("$"+e.Operator.String())))
				}
			}
		}
		return ret
	}
	ruleReorder := func(r *Expr) []*IntoExpr {
		var ret []*IntoExpr
		if r.Operator.String() == "join" {
			for _, e := range r.Inputs[0].Exprs {
				if e.Operator.String() == "join" {
					alt := ex("join", e.Inputs[0], ex("join", r.Inputs[1], e.Inputs[1]))
					ret = append(ret, alt)
				}
			}
		}
		return ret
	}
	ruleSelect := func(r *Expr) []*IntoExpr {
		var ret []*IntoExpr
		if r.Operator.String() == "select" {
			for _, e := range r.Inputs[0].Exprs {
				if e.Operator.String() == "join" {
					alt := ex("join", ex("select", e.Inputs[0]), e.Inputs[1])
					ret = append(ret, alt)
				}
			}
		}
		return ret
	}
	spaceDef := testDef{
		explore: []ExplorationRule{
			{Name: "ruleSelect", Apply: ruleSelect},
			{Name: "ruleJoin", Apply: ruleJoin},
			{Name: "reorder", Apply: ruleReorder},
		},
	}
	plan := NewSpace(start, &spaceDef, Options{CheckInternalInvariants: true})
	assert.NoError(plan.CheckInvariants())
	assert.NotPanics(plan.Explore)
}
