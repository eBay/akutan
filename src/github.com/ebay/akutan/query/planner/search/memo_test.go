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
	"strings"
	"testing"

	"github.com/ebay/akutan/util/cmp"
	"github.com/stretchr/testify/assert"
)

func NewTestPlan(t *testing.T, expr *IntoExpr) *Space {
	plan := NewSpace(expr, &testDef{}, Options{
		CheckInternalInvariants: true,
	})
	assert.NoError(t, plan.CheckInvariants())
	return plan
}

type testDef struct {
	explore      []ExplorationRule
	implement    []ImplementationRule
	localCost    func(*Expr) Cost
	combinedCost func(*Expr) Cost
	mkPlanNode   func(op Operator, inputs []PlanNode, lprop LogicalProperties) PlanNode
}

func (def *testDef) ExplorationRules() []ExplorationRule {
	return def.explore
}

func (def *testDef) ImplementationRules() []ImplementationRule {
	return def.implement
}

func (def *testDef) LocalCost(expr *Expr) Cost {
	if def.localCost != nil {
		return def.localCost(expr)
	}
	return nil
}

func (def *testDef) CombinedCost(expr *Expr) Cost {
	if def.combinedCost != nil {
		return def.combinedCost(expr)
	}
	return nil
}

type testCosts struct {
	cost int
}

func (t *testCosts) String() string {
	return fmt.Sprintf("seeks %d", t.cost)
}

func (t *testCosts) Infinite() bool {
	return t.cost > 1000
}

func (t *testCosts) Less(other Cost) bool {
	return t.cost < other.(*testCosts).cost
}

type testLProps struct {
}

func (lprop *testLProps) String() string {
	return ""
}

func (lprop *testLProps) DetailString() string {
	return ""
}

func (def *testDef) LogicalProperties(op Operator, inputs []LogicalProperties) LogicalProperties {
	return new(testLProps)
}

func (def *testDef) MakePlanNode(op Operator, inputs []PlanNode, lprop LogicalProperties) PlanNode {
	if def.mkPlanNode != nil {
		return def.mkPlanNode(op, inputs, lprop)
	}
	return nil
}

type testOp struct {
	str string
}

func (op *testOp) String() string {
	var _ Operator = op
	return op.str
}

func (op *testOp) Key(b *strings.Builder) {
	b.WriteString("key:")
	b.WriteString(op.String())
}

func NewTestPlan1(t *testing.T) *Space {
	return NewTestPlan(t,
		NewExpr(&testOp{str: "Join"},
			NewExpr(&testOp{str: "Scan A"}),
			NewExpr(&testOp{str: "Join"},
				NewExpr(&testOp{str: "Scan A"}),
				NewExpr(&testOp{str: "Scan B"}))))
}

func TestPlan_Groups(t *testing.T) {
	assert := assert.New(t)
	plan := NewTestPlan1(t)
	var str string
	for _, group := range plan.groups() {
		str += fmt.Sprintf("(%v) %v ", group.ID, group.Exprs[0])
	}
	assert.Equal("(1) Scan A (2) Scan B (3) Join [1 2] (4) Join [1 3] ", str)
}

func TestPlan_String(t *testing.T) {
	assert := assert.New(t)
	plan := NewTestPlan1(t)
	assert.Equal(`
Group 1 []
	Scan A
Group 2 []
	Scan B
Group 3 []
	Join [1 2]
Group 4 []
	Join [1 3]
`, "\n"+plan.String())
}

func TestPlan_Graphviz(t *testing.T) {
	assert := assert.New(t)
	plan := NewTestPlan1(t)
	var buf strings.Builder
	plan.Graphviz(&buf)
	str := buf.String()
	assert.Contains(str, "Join [1 3]")
	// Not sure how much we can test here without having a dot parser.
}

func Test_leftSpace(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("", leftSpace(""))
	assert.Equal("", leftSpace("hi"))
	assert.Equal("\t \t", leftSpace("\t \t"))
	assert.Equal("\t \t", leftSpace("\t \thi"))
}

func (node *patternTree) LispString() string {
	var inputs string
	for _, input := range node.inputs {
		inputs += " " + input.LispString()
	}
	return fmt.Sprintf("(%v%v)", node.op, inputs)
}

func Test_parsePattern(t *testing.T) {
	assert := assert.New(t)
	tree := parsePattern(`
		Join
			Scan A
			StupidJoin
				Scan B
				Scan C
			Select
				Scan D
	`)
	assert.Equal("(Join (Scan A) (StupidJoin (Scan B) (Scan C)) (Select (Scan D)))",
		tree.LispString())
	assert.Panics(func() {
		parsePattern(`
		Join
		Join
	`)
	})
}

func Test_matches(t *testing.T) {
	assert := assert.New(t)
	plan := NewTestPlan(t,
		NewExpr(&testOp{str: "Join"},
			NewExpr(&testOp{str: "Scan A"}),
			NewExpr(&testOp{str: "Scan B"})))
	join := plan.root.Exprs[0]
	scana := join.Inputs[0].Exprs[0]
	scanb := join.Inputs[1].Exprs[0]
	assert.True(matches(join, "Join", []int{scana.Group.ID, scanb.Group.ID}))
	assert.False(matches(join, "Join", []int{scana.Group.ID, scanb.Group.ID, 1}))
	assert.False(matches(join, "Join", []int{scana.Group.ID}))
	assert.False(matches(join, "Join", []int{scanb.Group.ID, scana.Group.ID}))
	assert.False(matches(join, "Join ", []int{scana.Group.ID, scanb.Group.ID}))
	assert.True(matches(scana, "Scan A", []int{}))
	assert.False(matches(scana, "Scan A", []int{1}))
}

func TestPlan_Contains(t *testing.T) {
	assert := assert.New(t)
	space := NewTestPlan1(t)
	assert.False(space.Contains(`
		Join
			Scan B
			Scan A
	`))
	assert.True(space.Contains(`
		Join
			Scan A
			Scan B
	`))
}

func TestExpr_Key_and_String(t *testing.T) {
	assert := assert.New(t)
	plan := NewTestPlan1(t)
	var str string
	for _, group := range plan.groups() {
		str += fmt.Sprintf("(%v) -%v- -%v-\n",
			group.ID,
			cmp.GetKey(group.Exprs[0]),
			group.Exprs[0].String())
	}
	assert.Equal(strings.TrimSpace(`
(1) -key:Scan A- -Scan A-
(2) -key:Scan B- -Scan B-
(3) -key:Join [1 2]- -Join [1 2]-
(4) -key:Join [1 3]- -Join [1 3]-
		`), strings.TrimSpace(str))
}

func TestExpr_HasInput(t *testing.T) {
	assert := assert.New(t)
	g1 := &Group{ID: 1}
	g2 := &Group{ID: 2}
	test := func(groupID int, expResult bool, exprHasGroups ...*Group) {
		expr := &Expr{Inputs: exprHasGroups}
		assert.Equal(expResult, expr.hasInput(groupID))
	}
	test(1, true, g1)
	test(1, true, g1, g2)
	test(2, true, g1, g2)
	test(3, false, g1, g2)
	test(2, false, g1)
	test(1, false)
}

func Test_Debug(t *testing.T) {
	expr := NewExpr(&testOp{str: "Join"},
		NewExpr(&testOp{str: "Join"},
			NewExpr(&testOp{str: "Scan A"}),
			NewExpr(&testOp{str: "Scan C"})),
		NewExpr(&testOp{str: "Scan B"}))
	def := testDef{
		explore: []ExplorationRule{{
			Name: "LoopJoin",
			Apply: func(e *Expr) []*IntoExpr {
				if e.Operator.String() == "Join" {
					alt := NewExpr(&testOp{str: "LoopJoin"},
						e.Inputs[0],
						NewExpr(&testOp{str: "Bind"}, e.Inputs[1]))
					return []*IntoExpr{alt}
				}
				return nil
			},
		}},
		localCost: func(e *Expr) Cost {
			switch e.Operator.String() {
			case "Scan A":
				return &testCosts{cost: 10}
			case "Scan B":
				return &testCosts{cost: 20}
			case "Scan C":
				return &testCosts{cost: 30}
			case "Join":
				return &testCosts{cost: 2}
			default:
				return &testCosts{cost: 0}
			}
		},
		combinedCost: func(e *Expr) Cost {
			total := *e.LocalCost.(*testCosts)
			for _, i := range e.Inputs {
				total.cost += i.Best.CombinedCost.(*testCosts).cost
			}
			return &total
		},
	}
	space := NewSpace(expr, &def, Options{})
	space.Explore()
	space.Implement()
	space.PredictCosts()
	space.BestPlan()
	act := strings.Builder{}
	space.DebugCostedBest(&act)
	t.Run("DebugCostedBest", func(t *testing.T) {
		assert.Equal(t, `
LoopJoin            costs local seeks 0 combined seeks 60 logicalProps: 
    LoopJoin        costs local seeks 0 combined seeks 40 logicalProps: 
        Scan A      costs local seeks 10 combined seeks 10 logicalProps: 
        Bind        costs local seeks 0 combined seeks 30 logicalProps: 
            Scan C  costs local seeks 30 combined seeks 30 logicalProps: 
    Bind            costs local seeks 0 combined seeks 20 logicalProps: 
        Scan B      costs local seeks 20 combined seeks 20 logicalProps: 
`, "\n"+act.String())
	})

	t.Run("Debug", func(t *testing.T) {
		act := strings.Builder{}
		space.Debug(&act)
		assert.Equal(t, `
Group 5 []
	Join [3 4]                       costs local seeks 2 combined seeks 62
	LoopJoin [3 7]                   costs local seeks 0 combined seeks 60 [best,selected]
Group 7 []
	Bind [4]                         costs local seeks 0 combined seeks 20 [best,selected]
Group 4 []
	Scan B                           costs local seeks 20 combined seeks 20 [best,selected]
Group 3 []
	Join [1 2]                       costs local seeks 2 combined seeks 42
	LoopJoin [1 6]                   costs local seeks 0 combined seeks 40 [best,selected]
Group 6 []
	Bind [2]                         costs local seeks 0 combined seeks 30 [best,selected]
Group 2 []
	Scan C                           costs local seeks 30 combined seeks 30 [best,selected]
Group 1 []
	Scan A                           costs local seeks 10 combined seeks 10 [best,selected]
`, "\n"+act.String())
	})
}
