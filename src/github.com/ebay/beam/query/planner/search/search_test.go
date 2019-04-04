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
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Prepare(t *testing.T) {
	expr := NewExpr(
		&testOp{str: "Join"},
		NewExpr(&testOp{str: "Scan A"}),
		NewExpr(&testOp{str: "Join"},
			NewExpr(&testOp{str: "Scan B"}),
			NewExpr(&testOp{str: "Scan C"})))

	expRule := func(e *Expr) []*IntoExpr {
		if e.String() == "Scan A" {
			return []*IntoExpr{NewExpr(&testOp{str: "Index Scan A"})}
		}
		return nil
	}
	implRule := func(e *Expr) []*IntoExpr {
		if e.String() == "Scan B" {
			return []*IntoExpr{NewExpr(&testOp{str: "Table Scan B"})}
		}
		if e.String() == "Scan C" {
			return []*IntoExpr{NewExpr(&testOp{str: "Table Scan C"})}
		}
		return nil
	}
	def := testDef{
		explore:   []ExplorationRule{{Name: "expRule", Apply: expRule}},
		implement: []ImplementationRule{{Name: "implRule", Apply: implRule}},
		mkPlanNode: func(op Operator, inputs []PlanNode, lprop LogicalProperties) PlanNode {
			return op
		},
		localCost: func(e *Expr) Cost {
			if strings.HasPrefix(e.String(), "Scan") {
				return &testCosts{cost: 100}
			}
			return &testCosts{cost: 50}
		},
		combinedCost: func(e *Expr) Cost {
			return e.LocalCost
		},
	}
	space, plan, err := Prepare(context.Background(), expr, &def, Options{CheckInvariantsAfterMajorSteps: true})
	assert.NoError(t, err)
	assert.NotNil(t, space)
	assert.Equal(t, expr.Operator, plan)
	assert.True(t, space.Contains(`
Join
	Index Scan A
	Join
		Table Scan B
		Table Scan C
`))
}
