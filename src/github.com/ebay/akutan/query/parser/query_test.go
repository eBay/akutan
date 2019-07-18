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

package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_QueryTypeStringer(t *testing.T) {
	assert.Equal(t, "LEGACY", LegacyPatternQuery.String())
	assert.Equal(t, "SELECT", SelectQuery.String())
	assert.Equal(t, "ASK", AskQuery.String())
	assert.Equal(t, "Unknown QueryType (42)", QueryType(42).String())
}

func Test_SelectClauseStringer(t *testing.T) {
	sel := SelectClause{
		Items: []selectClauseItem{
			&Variable{Name: "name"},
			&BoundExpression{Expr: &AggregateExpr{Function: AggCount, Of: Wildcard{}}, As: &Variable{Name: "num"}},
		},
	}
	assert.Equal(t, "?name (COUNT(*) AS ?num)", sel.String())

	sel = SelectClause{
		Keyword: Distinct{},
		Items: []selectClauseItem{
			&Variable{Name: "name"},
			&BoundExpression{Expr: &AggregateExpr{Function: AggCount, Of: Wildcard{}}, As: &Variable{Name: "num"}},
		},
	}
	assert.Equal(t, "DISTINCT ?name (COUNT(*) AS ?num)", sel.String())
}

func Test_BoundExpressionStringer(t *testing.T) {
	a := BoundExpression{Expr: &AggregateExpr{Function: AggCount, Of: &Variable{Name: "v"}}, As: &Variable{Name: "out"}}
	assert.Equal(t, "(COUNT(?v) AS ?out)", a.String())
}
func Test_AggregateExprStringer(t *testing.T) {
	a := AggregateExpr{Function: AggCount, Of: &Variable{Name: "name"}}
	assert.Equal(t, "COUNT(?name)", a.String())

	a = AggregateExpr{Function: AggCount, Of: Wildcard{}}
	assert.Equal(t, "COUNT(*)", a.String())
}

func Test_AggregateFunctionStringer(t *testing.T) {
	assert.Equal(t, "COUNT", AggCount.String())
	assert.Equal(t, "Unknown AggregateFunction (42)", AggregateFunction(42).String())
}

func Test_SolutionModifierStringer(t *testing.T) {
	v1 := &Variable{Name: "name"}
	v2 := &Variable{Name: "make"}
	sm := SolutionModifier{
		OrderBy: []OrderCondition{
			{On: v1, Direction: SortAsc},
			{On: v2, Direction: SortDesc},
		},
	}
	assert.Equal(t, "ORDER BY ASC(?name) DESC(?make)", sm.String())

	sm = SolutionModifier{
		OrderBy: []OrderCondition{{On: v2, Direction: SortAsc}},
		Paging: LimitOffset{
			Limit:  uint64p(10),
			Offset: uint64p(20),
		},
	}
	assert.Equal(t, "ORDER BY ASC(?make)\nLIMIT 10 OFFSET 20", sm.String())

	sm = SolutionModifier{
		Paging: LimitOffset{
			Limit:  uint64p(10),
			Offset: uint64p(20),
		},
	}
	assert.Equal(t, "LIMIT 10 OFFSET 20", sm.String())
}

func Test_OrderConditionStringer(t *testing.T) {
	v := &Variable{Name: "name"}
	assert.Equal(t, "ASC(?name)", (&OrderCondition{On: v, Direction: SortAsc}).String())
	assert.Equal(t, "DESC(?name)", (&OrderCondition{On: v, Direction: SortDesc}).String())
}

func Test_SortDirectionStringer(t *testing.T) {
	assert.Equal(t, "ASC", SortAsc.String())
	assert.Equal(t, "DESC", SortDesc.String())
	assert.Equal(t, "Unknown Direction (42)", SortDirection(42).String())
}

func Test_LimitOffsetStringer(t *testing.T) {
	assert.Equal(t, "", new(LimitOffset).String())
	assert.Equal(t, "LIMIT 42", (&LimitOffset{Limit: uint64p(42)}).String())
	assert.Equal(t, "OFFSET 10", (&LimitOffset{Offset: uint64p(10)}).String())
	assert.Equal(t, "LIMIT 42 OFFSET 10", (&LimitOffset{Limit: uint64p(42), Offset: uint64p(10)}).String())
}

// uint64p is a stupid helper to deal with creating pointers to uint64s in
// test literals.
func uint64p(v uint64) *uint64 {
	return &v
}
