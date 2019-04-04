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

package plandef

import (
	"testing"

	"github.com/ebay/beam/query/parser"
	"github.com/ebay/beam/util/cmp"
	"github.com/stretchr/testify/assert"
)

func Test_ProjectionStringer(t *testing.T) {
	varS := &Variable{Name: "s"}
	varX := &Variable{Name: "x"}
	p := Projection{
		Select: []ExprBinding{
			{Expr: varS, Out: varS}, {Expr: varX, Out: varX},
		},
	}
	assert.Equal(t, "Project ?s ?x", p.String())
	assert.Equal(t, "Project bind(?s ?s) bind(?x ?x)", cmp.GetKey(&p))
	p = Projection{
		Select: []ExprBinding{{
			Expr: &AggregateExpr{Func: parser.AggCount, Of: WildcardExpr{}},
			Out:  &Variable{Name: "postcards"},
		}},
	}
	assert.Equal(t, "Project (COUNT(*) AS ?postcards)", p.String())
	assert.Equal(t, "Project bind(COUNT(*) ?postcards)", cmp.GetKey(&p))
}

func Test_AskStringer(t *testing.T) {
	op := &Ask{Out: &Variable{Name: "result"}}
	assert.Equal(t, "Ask ?result", op.String())
	assert.Equal(t, "Ask ?result", cmp.GetKey(op))
}

func Test_ExtIDsStringer(t *testing.T) {
	e := &ExternalIDs{}
	assert.Equal(t, "ExternalIDs", e.String())
	assert.Equal(t, "ExternalIDs", cmp.GetKey(e))
}

func Test_OrderByOpStringer(t *testing.T) {
	o := &OrderByOp{
		OrderBy: []OrderCondition{
			{
				Direction: SortAsc,
				On:        &Variable{Name: "x"},
			},
			{
				Direction: SortDesc,
				On:        &Variable{Name: "y"},
			},
		},
	}
	assert.Equal(t, "OrderBy ASC(?x) DESC(?y)", o.String())
	assert.Equal(t, "OrderBy ASC(?x) DESC(?y)", cmp.GetKey(o))
}

func Test_LimitAndOffsetOpStringer(t *testing.T) {
	limit := uint64(50)
	offset := uint64(0)

	o := &LimitAndOffsetOp{
		Paging: LimitOffset{
			Limit:  &limit,
			Offset: &offset,
		},
	}
	assert.Equal(t, "LimitOffset (Lmt 50 Off 0)", o.String())
	assert.Equal(t, "LimitOffset (Lmt 50 Off 0)", cmp.GetKey(o))

	o = &LimitAndOffsetOp{
		Paging: LimitOffset{
			Limit: &limit,
		},
	}
	assert.Equal(t, "LimitOffset (Lmt 50)", o.String())
	assert.Equal(t, "LimitOffset (Lmt 50)", cmp.GetKey(o))

	o = &LimitAndOffsetOp{
		Paging: LimitOffset{
			Offset: &offset,
		},
	}
	assert.Equal(t, "LimitOffset (Off 0)", o.String())
	assert.Equal(t, "LimitOffset (Off 0)", cmp.GetKey(o))
}

func Test_DistinctStringer(t *testing.T) {
	e := &DistinctOp{}
	assert.Equal(t, "Distinct", e.String())
	assert.Equal(t, "Distinct", cmp.GetKey(e))
}
