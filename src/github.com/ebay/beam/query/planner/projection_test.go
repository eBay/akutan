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
	"testing"

	"github.com/ebay/beam/query/parser"
	"github.com/ebay/beam/query/planner/plandef"
	"github.com/stretchr/testify/assert"
)

func Test_ProjectionProperties(t *testing.T) {
	product := &plandef.Variable{Name: "product"}
	products := &plandef.Variable{Name: "products"}
	price := &plandef.Variable{Name: "price"}

	inputs := []*logicalProperties{{
		variables:  plandef.VarSet{price, product},
		resultSize: 100,
	}}
	op := &plandef.Projection{
		Select:    []plandef.ExprBinding{{Expr: product, Out: product}},
		Variables: plandef.VarSet{product},
	}
	act := projectionLogicalProperties(op, inputs, nil)
	exp := &logicalProperties{
		variables:  plandef.VarSet{product},
		resultSize: 100,
	}
	assert.Equal(t, exp, act)

	aggOp := &plandef.Projection{
		Select: []plandef.ExprBinding{
			{Expr: &plandef.AggregateExpr{Func: parser.AggCount, Of: price}, Out: products},
		},
		Variables: plandef.VarSet{products},
	}
	act = projectionLogicalProperties(aggOp, inputs, nil)
	exp = &logicalProperties{
		variables:  plandef.VarSet{products},
		resultSize: 1,
	}
	assert.Equal(t, exp, act)
}
