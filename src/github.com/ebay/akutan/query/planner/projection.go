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
	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/query/planner/search"
)

func askLogicalProperties(op *plandef.Ask) *logicalProperties {
	return &logicalProperties{
		variables:  plandef.VarSet{op.Out},
		resultSize: 1,
	}
}

// projectionCost returns the estimated cost for a Projection operator.
func projectionCost(expr *search.Expr, stats Stats) *estCost {
	return &estCost{
		diskBytes: 0,
		diskSeeks: 0,
	}
}

func projectionLogicalProperties(op *plandef.Projection, inputs []*logicalProperties, stats Stats) *logicalProperties {
	p := logicalProperties{
		variables:  op.Variables,
		resultSize: inputs[0].resultSize,
	}
	if hasAggregateExpr(op.Select) {
		p.resultSize = 1
	}
	return &p
}

// hasAggregateExpr returns true if any item in the select list is an aggregate
// expression.
func hasAggregateExpr(s []plandef.ExprBinding) bool {
	for _, item := range s {
		if _, isAggr := item.Expr.(*plandef.AggregateExpr); isAggr {
			return true
		}
	}
	return false
}

func externalIdsCost(expr *search.Expr, stats Stats) *estCost {
	return &estCost{
		diskBytes: 0,
		diskSeeks: 0,
	}
}

func externalIdsLogicalProperties(op *plandef.ExternalIDs, inputs []*logicalProperties, stats Stats) *logicalProperties {
	return &logicalProperties{
		variables:  inputs[0].variables,
		resultSize: inputs[0].resultSize,
	}
}
