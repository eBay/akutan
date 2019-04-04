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
	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/query/planner/search"
)

// orderByCost returns the estimated cost for an OrderBy operator.
func orderByCost(expr *search.Expr, stats Stats) *estCost {
	return &estCost{
		diskBytes: 0,
		diskSeeks: 0,
	}
}

// orderByLogicalProperties returns logical properties for an OrderBy operator.
func orderByLogicalProperties(op *plandef.OrderByOp, inputs []*logicalProperties, stats Stats) *logicalProperties {
	return &logicalProperties{
		variables:  inputs[0].variables,
		resultSize: inputs[0].resultSize,
	}
}
