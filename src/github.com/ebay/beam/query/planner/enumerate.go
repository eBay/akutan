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

// enumerateLogicalProperties returns logical properties for an Enumerate operator.
func enumerateLogicalProperties(op *plandef.Enumerate, inputs []*logicalProperties, stats Stats) *logicalProperties {
	resultSize := len(op.Values)
	if resultSize == 0 {
		// If we leave resultSize=0 here, the invariants check will fail with "expected
		// result size is unset".
		resultSize = 1
	}
	return &logicalProperties{
		variables:  termsToVars(op.Output),
		resultSize: resultSize,
	}
}

// enumerateCost returns the estimated cost for an Enumerate operator.
func enumerateCost(expr *search.Expr, stats Stats) *estCost {
	return &estCost{
		diskBytes: 0,
		diskSeeks: 0,
	}
}
