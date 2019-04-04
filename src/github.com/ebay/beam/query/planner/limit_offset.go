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
	"math/bits"

	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/query/planner/search"
)

// limitAndOffsetCost returns the estimated cost for an LimitAndOffsetOp
// operator.
func limitAndOffsetCost(expr *search.Expr, stats Stats) *estCost {
	return &estCost{
		diskBytes: 0,
		diskSeeks: 0,
	}
}

// limitAndOffsetLogicalProperties returns logical properties for an
// LimitAndOffsetOp operator.
func limitAndOffsetLogicalProperties(op *plandef.LimitAndOffsetOp, inputs []*logicalProperties, stats Stats) *logicalProperties {
	size := inputs[0].resultSize

	if op.Paging.Limit != nil {
		limit := *op.Paging.Limit
		if limit <= uint64(maxInt) && int(limit) < size {
			size = int(limit)
		}
	}
	return &logicalProperties{
		variables:  inputs[0].variables,
		resultSize: size,
	}
}

const maxInt int = 1<<(bits.UintSize-1) - 1
