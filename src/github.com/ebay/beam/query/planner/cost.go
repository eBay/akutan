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
	"fmt"
	"time"

	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/query/planner/search"
	log "github.com/sirupsen/logrus"
)

type estCost struct {
	diskBytes int
	diskSeeks int
}

var infiniteCost = &estCost{
	diskBytes: 1 << 30,
	diskSeeks: 1 << 30,
}

func (cost *estCost) time() time.Duration {
	micros := 0
	// ~40K random reads per second => ~25us per seek
	micros += cost.diskSeeks * 25
	// ~300 MB/s disk read throughput => ~300 bytes per us
	micros += cost.diskBytes / 300
	return time.Duration(micros * 1000)
}

func (cost *estCost) Less(other search.Cost) bool {
	return cost.time() < other.(*estCost).time()
}

func (cost *estCost) Infinite() bool {
	return cost.diskBytes >= 1<<30 ||
		cost.diskSeeks >= 1<<30
}

func (cost *estCost) String() string {
	if cost.Infinite() {
		return "∞"
	}
	return fmt.Sprintf("[disk: %v seeks, %v KiB]",
		cost.diskSeeks, cost.diskBytes/1024)
}

func (planner *planner) LocalCost(expr *search.Expr) search.Cost {
	switch op := expr.Operator.(type) {
	case *plandef.HashJoin:
		return hashJoinCost(expr, planner.stats)
	case *plandef.LoopJoin:
		return loopJoinCost(expr, planner.stats)
	case *plandef.InferPO:
		return inferPOCost(expr, planner.stats)
	case *plandef.InferSP:
		return inferSPCost(expr, planner.stats)
	case *plandef.InferSPO:
		return inferSPOCost(expr, planner.stats)
	case *plandef.LookupPO:
		return lookupPOCost(expr, planner.stats)
	case *plandef.LookupPOCmp:
		return lookupPOCmpCost(expr, planner.stats)
	case *plandef.LookupS:
		return lookupSCost(expr, planner.stats)
	case *plandef.LookupSP:
		return lookupSPCost(expr, planner.stats)
	case *plandef.LookupSPO:
		return lookupSPOCost(expr, planner.stats)
	case *plandef.SelectLit:
		return selectionCost(expr, planner.stats)
	case *plandef.SelectVar:
		return selectionCost(expr, planner.stats)
	case *plandef.Enumerate:
		return enumerateCost(expr, planner.stats)
	case *plandef.ExternalIDs:
		return externalIdsCost(expr, planner.stats)
	case *plandef.Ask:
		return &estCost{diskBytes: 0, diskSeeks: 0}
	case *plandef.Projection:
		return projectionCost(expr, planner.stats)
	case *plandef.OrderByOp:
		return orderByCost(expr, planner.stats)
	case *plandef.LimitAndOffsetOp:
		return limitAndOffsetCost(expr, planner.stats)
	case *plandef.DistinctOp:
		return distinctCost(expr, planner.stats)
	case plandef.Operator:
		log.Panicf("estimateCosts(%T) not implemented", op)
	}
	// op must be a non-physical operator
	return infiniteCost
}

func (planner *planner) CombinedCost(expr *search.Expr) search.Cost {
	switch expr.Operator.(type) {
	case *plandef.LoopJoin:
		return loopJoinCombinedCost(expr)
	}
	localCost := expr.LocalCost.(*estCost)
	cost := &estCost{
		diskSeeks: localCost.diskSeeks,
		diskBytes: localCost.diskBytes,
	}
	for _, group := range expr.Inputs {
		input := group.Best.CombinedCost.(*estCost)
		cost.diskSeeks += input.diskSeeks
		cost.diskBytes += input.diskBytes
	}
	return cost
}
