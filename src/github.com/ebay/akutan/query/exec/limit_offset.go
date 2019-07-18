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

package exec

import (
	"context"
	"fmt"
	"math"

	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/util/parallel"
)

// newLimitAndOffsetOp returns a new operator for the pagination operation. It
// provides streaming implementation of Limit & Offset, when executed it outputs
// subset of the results.
func newLimitAndOffsetOp(op *plandef.LimitAndOffsetOp, inputs []queryOperator) operator {
	if len(inputs) != 1 {
		panic(fmt.Sprintf("LimitAndOffsetOp operation with unexpected inputs: %v", len(inputs)))
	}
	return &limitAndOffsetOp{
		def:   op,
		input: inputs[0],
	}
}

type limitAndOffsetOp struct {
	def   *plandef.LimitAndOffsetOp
	input queryOperator
}

func (op *limitAndOffsetOp) columns() Columns {
	return op.input.columns()
}

func (op *limitAndOffsetOp) operator() plandef.Operator {
	return op.def
}

func (op *limitAndOffsetOp) execute(ctx context.Context, binder valueBinder, res results) error {
	if binder.len() != 1 {
		panic(fmt.Sprintf("limitAndOffsetOp operator %v unexpectedly bulk bound to %d rows",
			op.def, binder.len()))
	}
	inputResCh := make(chan ResultChunk, 4)
	wait := parallel.Go(func() {
		limit := uint64(math.MaxUint64)
		if op.def.Paging.Limit != nil {
			limit = *op.def.Paging.Limit
		}
		offset := uint64(0)
		if op.def.Paging.Offset != nil {
			offset = *op.def.Paging.Offset
		}

		skippedCount := uint64(0)
		forwardedCount := uint64(0)
		fullResultCount := uint64(0)
		for chunk := range inputResCh {
			numRows := uint64(chunk.NumRows())
			if skippedCount+numRows <= offset {
				skippedCount += numRows
				fullResultCount += numRows
				continue
			}
			for i := range chunk.offsets {
				fullResultCount++
				if skippedCount < offset {
					skippedCount++
					continue
				}
				if forwardedCount < limit {
					forwardedCount++
					res.add(ctx, chunk.offsets[i], FactSet{}, chunk.Row(i))
				}
			}
		}
		stats := FinalStatistics{TotalResultSize: fullResultCount}
		res.setFinalStatistics(stats)
	})

	err := op.input.run(ctx, binder, inputResCh)
	wait()
	return err
}
