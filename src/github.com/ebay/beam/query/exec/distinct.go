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

	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/util/parallel"
)

// newDistinctOp returns a new operator for the distinct operation. When
// executed it will remote duplicate rows from the output results.
func newDistinctOp(op *plandef.DistinctOp, inputs []queryOperator) operator {
	if len(inputs) != 1 {
		panic(fmt.Sprintf("DistinctOp operation with unexpected inputs: %v", len(inputs)))
	}
	return &distinctOp{
		def:   op,
		input: inputs[0],
	}
}

type distinctOp struct {
	def   *plandef.DistinctOp
	input queryOperator
}

func (op *distinctOp) columns() Columns {
	return op.input.columns()
}

func (op *distinctOp) operator() plandef.Operator {
	return op.def
}

func (op *distinctOp) execute(ctx context.Context, binder valueBinder, res results) error {
	if binder.len() != 1 {
		panic(fmt.Sprintf("distinctOp operator %v unexpectedly bulk bound to %d rows",
			op.def, binder.len()))
	}
	inputResCh := make(chan ResultChunk, 4)
	wait := parallel.Go(func() {
		// Distinct doesn't care about the variable names, just populate column
		// indexes based on the number of input columns.
		colIndexes := make([]int, len(op.input.columns()))
		for i := range colIndexes {
			colIndexes[i] = i
		}
		seen := make(map[string]struct{})
		for chunk := range inputResCh {
			res.setFinalStatistics(chunk.FinalStatistics)
			for i := range chunk.offsets {
				key := chunk.identityKeysOf(i, colIndexes)
				if _, exists := seen[string(key)]; !exists {
					seen[string(key)] = struct{}{}
					res.add(ctx, chunk.offsets[i], chunk.Facts[i], chunk.Row(i))
				}
			}
		}
	})

	err := op.input.run(ctx, binder, inputResCh)
	wait()
	return err
}
