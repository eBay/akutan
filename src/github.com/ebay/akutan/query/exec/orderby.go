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
	"sort"
	"strings"

	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/parallel"
)

// newOrderByOp returns a OrderBy operator.
func newOrderByOp(op *plandef.OrderByOp, inputs []queryOperator) operator {
	if len(inputs) != 1 {
		panic(fmt.Sprintf("OrderByOp operation with unexpected inputs: %v",
			len(inputs)))
	}
	return &orderByOp{
		def:   op,
		input: inputs[0],
	}
}

type orderByOp struct {
	def   *plandef.OrderByOp
	input queryOperator
}

func (op *orderByOp) columns() Columns {
	return op.input.columns()
}

func (op *orderByOp) operator() plandef.Operator {
	return op.def
}

func (op *orderByOp) execute(ctx context.Context, binder valueBinder, res results) error {
	if binder.len() != 1 {
		panic(fmt.Sprintf("orderByOp operator %v unexpectedly bulk bound to %d rows",
			op.def, binder.len()))
	}
	// row describes a single row from a table output.
	type row []Value

	comparers := make([]func(a, b row) int, 0, len(op.def.OrderBy))
	for _, cond := range op.def.OrderBy {
		colIdx := op.columns().MustIndexOf(cond.On)
		switch cond.Direction {
		case plandef.SortAsc:
			comparers = append(comparers, func(a, b row) int {
				return compareValues(&a[colIdx], &b[colIdx])
			})
		case plandef.SortDesc:
			comparers = append(comparers, func(a, b row) int {
				return compareValues(&b[colIdx], &a[colIdx])
			})
		}
	}

	less := func(a, b row) bool {
		for _, comp := range comparers {
			result := comp(a, b)
			if result < 0 {
				return true
			} else if result > 0 {
				return false
			}
		}
		return false
	}

	inputResCh := make(chan ResultChunk, 4)
	wait := parallel.Go(func() {
		var rows []row
		for chunk := range inputResCh {
			res.setFinalStatistics(chunk.FinalStatistics)
			for i := range chunk.offsets {
				rowIn := chunk.Row(i)
				rows = append(rows, rowIn)
			}
		}
		sort.Slice(rows, func(i, j int) bool {
			return less(rows[i], rows[j])
		})
		for _, r := range rows {
			res.add(ctx, 0, FactSet{}, r)
		}
	})

	err := op.input.run(ctx, binder, inputResCh)
	wait()
	return err
}

// compareValues returns an integer comparing two Value. The result will be 0 if
// a==b, -1 if a < b, and +1 if a > b.
func compareValues(a, b *Value) int {
	// Compare the value type.
	if a.KGObject.ValueType() < b.KGObject.ValueType() {
		return -1
	} else if a.KGObject.ValueType() > b.KGObject.ValueType() {
		return 1
	}

	// Both a and b are of same type, now compare the value.
	switch a.KGObject.ValueType() {
	case rpc.KtNil:
		return 0
	case rpc.KtKID:
		return strings.Compare(a.ExtID, b.ExtID)
	case rpc.KtString:
		res := strings.Compare(a.KGObject.ValString(), b.KGObject.ValString())
		if res != 0 {
			return res
		}
		// Compare the language external ID.
		return strings.Compare(a.LangExtID, b.LangExtID)
	case rpc.KtInt64, rpc.KtFloat64, rpc.KtTimestamp, rpc.KtBool:
		// Compare the unit external ID.
		res := strings.Compare(a.UnitExtID, b.UnitExtID)
		if res != 0 {
			return res
		}
		if a.KGObject.Equal(b.KGObject) {
			return 0
		}
		if a.KGObject.Less(b.KGObject) {
			return -1
		}
		return 1
	default:
		panic(fmt.Sprintf("Unknown KGObject type %v", a.KGObject.ValueType()))
	}
}
