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
	"strings"

	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/parallel"
)

// TODO: What are the comparison/selection semantics for langID/unitID ?

// newSelectLitOp returns a new operator for the supplied SelectLit plan
// operator and its inputs. selectLitOp expects a single input.
func newSelectLitOp(op *plandef.SelectLit, inputs []queryOperator) operator {
	if len(inputs) != 1 {
		panic(fmt.Sprintf("selectLit operator called with unexpected number of inputs: %d", len(inputs)))
	}
	objComparerOp := buildComparers(op)
	testIdx, exists := inputs[0].columns().IndexOf(op.Test)
	if !exists {
		panic(fmt.Sprintf("selectLit operator has plan with variable %s but, that's not in the input columns %v",
			op.Test, inputs[0].columns()))
	}
	return &selectOp{
		def:   op,
		input: inputs[0],
		comparer: func(row []Value) bool {
			return objComparerOp(row[testIdx].KGObject)
		},
	}
}

// newSelectVarOp returns a new operator for the supplied SelectVar plan
// operator and its inputs. selectVarOp expects a single input.
func newSelectVarOp(op *plandef.SelectVar, inputs []queryOperator) operator {
	if len(inputs) != 1 {
		panic(fmt.Sprintf("selectVar operator called with unexpected number of inputs: %d", len(inputs)))
	}
	return &selectOp{
		def:      op,
		input:    inputs[0],
		comparer: selectVarCompareOp(op, inputs[0].columns()),
	}
}

// selectOp does the bulk of the operator work for SelectLit & SelectVar. Its an
// operator that executes the input, and filters out results where the comparer
// returns false.
type selectOp struct {
	def      plandef.Operator
	input    queryOperator
	comparer func(row []Value) bool
}

func (s *selectOp) operator() plandef.Operator {
	return s.def
}

func (s *selectOp) columns() Columns {
	return s.input.columns()
}

func (s *selectOp) execute(ctx context.Context, binder valueBinder, res results) error {
	inputResCh := make(chan ResultChunk, 4)
	wait := parallel.Go(func() {
		for chunk := range inputResCh {
			for i := range chunk.offsets {
				row := chunk.Row(i)
				if s.comparer(row) {
					// this factset meets the selection criteria pass it along
					res.add(ctx, chunk.offsets[i], chunk.Facts[i], row)
				}
			}
		}
	})
	err := s.input.run(ctx, binder, inputResCh)
	wait()
	return err
}

// comparer should return true if the supplied value meets the required condition
type comparer func(val rpc.KGObject) bool

func and(c1, c2 comparer) comparer {
	return func(v rpc.KGObject) bool {
		return c1(v) && c2(v)
	}
}

func not(c comparer) comparer {
	return func(v rpc.KGObject) bool {
		return !c(v)
	}
}

func buildComparers(op *plandef.SelectLit) comparer {
	if len(op.Clauses) == 1 {
		return buildComparer(op.Test, &op.Clauses[0])
	}
	c := make([]comparer, len(op.Clauses))
	for i := range op.Clauses {
		c[i] = buildComparer(op.Test, &op.Clauses[i])
	}
	return func(v rpc.KGObject) bool {
		for i := range c {
			if !c[i](v) {
				return false
			}
		}
		return true
	}
}

func buildComparer(v *plandef.Variable, clause *plandef.SelectClause) comparer {
	// TODO: for functional parity we include comparison of literal units and languages however we need to resolve whether
	// or not no unit/language indicated in the query object indicates whether or not we ignore units in the comparison
	//
	toType := clause.Literal1.Value.ValueType()
	langID := clause.Literal1.Value.LangID()
	unitID := clause.Literal1.Value.UnitID()
	var langUnitTypeCheck comparer
	switch toType {
	case rpc.KtKID:
		if clause.Comparison != rpc.OpNotEqual && clause.Comparison != rpc.OpEqual {
			panic("invalid comparison, if the literal value is a KID, only == & != are allowed comparisons")
		}
		fallthrough
	case rpc.KtNil:
		langUnitTypeCheck = func(v rpc.KGObject) bool {
			return v.IsType(toType)
		}
	case rpc.KtString:
		langUnitTypeCheck = func(v rpc.KGObject) bool {
			return v.LangID() == langID && v.IsType(toType)
		}
	case rpc.KtInt64:
		fallthrough
	case rpc.KtFloat64:
		fallthrough
	case rpc.KtBool:
		fallthrough
	case rpc.KtTimestamp:
		langUnitTypeCheck = func(v rpc.KGObject) bool {
			return v.UnitID() == unitID && v.IsType(toType)
		}
	default:
		panic(fmt.Sprintf("Unexpected KGObject type of %T/%v in select.go", clause.Literal1.Value, clause.Literal1.Value))
	}

	switch clause.Comparison {
	case rpc.OpEqual:
		// equal & notEqual don't need the langUnitTypeCheck because they are comparing the entire KGObject
		// and that includes the lang & type
		return clause.Literal1.Value.Equal
	case rpc.OpNotEqual:
		return not(clause.Literal1.Value.Equal)
	case rpc.OpLessOrEqual:
		return and(langUnitTypeCheck, cLessOrEqual(clause.Literal1.Value))
	case rpc.OpLess:
		return and(langUnitTypeCheck, cLess(clause.Literal1.Value))
	case rpc.OpGreater:
		return and(langUnitTypeCheck, cGreater(clause.Literal1.Value))
	case rpc.OpGreaterOrEqual:
		return and(langUnitTypeCheck, cGreaterOrEqual(clause.Literal1.Value))
	case rpc.OpPrefix:
		return and(langUnitTypeCheck, cPrefix(clause.Literal1.Value))
	case rpc.OpRangeIncInc:
		return and(langUnitTypeCheck, cRange(rInc, rInc, clause.Literal1.Value, clause.Literal2.Value))
	case rpc.OpRangeIncExc:
		return and(langUnitTypeCheck, cRange(rInc, rExc, clause.Literal1.Value, clause.Literal2.Value))
	case rpc.OpRangeExcInc:
		return and(langUnitTypeCheck, cRange(rExc, rInc, clause.Literal1.Value, clause.Literal2.Value))
	case rpc.OpRangeExcExc:
		return and(langUnitTypeCheck, cRange(rExc, rExc, clause.Literal1.Value, clause.Literal2.Value))
	}
	panic(fmt.Sprintf("Unexpected comparison type in buildComparer %d", clause.Comparison))
}

type rangeType int

const (
	rInc rangeType = iota
	rExc
)

func cLessOrEqual(to rpc.KGObject) comparer {
	return func(v rpc.KGObject) bool {
		return v.Less(to) || v.Equal(to)
	}
}

func cLess(to rpc.KGObject) comparer {
	return func(v rpc.KGObject) bool {
		return v.Less(to)
	}
}

func cGreater(to rpc.KGObject) comparer {
	return func(v rpc.KGObject) bool {
		return to.Less(v)
	}
}

func cGreaterOrEqual(to rpc.KGObject) comparer {
	return func(v rpc.KGObject) bool {
		return to.Less(v) || to.Equal(v)
	}
}

func cPrefix(to rpc.KGObject) comparer {
	prefix := to.ValString()
	return func(v rpc.KGObject) bool {
		return strings.HasPrefix(v.ValString(), prefix)
	}
}

func cRange(startType, endType rangeType, lowerBound rpc.KGObject, upperBound rpc.KGObject) comparer {
	// upper & lower bounds better be the same type
	if lowerBound.ValueType() != upperBound.ValueType() {
		panic(fmt.Sprintf("qexec.cRange was passed lower/upper bounds of different types %d/%d", lowerBound.ValueType(), upperBound.ValueType()))
	}
	var lowerCmp comparer
	var upperCmp comparer
	switch startType {
	case rInc:
		lowerCmp = cGreaterOrEqual(lowerBound)
	case rExc:
		lowerCmp = cGreater(lowerBound)
	}
	switch endType {
	case rInc:
		upperCmp = cLessOrEqual(upperBound)
	case rExc:
		upperCmp = cLess(upperBound)
	}
	return and(lowerCmp, upperCmp)
}

// selectVarCompareOp returns a function that returns true if the supplied
// FactSet passes the selection criteria.
func selectVarCompareOp(op *plandef.SelectVar, cols Columns) func(row []Value) bool {
	if op.Operator != rpc.OpEqual && op.Operator != rpc.OpNotEqual {
		panic(fmt.Sprintf("selectVarCompareOp operator '%v' not supported", op.Operator))
	}
	leftIdx, exists := cols.IndexOf(op.Left)
	if !exists {
		panic(fmt.Sprintf("selectOp is trying to use variable %s for the Left side, but that's not available (have %s)", op.Left, cols))
	}
	rightIdx, exists := cols.IndexOf(op.Right)
	if !exists {
		panic(fmt.Sprintf("selectOp is trying to use variable %s for the Right side, but that's not available (has %s)", op.Left, cols))
	}
	return func(row []Value) bool {
		left := row[leftIdx].KGObject
		right := row[rightIdx].KGObject
		switch op.Operator {
		case rpc.OpEqual:
			return left.Equal(right)
		case rpc.OpNotEqual:
			return !left.Equal(right)
		}
		panic("not reachable")
	}
}
