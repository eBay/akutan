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

	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/rpc"
	"github.com/sirupsen/logrus"
)

//enumerateOp is an operator that emits values.
type enumerateOp struct {
	def *plandef.Enumerate
}

func (o *enumerateOp) operator() plandef.Operator {
	return o.def
}

func (o *enumerateOp) columns() Columns {
	switch output := o.def.Output.(type) {
	case *plandef.Variable:
		return Columns{output}
	case *plandef.Binding:
		return Columns{output.Var}
	default:
		panic(fmt.Sprintf("Unexpected output type in enumerateOp.columns: %T", output))
	}
}

func (o *enumerateOp) execute(ctx context.Context, binder valueBinder, res results) error {
	switch output := o.def.Output.(type) {
	case *plandef.Variable:
		runUnboundEnumerate(ctx, o.def, binder, res)
	case *plandef.Binding:
		runBoundEnumerate(ctx, o.def, binder, res)
	default:
		panic(fmt.Sprintf("Unexpected output type in enumerateOp: %T", output))
	}
	return nil
}

// runUnboundEnumerate emits the values contained within the given Enumerate
// operator. It assumes the operator's output is a Variable, not a Binding. It
// emits one result per value.
func runUnboundEnumerate(ctx context.Context, op *plandef.Enumerate, binder valueBinder, res results) {
	// This normally emits its values just once, but in case it's located within the
	// right-hand side of some LoopJoin, it needs to output its values binder.len()
	// times.
	for offset := uint32(0); offset < binder.len(); offset++ {
		for _, opValue := range op.Values {
			val := Value{}
			switch opValue := opValue.(type) {
			case *plandef.OID:
				val = Value{KGObject: rpc.AKID(opValue.Value)}
			case *plandef.Literal:
				val = Value{KGObject: opValue.Value}
			default:
				logrus.Panicf("Unexpected value type in enumerateOp: %T", opValue)
			}
			res.add(ctx, offset, FactSet{}, []Value{val})
		}
	}
}

// runBoundEnumerate emits bound values that are also contained within the
// given Enumerate operator. It assumes the operator's output is a Binding, not
// a Variable. It emits one FactSet per value.
func runBoundEnumerate(ctx context.Context, op *plandef.Enumerate, binder valueBinder, res results) {
	output := op.Output.(*plandef.Binding)
	for offset := uint32(0); offset < binder.len(); offset++ {
		value := binder.bind(offset, output)
		if containsValue(op, value) {
			res.add(ctx, offset, FactSet{}, []Value{*value})
		}
	}
}

// containsValue returns true if the given value is found within the given
// Enumerate operator's set of values.
func containsValue(op *plandef.Enumerate, value *Value) bool {
	for _, opValue := range op.Values {
		switch opValue := opValue.(type) {
		case *plandef.OID:
			if value.KGObject.ValKID() == opValue.Value {
				return true
			}
		case *plandef.Literal:
			if value.KGObject.Equal(opValue.Value) {
				return true
			}
		default:
			logrus.Panicf("Unexpected value type in enumerateOp: %T", opValue)
		}
	}
	return false
}
