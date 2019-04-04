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

// Package plandef defines the output of the query planner. It defines all of
// Beam's physical execution plan operators.
package plandef

import (
	"fmt"
	"strings"

	"github.com/ebay/beam/util/cmp"
)

// A Plan is an tree of physical operators that Beam can execute to answer a
// query.
type Plan struct {
	// Which operation to execute, including its scalar arguments.
	Operator Operator
	// The results that the operator takes as inputs, if any. For example, a
	// HashJoin takes two inputs, but lookups take none.
	Inputs []*Plan
	// The variables whose values are output by the plan node.
	Variables VarSet
}

// String returns a multi-line indented human-readable string describing the
// execution plan.
func (plan *Plan) String() string {
	var b strings.Builder
	var print func(plan *Plan, indent string)
	print = func(plan *Plan, indent string) {
		fmt.Fprintf(&b, "%v%v\n", indent, plan.Operator)
		for _, input := range plan.Inputs {
			print(input, indent+"\t")
		}
	}
	print(plan, "")
	return b.String()
}

// Operator is a physical, executable operator.
type Operator interface {
	String() string
	cmp.Key
	anOperator()
}

// ImplementOperator is a list of types that implement Operator.
// This serves as documentation and as a compile-time check.
var ImplementOperator = []Operator{
	// Defined in joins.go
	new(HashJoin),
	new(LoopJoin),
	// Defined in lookups.go
	new(InferPO),
	new(InferSP),
	new(InferSPO),
	new(LookupPO),
	new(LookupPOCmp),
	new(LookupS),
	new(LookupSP),
	new(LookupSPO),
	// Defined in select.go
	new(SelectLit),
	new(SelectVar),
	// Defined in enumerate.go
	new(Enumerate),
	// Defined in query.go
	new(ExternalIDs),
	new(Projection),
	new(OrderByOp),
	new(LimitAndOffsetOp),
	new(DistinctOp),
}
