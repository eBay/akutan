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

package plandef

import (
	"fmt"
	"strings"

	"github.com/ebay/akutan/rpc"
)

// A SelectLit Operator is a filter on a variable against one or more literals.
type SelectLit struct {
	// Which Variable to filter.
	Test *Variable
	// One or more ways to filter it.
	Clauses []SelectClause
}

func (*SelectLit) anOperator() {}

// String returns a string like "SelectLit ?foo < 30".
func (op *SelectLit) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "SelectLit %v", op.Test)
	for _, c := range op.Clauses {
		fmt.Fprintf(&b, " %v", c.String())
	}
	return b.String()
}

// Key implements cmp.Key.
func (op *SelectLit) Key(b *strings.Builder) {
	b.WriteString("SelectLit ")
	op.Test.Key(b)
	for _, c := range op.Clauses {
		b.WriteByte(' ')
		c.Key(b)
	}
}

// A SelectClause defines a filter in a Select.
type SelectClause struct {
	// A comparison OID such as query.OIDGreater, etc.
	Comparison rpc.Operator
	// Most comparisons take this one argument.
	Literal1 *Literal
	// The range-based comparisons need this second argument. Otherwise, it must
	// be nil.
	Literal2 *Literal
}

func (c SelectClause) String() string {
	// Provide nicer output for ranges.
	switch c.Comparison {
	case rpc.OpRangeIncExc:
		return fmt.Sprintf("in [%v, %v)", c.Literal1, c.Literal2)
	case rpc.OpRangeIncInc:
		return fmt.Sprintf("in [%v, %v]", c.Literal1, c.Literal2)
	case rpc.OpRangeExcInc:
		return fmt.Sprintf("in (%v, %v]", c.Literal1, c.Literal2)
	case rpc.OpRangeExcExc:
		return fmt.Sprintf("in (%v, %v)", c.Literal1, c.Literal2)
	}
	// For everything else, just use the rpc.Operator's stringer.
	if c.Literal2 == nil {
		return fmt.Sprintf("%v %v", c.Comparison, c.Literal1)
	}
	return fmt.Sprintf("%v %v %v", c.Comparison, c.Literal1, c.Literal2)
}

// Key implements cmp.Key.
func (c SelectClause) Key(b *strings.Builder) {
	c.Comparison.Key(b)
	b.WriteByte(' ')
	c.Literal1.Key(b)
	if c.Literal2 != nil {
		b.WriteByte(' ')
		c.Literal2.Key(b)
	}
}

// A SelectVar Operator is a filter that compares the values of 2 variables.
type SelectVar struct {
	// Which Variables to compare.
	Left  *Variable
	Right *Variable
	// The comparison operator.
	Operator rpc.Operator
}

func (*SelectVar) anOperator() {}

// String returns a string like "SelectVar ?src != ?dest".
func (op *SelectVar) String() string {
	return fmt.Sprintf("SelectVar %v %v %v", op.Left, op.Operator, op.Right)
}

// Key implements cmp.Key.
func (op *SelectVar) Key(b *strings.Builder) {
	b.WriteString("SelectVar ")
	op.Left.Key(b)
	b.WriteByte(' ')
	op.Operator.Key(b)
	b.WriteByte(' ')
	op.Right.Key(b)
}
