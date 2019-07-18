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
	"strconv"
	"strings"

	"github.com/ebay/akutan/query/parser"
	"github.com/ebay/akutan/util/cmp"
)

// Projection is an Operator which takes input rows, evaluates them, and
// generates output rows. This is mapped from the SELECT keyword in the parser.
// For SELECT * WHERE ... queries there is no Projection in the operator tree.
// As the API returns all the variables in the ResultChunks, there's no need for
// one.
type Projection struct {
	// The list of expressions to project. If the query used SELECT ?someVar it
	// is converted into an ExprBinding of (?someVar AS ?someVar)
	Select []ExprBinding
	// contains the set of variables that'll be in the output.
	Variables VarSet
}

func (*Projection) anOperator() {}

// Key implements the cmp.Key interface
func (p *Projection) Key(k *strings.Builder) {
	k.WriteString("Project")
	for _, item := range p.Select {
		k.WriteByte(' ')
		item.Key(k)
	}
}

func (p *Projection) String() string {
	b := strings.Builder{}
	b.Grow(64)
	b.WriteString("Project")
	for _, item := range p.Select {
		b.WriteByte(' ')
		b.WriteString(item.String())
	}
	return b.String()
}

// Ask is an operator which generates a boolean output indicating
// if it received any rows from its input.
type Ask struct {
	Out *Variable
}

func (a *Ask) anOperator() {}

// Key implements cmp.Key
func (a *Ask) Key(k *strings.Builder) {
	k.WriteString("Ask ")
	a.Out.Key(k)
}

func (a *Ask) String() string {
	return "Ask " + a.Out.String()
}

// ExternalIDs is an operator that will fetch and add externalIDs to variable
// values. It takes a single input.
type ExternalIDs struct {
}

func (*ExternalIDs) anOperator() {}

// Key implements cmp.Key
func (e *ExternalIDs) Key(k *strings.Builder) {
	k.WriteString("ExternalIDs")
}

func (e *ExternalIDs) String() string {
	return cmp.GetKey(e)
}

// OrderByOp is an operator that will order the results based on the order condition.
type OrderByOp struct {
	OrderBy []OrderCondition
}

// OrderCondition describes a single expression in the order by cluase.
type OrderCondition struct {
	Direction SortDirection
	On        *Variable
}

// Key implementes cmp.Key
func (o *OrderCondition) Key(k *strings.Builder) {
	k.WriteString(o.Direction.String())
	k.WriteByte('(')
	o.On.Key(k)
	k.WriteByte(')')
}

func (o *OrderCondition) String() string {
	return cmp.GetKey(o)
}

// SortDirection is the direction that a sort should be in.
type SortDirection = parser.SortDirection

const (
	// SortAsc indicate an ascending sort, i.e. smaller values appear before
	// larger values.
	SortAsc SortDirection = parser.SortAsc
	// SortDesc indicate a descending sort, i.e. larger values appear
	// before smaller values.
	SortDesc SortDirection = parser.SortDesc
)

func (*OrderByOp) anOperator() {}

// Key implements cmp.Key
func (o *OrderByOp) Key(k *strings.Builder) {
	k.WriteString("OrderBy")

	for _, cond := range o.OrderBy {
		k.WriteByte(' ')
		cond.Key(k)
	}
}

func (o *OrderByOp) String() string {
	return cmp.GetKey(o)
}

// LimitAndOffsetOp is an operator that will paginate the results.
type LimitAndOffsetOp struct {
	Paging LimitOffset
}

// LimitOffset contains paging related values
type LimitOffset struct {
	// Limit or Offset can be nil if not explicitly specified in the query.
	Limit  *uint64
	Offset *uint64
}

// Key implements cmp.Key
func (l *LimitOffset) Key(k *strings.Builder) {
	if l.Limit != nil {
		k.WriteString("Lmt ")
		k.WriteString(strconv.FormatUint(*l.Limit, 10))
	}
	if l.Limit != nil && l.Offset != nil {
		k.WriteByte(' ')
	}
	if l.Offset != nil {
		k.WriteString("Off ")
		k.WriteString(strconv.FormatUint(*l.Offset, 10))
	}
}

func (l *LimitOffset) String() string {
	return cmp.GetKey(l)
}

func (*LimitAndOffsetOp) anOperator() {}

// Key implements cmp.Key
func (lop *LimitAndOffsetOp) Key(k *strings.Builder) {
	k.WriteString("LimitOffset (")
	lop.Paging.Key(k)
	k.WriteByte(')')
}

func (lop *LimitAndOffsetOp) String() string {
	return cmp.GetKey(lop)
}

// DistinctOp is an operator that will remove the duplicate rows from the
// results.
type DistinctOp struct {
}

func (*DistinctOp) anOperator() {}

// Key implements cmp.Key
func (d *DistinctOp) Key(k *strings.Builder) {
	k.WriteString("Distinct")
}

func (d *DistinctOp) String() string {
	return cmp.GetKey(d)
}
