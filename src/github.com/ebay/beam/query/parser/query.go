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

package parser

import (
	"fmt"
	"strings"
)

// The query language started with a home grown format kinda like SPARQL. As the
// functionality has expanded we're moving towards it being SPARQL or at least
// a sub-set of.
//
// Types in this file are based on their names in the SPARQL 1.1 grammar. The SPARQL
// 1.1 Query Language Spec is at https://www.w3.org/TR/sparql11-query and the grammar
// is at https://www.w3.org/TR/sparql11-query/#sparqlGrammar
//
// The where clause still uses our original syntax, and its types are in where.go
//

// Query contains the definition of a single query.
type Query struct {
	Type   QueryType
	Select SelectClause
	// DatasetClause not supported
	Where     WhereClause
	Modifiers SolutionModifier
}

func (q *Query) String() string {
	// As a temp measure, we'll generate the old style query string output if
	// the Type is LegacyPatternQuery. This helps not break downstream tests
	// that still depend on this format.
	if q.Type == LegacyPatternQuery {
		return q.Where.String()
	}
	modifiers := q.Modifiers.String()
	if len(modifiers) > 0 {
		modifiers = "\n" + modifiers
	}
	return fmt.Sprintf("%v %v\nWHERE {\n%v\n}%v", q.Type, q.Select, q.Where, modifiers)
}

// QueryType defines the different type of queries. See
// https://www.w3.org/TR/sparql11-query/#QueryForms
type QueryType int

const (
	// LegacyPatternQuery indicates the query should use the legacy beam query
	// where a fact pattern is specified, and sets of facts matching the pattern
	// are returned. This is what is now in the Where clause of a Select query.
	LegacyPatternQuery QueryType = iota + 1
	// SelectQuery indicates the query should match patterns from the where
	// clause and return results as a tabular set of values. The result values
	// are either based on variables from the where clause, or on an aggregate
	// function.
	SelectQuery
	// AskQuery indicates the query should check if there exists at least one
	// set of facts that match the WHERE clause. If so it returns true, false
	// otherwise
	AskQuery
	// SPARQL also defines these additional query types, we're not supporting
	// these.
	//  ConstructQuery
	//  DescribeQuery
)

func (q QueryType) String() string {
	switch q {
	case LegacyPatternQuery:
		return "LEGACY"
	case SelectQuery:
		return "SELECT"
	case AskQuery:
		return "ASK"
	default:
		return fmt.Sprintf("Unknown QueryType (%d)", int(q))
	}
}

// SelectClause details the requested results projection for the query. A Select
// * query would result in a single Wildcard instance in the Items slice.
type SelectClause struct {
	Keyword selectClauseKeyword // Distinct / Reduced
	Items   []selectClauseItem  // *Variable, *BoundExpression, Wildcard
}

func (s SelectClause) String() string {
	res := strings.Builder{}
	if s.Keyword != nil {
		res.WriteString(s.Keyword.String())
		res.WriteByte(' ')
	}
	for i, item := range s.Items {
		if i > 0 {
			res.WriteByte(' ')
		}
		res.WriteString(item.String())
	}
	return res.String()
}

// selectClauseKeyword is a marker interface for the keywords that can appear in
// the select clause.
type selectClauseKeyword interface {
	isSelectClauseKeyword()
	String() string
}

var _ = []selectClauseKeyword{
	Distinct{},
	Reduced{},
}

// Distinct defines select clause DISTINCT keyword.
type Distinct struct {
}

func (d Distinct) isSelectClauseKeyword() {}
func (d Distinct) String() string {
	return "DISTINCT"
}

// Reduced defines select clause REDUCED keyword.
type Reduced struct {
}

func (d Reduced) isSelectClauseKeyword() {}
func (d Reduced) String() string {
	return "REDUCED"
}

// selectClauseItem is a marker interface for types that can appear in the
// select clause
type selectClauseItem interface {
	isSelectClauseItem()
	String() string
}

// Expression is a marker interface for types that are expressions. For example
// COUNT(?v) is an expression. (AggregateExpr is our only Expression type)
type Expression interface {
	isExpression()
	String() string
}

var _ = []selectClauseItem{
	new(Variable),
	new(BoundExpression),
	new(Wildcard),
}

var _ = []aggregateTarget{
	new(Wildcard),
	new(Variable),
}

// BoundExpression represents an expression whose result is bound to a variable.
// e.g (COUNT(?v) AS ?c) is a BoundExpression, COUNT(?v) is the expression and
// its bound to the ?c Variable.
type BoundExpression struct {
	Expr Expression
	As   *Variable
}

func (b *BoundExpression) String() string {
	return fmt.Sprintf("(%v AS %v)", b.Expr, b.As)
}

func (b *BoundExpression) isSelectClauseItem() {}

// AggregateExpr describes a single aggregate expression.
type AggregateExpr struct {
	Function AggregateFunction
	// Distinct not supported

	// Of is the target to be aggregated.
	Of aggregateTarget // *Variable, Wildcard [Later Expression]
}

func (a *AggregateExpr) String() string {
	return fmt.Sprintf("%v(%v)", a.Function, a.Of)
}

func (*AggregateExpr) isExpression() {}

// aggregateTarget is a marker interface for types that can be aggregated by
// AggregateExpr
type aggregateTarget interface {
	isAggregateTarget()
}

// AggregateFunction describes the different types of aggregate functions. Only
// COUNT is currently supported.
type AggregateFunction int

const (
	// AggCount counts rows (or solutions in SPARQL terms)
	AggCount AggregateFunction = iota + 1

// SPARQL also defines other aggregate function, we're don't support them
//  AggSum
//  AggMin
//  AggMax
//  AggAvg
//  AggSample
//  AggGroupConcat
)

func (f AggregateFunction) String() string {
	switch f {
	case AggCount:
		return "COUNT"
	default:
		return fmt.Sprintf("Unknown AggregateFunction (%d)", int(f))
	}
}

// Wildcard represents the * identifier used in the SELECT *... and SELECT
// (COUNT(*)...) expressions.
type Wildcard struct {
}

func (w Wildcard) String() string {
	return "*"
}

func (w Wildcard) isSelectClauseItem() {}
func (w Wildcard) isAggregateTarget()  {}

// SolutionModifier describes the different ways in which the solutions (aka
// rows) produced by the where clause can be manipulated, such as sorting.
type SolutionModifier struct {
	// GroupClause not supported
	// HavingClause not supported
	OrderBy []OrderCondition
	Paging  LimitOffset
}

func (s *SolutionModifier) String() string {
	res := strings.Builder{}
	for idx, o := range s.OrderBy {
		if idx == 0 {
			res.WriteString("ORDER BY")
		}
		res.WriteByte(' ')
		res.WriteString(o.String())
	}
	paging := s.Paging.String()
	if len(paging) > 0 {
		if res.Len() > 0 {
			res.WriteByte('\n')
		}
		res.WriteString(paging)
	}
	return res.String()
}

// OrderCondition describes a single expression in the order by clause. SPARQL
// supports ordering by expressions, however we only support ordering by
// variables.
type OrderCondition struct {
	Direction SortDirection
	On        *Variable
}

func (o *OrderCondition) String() string {
	return fmt.Sprintf("%v(%s)", o.Direction, o.On)
}

// SortDirection is the direction that a sort should be in.
type SortDirection int

const (
	// SortAsc indicate an ascending sort, i.e. smaller values appear before
	// larger values.
	SortAsc SortDirection = 1
	// SortDesc indicate a descending sort, i.e. larger values appear
	// before smaller values.
	SortDesc SortDirection = 2
)

func (d SortDirection) String() string {
	switch d {
	case SortAsc:
		return "ASC"
	case SortDesc:
		return "DESC"
	default:
		return fmt.Sprintf("Unknown Direction (%d)", int(d))
	}
}

// LimitOffset contains paging related values
type LimitOffset struct {
	// Limit and Offset are nil if not explicitly specified in the query.
	Limit  *uint64
	Offset *uint64
}

func (o *LimitOffset) String() string {
	res := strings.Builder{}
	if o.Limit != nil {
		fmt.Fprintf(&res, "LIMIT %d", *o.Limit)
	}
	if o.Offset != nil {
		if res.Len() > 0 {
			res.WriteByte(' ')
		}
		fmt.Fprintf(&res, "OFFSET %d", *o.Offset)
	}
	return res.String()
}
