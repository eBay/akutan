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
	"time"

	"github.com/ebay/akutan/api"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/unicode"
	"github.com/vektah/goparsify"
)

func unit(n *goparsify.Result) {
	switch t := n.Child[1].Result.(type) {
	case *QName:
		n.Result = &Unit{Value: t.Value}
	case *Entity:
		n.Result = &Unit{Value: t.Value}
	default:
		panic(fmt.Sprintf("unsupported unit type: %T", t))
	}
}

func lang(n *goparsify.Result) {
	if n.Child[1].Token == "" {
		n.Result = NoLanguage
	} else {
		n.Result = &Language{Value: n.Child[1].Token}
	}
}

func literalBool(n *goparsify.Result) {
	switch n.Child[0].Token {
	case "true":
		n.Result = &LiteralBool{getUnitType(n), true}
	case "false":
		n.Result = &LiteralBool{getUnitType(n), false}
	default:
		panic(fmt.Sprintf("unsupported bool literal: %s", n.Token))
	}
}

// getUnitType returns either NoUnits or Unit from the child result.
func getUnitType(n *goparsify.Result) Unit {
	if n.Child[1].Result == nil {
		return NoUnit
	}
	return *n.Child[1].Result.(*Unit)
}

func literalNumber(n *goparsify.Result) {
	switch v := n.Child[0].Result.(type) {
	case float64:
		n.Result = &LiteralFloat{getUnitType(n), v}
	case int64:
		n.Result = &LiteralInt{getUnitType(n), v}
	default:
		panic(fmt.Sprintf("unsupported number literal: '%s' %v", n.Token, v))
	}
}

func literalString(n *goparsify.Result) {
	res := LiteralString{
		Value:    unicode.Normalize(n.Child[0].Token),
		Language: NoLanguage,
	}
	if n.Child[1].Result != nil {
		res.Language = *n.Child[1].Result.(*Language)
	}
	n.Result = &res
}

func literalTimeY(n *goparsify.Result) {
	year := n.Result.(int)
	n.Result = &LiteralTime{
		NoUnit,
		time.Date(year, zero.Month(), zero.Day(), zero.Hour(), zero.Minute(), zero.Second(), zero.Nanosecond(), zero.Location()),
		api.Year,
	}
}

func literalTimeYM(n *goparsify.Result) {
	lt := n.Child[0].Result.(*LiteralTime).Value
	month := time.Month(n.Child[2].Result.(int))
	n.Result = &LiteralTime{
		NoUnit,
		time.Date(lt.Year(), month, zero.Day(), zero.Hour(), zero.Minute(), zero.Second(), zero.Nanosecond(), zero.Location()),
		api.Month,
	}
}

func literalTimeYMD(n *goparsify.Result) {
	lt := n.Child[0].Result.(*LiteralTime).Value
	day := n.Child[2].Result.(int)
	n.Result = &LiteralTime{
		NoUnit,
		time.Date(lt.Year(), lt.Month(), day, zero.Hour(), zero.Minute(), zero.Second(), zero.Nanosecond(), zero.Location()),
		api.Day,
	}
}

func literalTimeYMDH(n *goparsify.Result) {
	lt := n.Child[0].Result.(*LiteralTime).Value
	hour := n.Child[2].Result.(int)
	n.Result = &LiteralTime{
		NoUnit,
		time.Date(lt.Year(), lt.Month(), lt.Day(), hour, zero.Minute(), zero.Second(), zero.Nanosecond(), zero.Location()),
		api.Hour,
	}
}

func literalTimeYMDHM(n *goparsify.Result) {
	lt := n.Child[0].Result.(*LiteralTime).Value
	minute := n.Child[2].Result.(int)
	n.Result = &LiteralTime{
		NoUnit,
		time.Date(lt.Year(), lt.Month(), lt.Day(), lt.Hour(), minute, zero.Second(), zero.Nanosecond(), zero.Location()),
		api.Minute,
	}
}

func literalTimeYMDHMS(n *goparsify.Result) {
	lt := n.Child[0].Result.(*LiteralTime).Value
	second := n.Child[2].Result.(int)
	n.Result = &LiteralTime{
		NoUnit,
		time.Date(lt.Year(), lt.Month(), lt.Day(), lt.Hour(), lt.Minute(), second, zero.Nanosecond(), zero.Location()),
		api.Second,
	}
}

func literalTimeYMDHMSN(n *goparsify.Result) {
	lt := n.Child[0].Result.(*LiteralTime).Value
	nanosecond := n.Child[2].Result.(int64)
	n.Result = &LiteralTime{
		NoUnit,
		time.Date(lt.Year(), lt.Month(), lt.Day(), lt.Hour(), lt.Minute(), lt.Second(), int(nanosecond), zero.Location()),
		api.Nanosecond,
	}
}

func literalTime(n *goparsify.Result) {
	lt := n.Child[0].Child[1].Result.(*LiteralTime)
	lt.Unit = getUnitType(n)
	n.Result = lt
}

// bindOpResult is a helper to generate a goparisfy Map function that will set
// an Operator value as the result. This returns a new instance every time
// rather than a shared one to stop issues in the event that something consuming
// the parsed output mutates the Operator instance it was given.
func bindOpResult(opResult rpc.Operator) func(n *goparsify.Result) {
	return func(n *goparsify.Result) {
		n.Result = &Operator{Value: opResult}
	}
}

// literalSet is a distinct set of Terms. It uses the Term's String method to
// define whether or not a Term has previously been seen.
func literalSet(n *goparsify.Result) {
	// 0: '{'
	// 1: Cut()
	// 2: optionalWS
	// 3: ${terms}
	// 4: optionalWS
	// 5: '}'
	set := &LiteralSet{
		Values: make([]Term, 0, len(n.Child[3].Child)),
	}
	seen := make(map[string]bool, len(n.Child[3].Child))
	for _, c := range n.Child[3].Child {
		term := c.Result.(Term)
		str := term.String()
		if seen[str] {
			continue
		}
		seen[str] = true
		set.Values = append(set.Values, term)
	}
	n.Result = set
}

func where(n *goparsify.Result) {
	quads := make(WhereClause, 0, len(n.Child))
	for _, c := range n.Child {
		if c.Result == nil {
			// Result will be nil for comment lines.
			continue
		}
		quads = append(quads, c.Result.(*Quad))
	}
	n.Result = quads
}

func limitOffset(n *goparsify.Result) {
	limit := n.Child[0].Result.(uint64)
	res := LimitOffset{
		Limit: &limit,
	}
	if n.Child[1].Result != nil {
		offset := n.Child[1].Result.(uint64)
		res.Offset = &offset
	}
	n.Result = res
}

func offsetLimit(n *goparsify.Result) {
	offset := n.Child[0].Result.(uint64)
	res := LimitOffset{
		Offset: &offset,
	}
	if n.Child[1].Result != nil {
		limit := n.Child[1].Result.(uint64)
		res.Limit = &limit
	}
	n.Result = res
}

func orderBy(direction SortDirection) func(*goparsify.Result) {
	return func(n *goparsify.Result) {
		n.Result = OrderCondition{
			On:        n.Result.(*Variable),
			Direction: direction,
		}
	}
}

func orderBys(n *goparsify.Result) {
	conditions := n.Child[4]
	res := make([]OrderCondition, 0, len(conditions.Child))
	for _, child := range conditions.Child {
		res = append(res, child.Result.(OrderCondition))
	}
	n.Result = res
}

func aggExpr(n *goparsify.Result) {
	res := AggregateExpr{
		Function: n.Child[0].Result.(AggregateFunction),
	}
	if n.Child[2].Token == "*" {
		res.Of = Wildcard{}
	} else {
		res.Of = n.Child[2].Result.(*Variable)
	}
	n.Result = &res
}

func boundExpr(n *goparsify.Result) {
	res := BoundExpression{
		Expr: n.Child[1].Result.(Expression),
		As:   n.Child[3].Result.(*Variable),
	}
	n.Result = &res
}

func selectExprs(n *goparsify.Result) {
	if n.Token == "*" {
		n.Result = []selectClauseItem{Wildcard{}}
		return
	}
	res := make([]selectClauseItem, 0, len(n.Child))
	for _, c := range n.Child {
		res = append(res, c.Result.(selectClauseItem))
	}
	n.Result = res
}

// child is a helper to generate a goparsify Map function that will grab a child
// result at a specific index and set it as the result for this node. This is
// useful for picking out the interesting part of a Seq().
func child(idx int) func(*goparsify.Result) {
	return func(n *goparsify.Result) {
		n.Result = n.Child[idx].Result
	}
}

func selectQuery(n *goparsify.Result) {
	// selectClauseKeyword is optional, so might be nil
	var kw selectClauseKeyword
	if n.Child[1].Result != nil {
		kw = n.Child[1].Result.(selectClauseKeyword)
	}
	q := Query{
		Type: SelectQuery,
		Select: SelectClause{
			Keyword: kw,
			Items:   n.Child[2].Result.([]selectClauseItem),
		},
		Where: n.Child[3].Result.(WhereClause),
	}
	// these 2 are optional, so might be nil
	if n.Child[4].Result != nil {
		q.Modifiers.OrderBy = n.Child[4].Result.([]OrderCondition)
	}
	if n.Child[5].Result != nil {
		q.Modifiers.Paging = n.Child[5].Result.(LimitOffset)
	}
	n.Result = &q
}

func askQuery(n *goparsify.Result) {
	q := Query{
		Type:  AskQuery,
		Where: n.Child[1].Result.(WhereClause),
	}
	n.Result = &q
}

func wrapWhere(n *goparsify.Result) {
	q := Query{
		Type:  LegacyPatternQuery,
		Where: n.Result.(WhereClause),
	}
	n.Result = &q
}
