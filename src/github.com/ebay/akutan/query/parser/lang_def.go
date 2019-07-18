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

	"github.com/ebay/akutan/rpc"
	p "github.com/vektah/goparsify"
)

var (
	// zero is used to fill in time fields for literal times.
	zero = &time.Time{}
	// literal is the parser function called by ParseLiteral. It extracts object
	// literal values.
	literal p.Parser
	// queryRootWhere is the parser function called by Parse. It extracts the
	// query in its entirety. The query must be in the legacy format of the
	// contents of a where clause. Queries that specify the QueryFactPattern
	// format use this parser.
	queryRootWhere p.Parser
	// queryRootSelectAsk is the parser function called by Parse. It extracts the
	// query in its entirety. The query must be a SELECT or ASK query. Queries that
	// specify the QuerySparql format use this parser.
	queryRootSelectAsk p.Parser
	// term is the parser function called by ParseTerm. It extracts terms comprised
	// of variables, operators, entities and literals. See below.
	term p.Parser
)

func init() {
	// If you need to debug what the parser is doing, you can enable goparsify's
	// built in debug support by building with -tags debug. See the docs for
	// more details https://github.com/vektah/goparsify#debugging-parsers
	//
	// The parser_debug.go file will setup sending the parser debug output to
	// stdOut when the debug tag is used.

	// unbroken character sequence used by various terms
	id := p.Chars("A-Za-z0-9_", 1)
	// unbroken character sequence used by various terms
	qnameChars := p.Chars("A-Za-z0-9%()_\\-", 1)
	// entity character sequence includes additional special characters
	entityID := p.Chars("A-Za-z0-9%()_\\-.,:", 1)
	// character sequence separating query lines
	lineSep := p.Chars("\n \t", 1)
	// character sequence separating query terms
	termSep := p.Chars(" \t", 1)

	entity := p.Seq("<", p.Cut(), entityID, ">").Map(func(n *p.Result) { // <createdOn>
		// TODO: Once parser accept Unicode for entity, do Unicode normalization.
		n.Result = &Entity{Value: n.Child[2].Token}
	})
	variable := p.Seq("?", id).Map(func(n *p.Result) { // ?s
		// TODO: Once parser accept Unicode for variable, do Unicode normalization.
		n.Result = &Variable{n.Child[1].Token}
	})
	qname := p.Seq(id, ":", qnameChars).Map(func(n *p.Result) { // rdfs:label
		// TODO: Once parser accept Unicode for QName, do Unicode normalization.
		n.Result = &QName{Value: fmt.Sprintf("%s%s%s", n.Child[0].Token, n.Child[1].Token, n.Child[2].Token)}
	})
	unit := p.Seq("^^", p.Any(entity, qname)).Map(unit)              // ^^<degrees> || ^^xsd:date
	lang := p.Seq("@", id).Map(lang)                                 // @en || @jp
	literalID := p.Seq("#", uint64Literal()).Map(func(n *p.Result) { // #12345
		n.Result = &LiteralID{Value: n.Child[1].Result.(uint64)}
	})
	literalBool := p.Seq(p.Any("true", "false"), p.Maybe(unit)).Map(literalBool) // true || false
	literalNumber := p.Seq(p.NumberLit(), p.Maybe(unit)).Map(literalNumber)      // 9 || 3.14159
	literalString := p.Seq(p.StringLit(`"`), p.Maybe(lang)).Map(literalString)   // "Samsung Group"

	literalTimeY := fixedLengthInt(4).Map(literalTimeY) // '2018-01-02 03:04:05.999999'
	literalTimeYM := p.Seq(literalTimeY, "-", fixedLengthInt(2)).Map(literalTimeYM)
	literalTimeYMD := p.Seq(literalTimeYM, "-", fixedLengthInt(2)).Map(literalTimeYMD)
	literalTimeYMDH := p.Seq(literalTimeYMD, " ", fixedLengthInt(2)).Map(literalTimeYMDH)
	literalTimeYMDHM := p.Seq(literalTimeYMDH, ":", fixedLengthInt(2)).Map(literalTimeYMDHM)
	literalTimeYMDHMS := p.Seq(literalTimeYMDHM, ":", fixedLengthInt(2)).Map(literalTimeYMDHMS)
	literalTimeYMDHMSN := p.Seq(literalTimeYMDHMS, ".", intLiteral(32)).Map(literalTimeYMDHMSN)
	literalTime := p.Seq(p.Seq("'", p.Any(
		literalTimeYMDHMSN, literalTimeYMDHMS, literalTimeYMDHM, literalTimeYMDH,
		literalTimeYMD, literalTimeYM, literalTimeY), "'"), p.Maybe(unit)).Map(literalTime)

	opEQ := p.Exact("<eq>").Map(bindOpResult(rpc.OpEqual))
	opNEQ := p.Exact("<notEqual>").Map(bindOpResult(rpc.OpNotEqual))
	opGT := p.Exact("<gt>").Map(bindOpResult(rpc.OpGreater))
	opGTE := p.Exact("<gte>").Map(bindOpResult(rpc.OpGreaterOrEqual))
	opLT := p.Exact("<lt>").Map(bindOpResult(rpc.OpLess))
	opLTE := p.Exact("<lte>").Map(bindOpResult(rpc.OpLessOrEqual))
	opIN := p.Exact("<in>").Map(bindOpResult(rpc.OpIn))
	operator := p.Any(opEQ, opGT, opGTE, opLT, opLTE, opIN, opNEQ)

	literal = p.Any(literalBool, literalID, literalNumber, literalString, literalTime) // bool || ID || float64 || int64 || string || time
	optionalWS := p.Chars(" \t", 0)
	literalSetSep := p.Seq(optionalWS, ",", optionalWS)
	literalSetTerm := p.Any(literal, entity, qname)
	literalSet := p.Seq("{", p.Cut(), optionalWS, repeatZeroOrMore(literalSetTerm, literalSetSep), optionalWS, "}").Map(literalSet)
	term = p.Any(literalSet, variable, operator, entity, qname, literal)
	optionalMatch := p.Maybe(p.Bind(p.Exact("?"), MatchOptional))
	termAndSpecificity := p.Seq(term, optionalMatch)
	termLine := termLineParser(termAndSpecificity, termSep, lineSep)
	// comment line starts with '#' followed by a white space
	comment := p.Seq("# ", p.Until("\n"))
	line := p.Any(termLine, comment)
	// the 3 or 4 terms per line model used in the WHERE clause needs the parsers to manually manage Whitespace.
	whereContents := p.NoAutoWS(repeatOneOrMore(line, lineSep).Map(where))

	// SolutionModifiers
	limit := p.Seq(ignoreCase("LIMIT"), p.Cut(), uint64Literal()).Map(child(2))
	offset := p.Seq(ignoreCase("OFFSET"), p.Cut(), uint64Literal()).Map(child(2))
	limitOffset := p.Any(
		p.Seq(limit, p.Maybe(offset)).Map(limitOffset),
		p.Seq(offset, p.Maybe(limit)).Map(offsetLimit))
	orderByItem := p.Any(
		variable.Map(orderBy(SortAsc)),
		p.Seq(ignoreCase("ASC"), "(", variable, ")").Map(child(2)).Map(orderBy(SortAsc)),
		p.Seq(ignoreCase("DESC"), "(", variable, ")").Map(child(2)).Map(orderBy(SortDesc)))
	orderBy := p.Seq(ignoreCase("ORDER"), p.Cut(), ignoreCase("BY"), p.Cut(), repeatOneOrMore(orderByItem)).Map(orderBys)

	distinct := ignoreCase("DISTINCT").Map(func(n *p.Result) {
		n.Result = Distinct{}
	})
	reduced := ignoreCase("REDUCED").Map(func(n *p.Result) {
		n.Result = Reduced{}
	})
	selectClauseKeyword := p.Maybe(p.Any(distinct, reduced))

	expr := p.Seq(functionNameParser(), "(", p.Any("*", variable), ")").Map(aggExpr)
	boundExpr := p.Seq("(", expr, ignoreCase("AS"), variable, ")").Map(boundExpr)
	selectExprs := p.Any("*", repeatOneOrMore(p.Any(boundExpr, variable))).Map(selectExprs)

	whereClause := p.Seq(p.Any("{", p.Seq(ignoreCase("WHERE"), "{")), sparqlWS, whereContents, "}").Map(child(2))
	selectQuery := p.Seq(ignoreCase("SELECT"), selectClauseKeyword, selectExprs,
		whereClause, p.Maybe(orderBy), p.Maybe(limitOffset)).Map(selectQuery)
	selectQuery = withWhitespace(sparqlWS, selectQuery)

	askQuery := withWhitespace(sparqlWS, p.Seq(ignoreCase("ASK"), whereClause).Map(askQuery))

	queryRootSelectAsk = p.Any(askQuery, selectQuery)
	queryRootWhere = whereContents.Map(wrapWhere)
}
