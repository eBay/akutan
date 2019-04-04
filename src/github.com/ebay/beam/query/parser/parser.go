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
	"unicode"
	"unicode/utf8"

	"github.com/ebay/beam/util/cmp"
	"github.com/sirupsen/logrus"
	"github.com/vektah/goparsify"
)

const (
	// QuerySparql indicates that the query is in the sparql(ish) subset format.
	QuerySparql = "sparql"
	// QueryFactPattern indicates that the query is a bare set of fact patterns to match.
	QueryFactPattern = "facts"
)

// MustParse parses the beam query language and panics if an error occurs.  It simplifies
// variable initialization. This is primarily meant for writing unit tests.
func MustParse(format, in string) *Query {
	query, err := Parse(format, in)
	if err != nil {
		panic(fmt.Sprintf("unable to parse query: '%s': %v", strings.Replace(in, "\n", "\\n", -1), err))
	}
	return query
}

// Parse parses a query of the specified format and builds a query definition.
// format can be either QuerySparl or QueryFactPattern
func Parse(format, in string) (*Query, error) {
	p := &parser{in: in}
	return p.parseQuery(format)
}

// ParseLiteral parses an object literal value.
func ParseLiteral(in string) (Literal, error) {
	p := &parser{in: in}
	return p.parseLiteral()
}

// ParseTerm parses a single term.
func ParseTerm(in string) (Term, error) {
	p := &parser{in: in}
	return p.parseTerm()
}

// parser implementation
type parser struct {
	in string
}

// parse reads the query and returns a beamql.Query object. If its unable to
// fully parse the input a ParseError will be returned that includes the
// position of where it parsed to, and what the problem is.
func (p *parser) parse(typ string, parser goparsify.Parser) (*goparsify.Result, error) {
	// parse the query; see lang_def.go for the combinator semantics
	state := goparsify.NewState(p.in)
	state.WS = goparsify.NoWhitespace
	// consume head whitespace
	goparsify.UnicodeWhitespace(state)

	result := &goparsify.Result{}
	parser(state, result)
	if state.Errored() {
		line, col := coordinates(p.in, state.Error.Pos())
		exp := strings.TrimPrefix(fmt.Sprintf("%q", expectedText(&state.Error)), `"`)
		exp = strings.TrimSuffix(exp, `"`)
		return nil, &ParseError{
			ParseType: typ,
			Input:     p.in,
			Offset:    state.Error.Pos(),
			Line:      line,
			Column:    col,
			Details:   "expected " + exp,
		}
	}
	// consume tail whitespace and check for unparsed text
	state.WS = goparsify.UnicodeWhitespace
	state.WS(state)
	unparsed := state.Get()
	if unparsed != "" {
		line, col := coordinates(p.in, state.Pos)
		return nil, &ParseError{
			ParseType: typ,
			Input:     p.in,
			Offset:    state.Pos,
			Line:      line,
			Column:    col,
			Details:   fmt.Sprintf("unparsed text: '%s'", strings.TrimRightFunc(unparsed, unicode.IsSpace)),
		}
	}
	return result, nil
}

// ParseError captures more detailed information about a parsing error, and
// where it occurred.
type ParseError struct {
	// query, literal, etc.
	ParseType string
	// The input string to the parser which resulted in this error.
	Input string
	// Offset is the byte offset into 'Input' at which the error ocurred.
	Offset int
	// Line is the line number in 'Input' at which the error ocurred.
	Line int
	// Column is the column (in runes) into the indicated Line that the error
	// ocurred. Line & Column represent the same point in 'Input' as 'Offset'.
	Column int
	// The specific parser error that ocurred.
	Details string
}

func (p *ParseError) Error() string {
	return fmt.Sprintf("unable to parse %s: line %d column %d: %s",
		p.ParseType, p.Line, p.Column, p.Details)
}

// coordinates returns the line & column of the supplied offset in the string
// 'input'. Offset is in bytes, the returned column value is in runes.
func coordinates(input string, atOffset int) (line, col int) {
	// Trim any trailing whitespace from the input, as most people wouldn't
	// consider it an expected place for an error.
	input = strings.TrimRightFunc(input, unicode.IsSpace)
	// Don't let atOffset be past the end of the input.
	atOffset = cmp.MinInt(atOffset, len(input))

	lines := strings.Split(input, "\n")
	current := 0
	line = 1
	for _, l := range lines {
		if current+len(l) >= atOffset {
			// offset is in bytes, but the reported column should be based on runes.
			col = utf8.RuneCountInString(l[:atOffset-current]) + 1
			return line, col
		}
		line++
		current += len(l) + 1 // remember to consume the \n
	}
	panic(fmt.Sprintf("shouldn't get here. Input was '%s' atOffset: %d", input, atOffset))
}

// expectedText extracts from the supplied goparsify Error the expected text
// i.e. the error from an unmatched parser. This relies on the format of the
// error message generated by goparsify.
func expectedText(e *goparsify.Error) string {
	msg := e.Error()
	expectedIdx := strings.Index(msg, "expected")
	if expectedIdx == -1 {
		logrus.WithField("err", msg).
			Warn("Got goparsify error with missing 'expected' string")
		return msg
	}
	expected := msg[expectedIdx+len("expected")+1:]
	return expected
}

// parseLiteral parses an object literal value.
func (p *parser) parseLiteral() (Literal, error) {
	result, err := p.parse("literal", literal)
	if err != nil {
		return nil, err
	}
	literal, ok := result.Result.(Literal)
	if !ok {
		return nil, fmt.Errorf("invalid result type: %T", result.Result)
	}
	return literal, nil
}

// parseQuery parses the entire query using the supplied parser.
func (p *parser) parseQuery(format string) (*Query, error) {
	var root goparsify.Parser
	switch format {
	case QuerySparql:
		root = queryRootSelectAsk
	case QueryFactPattern:
		root = queryRootWhere
	default:
		return nil, fmt.Errorf("query format '%s' is not valid", format)
	}
	result, err := p.parse("query", root)
	if err != nil {
		return nil, err
	}
	query, ok := result.Result.(*Query)
	if !ok {
		return nil, fmt.Errorf("invalid result type: %T", result.Result)
	}
	return query, nil
}

// parseTerm parses a term.
func (p *parser) parseTerm() (Term, error) {
	result, err := p.parse("term", term)
	if err != nil {
		return nil, err
	}
	term, ok := result.Result.(Term)
	if !ok {
		return nil, fmt.Errorf("invalid result type: %T", result.Result)
	}
	return term, nil
}
