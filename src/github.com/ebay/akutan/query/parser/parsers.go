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
	"sort"
	"strconv"
	"strings"

	"github.com/vektah/goparsify"
)

// repeatZeroOrMore matches zero or more parsers and returns the value as
// .Child[n]. An optional separator can be provided and that value will be
// consumed but not returned. Only one separator can be provided.
//
// This and repeatOneOrMore exist because the difference between Some & Many is
// not obvious from the name.
func repeatZeroOrMore(p goparsify.Parserish, sep ...goparsify.Parserish) goparsify.Parser {
	return goparsify.Some(p, sep...)
}

// repeatOneOrMore matches one or more parsers and returns the value as
// .Child[n]. An optional separator can be provided and that value will be
// consumed but not returned. Only one separator can be provided.
func repeatOneOrMore(p goparsify.Parserish, sep ...goparsify.Parserish) goparsify.Parser {
	return goparsify.Many(p, sep...)
}

// fixedLengthInt parses out fixed length integers
func fixedLengthInt(length int) goparsify.Parser {
	description := fmt.Sprintf("fixedLengthInt:%d", length)
	return goparsify.NewParser(description, func(ps *goparsify.State, node *goparsify.Result) {
		ps.WS(ps)
		maxPos := ps.Pos
		len := len(ps.Input)
		for i := 0; i < length; i++ {
			if maxPos < len && ps.Input[maxPos] >= '0' && ps.Input[maxPos] <= '9' {
				maxPos++
			}
		}
		var err error
		node.Result, err = strconv.Atoi(ps.Input[ps.Pos:maxPos])
		if err != nil {
			ps.ErrorHere(description)
			return
		}
		ps.Pos = maxPos
	})
}

// uint64Literal parses a uint64 in base 10 from state.
func uint64Literal() goparsify.Parser {
	return goparsify.NewParser("uint64Literal", func(ps *goparsify.State, node *goparsify.Result) {
		ps.WS(ps)
		maxPos := ps.Pos // mark how far we have come
		len := len(ps.Input)

		for maxPos < len && ps.Input[maxPos] >= '0' && ps.Input[maxPos] <= '9' {
			maxPos++
		}
		if maxPos == ps.Pos {
			ps.ErrorHere("number")
			return
		}
		var err error
		node.Result, err = strconv.ParseUint(ps.Input[ps.Pos:maxPos], 10, 64)
		if err != nil {
			ps.ErrorHere("number")
			return
		}
		ps.Pos = maxPos
	})
}

// intLiteral parses types of int from state.
func intLiteral(bitsize int) goparsify.Parser {
	description := fmt.Sprintf("int%d", bitsize)
	return goparsify.NewParser(description, func(ps *goparsify.State, node *goparsify.Result) {
		ps.WS(ps)
		maxPos := ps.Pos // mark how far we have come
		len := len(ps.Input)

		if maxPos < len && (ps.Input[maxPos] == '-' || ps.Input[maxPos] == '+') {
			maxPos++
		}
		for maxPos < len && ps.Input[maxPos] >= '0' && ps.Input[maxPos] <= '9' {
			maxPos++
		}
		if maxPos == ps.Pos {
			ps.ErrorHere("number")
			return
		}
		var err error
		node.Result, err = strconv.ParseInt(ps.Input[ps.Pos:maxPos], 10, bitsize)
		if err != nil {
			ps.ErrorHere("number")
			return
		}
		ps.Pos = maxPos
	})
}

// withWhitespace will set Auto Whitespace to 'ws' for parser and all its
// children. The original Whitespace settting will be restored once 'parser'
// returns.
func withWhitespace(ws goparsify.VoidParser, parserish goparsify.Parserish) goparsify.Parser {
	parser := goparsify.Parsify(parserish)
	return func(ps *goparsify.State, node *goparsify.Result) {
		oldWS := ps.WS
		ps.WS = ws
		parser(ps, node)
		ps.WS = oldWS
	}
}

// sparqlWS is a goparsify Whitespace parser that understands SPARQLs whitespace
// rules. Whitespace chars are ' ' \t \r \n only. # starts a comment which runs
// to the end of the line
func sparqlWS(s *goparsify.State) {
	for s.Pos < len(s.Input) {
		switch s.Input[s.Pos] {
		// The explicit whitespace chars in the SPARQL grammar
		case ' ', '\t', '\r', '\n':
			s.Pos++

		// Start of a comment, comment runs to the end of the line. Yes,
		// that means we have different comment styles inside the WHERE
		// clause to outside it.
		case '#':
			s.Pos++
			// consume the rest of the line
			for s.Pos < len(s.Input) {
				c := s.Input[s.Pos]
				s.Pos++
				if c == '\n' || c == '\r' {
					break
				}
			}
		default:
			return
		}
	}
}

// ignoreCase returns a parser that matches the supplied string exactly ignoring
// case.
func ignoreCase(match string) goparsify.Parser {
	lenMatch := len(match)
	return goparsify.NewParser("i/"+match+"/", func(s *goparsify.State, r *goparsify.Result) {
		s.WS(s)
		in := s.Get()
		if len(in) < lenMatch || !strings.EqualFold(match, in[:lenMatch]) {
			s.ErrorHere(match)
			return
		}
		s.Advance(lenMatch)
		r.Token = in[:lenMatch]
	})
}

// functionNameParser returns a parser that is used to parse function names in
// expressions. Its primary job is to generate better error messages.
func functionNameParser() goparsify.Parser {
	aggFunctions := map[string]AggregateFunction{
		"COUNT": AggCount,
	}
	fns := make([]string, 0, len(aggFunctions))
	for fn := range aggFunctions {
		fns = append(fns, fn)
	}
	sort.Strings(fns)
	// goparsify will put "expected " at the start of the final error message
	errMsg := strings.Join(fns, ", ")

	return goparsify.NewParser("function", func(s *goparsify.State, r *goparsify.Result) {
		s.WS(s)
		in := s.Get()
		for fn, val := range aggFunctions {
			if len(in) >= len(fn) && strings.EqualFold(fn, in[:len(fn)]) {
				r.Token = fn
				r.Result = val
				s.Advance(len(fn))
				return
			}
		}
		s.ErrorHere(errMsg)
	})
}

// termLineParser will parse a single term line in a WHERE clause. We need a
// specific parser for this to handle the fact that the first term is optional.
// Trying to do any(4term, 3term) doesn't work because we use Cut() in the term
// parsers, and so the 4term parser ends up calling Cut() stopping the remaining
// 3term parser from being attempted.
func termLineParser(termAndSpecificity, termSep, lineSep goparsify.Parser) goparsify.Parser {
	// termParser does the bulk of the actual parsing work, we're just going to
	// post process the results. This can't be done in a Map function, as they
	// can't set Error.
	termParser := repeatOneOrMore(termAndSpecificity, termSep)

	return goparsify.NewParser("termLine", func(s *goparsify.State, r *goparsify.Result) {
		start := s.Pos
		termParser(s, r)
		if s.Errored() {
			s.Pos = start
			return
		}
		// At this point we expect there to be a lineSep (or be at the end of
		// the input). Check that first so that remaining items on the line
		// generate better errors that the invalid number of term errors. The
		// outer lines parser expects to see the lineSep, so we have to peek
		// that.
		endTerms := s.Pos
		// we also need to set the Cut pos, otherwise the parser will just backtrack all the
		// way and try the other line options (like comment)
		s.Cut = endTerms
		if s.Pos < len(s.Input) {
			lineSep(s, goparsify.TrashResult)
			s.Pos = endTerms
			if s.Errored() {
				remaining := s.Input[endTerms:]
				endIndex := strings.IndexByte(remaining, '\n')
				if endIndex > 0 {
					remaining = remaining[:endIndex]
				}
				switch len(r.Child) {
				case 3, 4:
					s.ErrorHere(fmt.Sprintf("end of line, but got '%s'", remaining))
				case 1, 2:
					s.ErrorHere(fmt.Sprintf("a valid query term, but got '%s'", remaining))
				}
				s.Pos = start
				return
			}
		}
		// r has a list of [termSet, optionalMatch] results, the length of r is
		// dependent on the number of items in the line that the parser
		// extracted.
		term := func(result *goparsify.Result) Term {
			if n, ok := result.Child[0].Result.(Term); ok {
				return n
			}
			// at this point we have a result we were unable to parse
			return &Nil{}
		}
		disallowed := func(r *goparsify.Result, fieldName string) bool {
			// each item in the results has a Child of two items, the result
			// from 'termSet' and the result from 'optionalMatch'
			if r.Child[1].Result != nil {
				s.ErrorHere(fmt.Sprintf("not to find an optional marker on the %s field", fieldName))
				s.Pos = start
				return true
			}
			return false
		}
		res := Quad{ID: &Nil{}}
		var spo []goparsify.Result
		switch len(r.Child) {
		case 3:
			spo = r.Child[0:]
		case 4:
			if disallowed(&r.Child[0], "ID") {
				return
			}
			res.ID = term(&r.Child[0])
			spo = r.Child[1:]
		default:
			s.ErrorHere(fmt.Sprintf("to find 3 or 4 terms in a query line, you have %d", len(r.Child)))
			s.Pos = start
			return
		}
		res.Subject = term(&spo[0])
		res.Predicate = term(&spo[1])
		if optVal, isOptional := spo[1].Child[1].Result.(MatchSpecificity); isOptional {
			res.Specificity = optVal
		}
		res.Object = term(&spo[2])
		if disallowed(&spo[0], "Subject") || disallowed(&spo[2], "Object") {
			return
		}
		// Disallow nil's in string literals. This is required to ensure strings
		// sort correctly in KGObject.
		if litString, ok := res.Object.(*LiteralString); ok {
			if strings.IndexByte(litString.Value, 0) >= 0 {
				s.ErrorHere("string literal to not contain a nil")
				s.Pos = start
				return
			}
		}
		r.Result = &res
	})
}
