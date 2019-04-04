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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vektah/goparsify"
)

func TestParseLiteral(t *testing.T) {
	assert := assert.New(t)
	for i, tc := range literals() {
		p := &parser{in: tc.in}
		out, err := p.parseLiteral()
		assert.NoError(err, "literal%d did not parse", i+1)
		assert.Equal(tc.parsed, out, "literal%d is incorrect: %s: %s<>%s", i+1, tc.parsed, out)
		lit := out.Literal()
		assert.NotNil(lit, "literal%d is nil", i+1)
	}

	p := parser{in: "true true"}
	out, err := p.parseLiteral()
	assert.Nil(out, "%s", p.in)
	assert.EqualError(err, "unable to parse literal: line 1 column 6: unparsed text: 'true'")

	p = parser{in: "?v"}
	out, err = p.parseLiteral()
	assert.Nil(out, "%s", p.in)
	assert.EqualError(err, `unable to parse literal: line 1 column 1: expected '`)

	p = parser{in: "?a ?b ?c\n?a <in> {?c}"}
	q, err := p.parseQuery(QueryFactPattern)
	assert.Nil(q)
	assert.EqualError(err, `unable to parse query: line 2 column 10: expected }`)
}

func TestParseTerm(t *testing.T) {
	for i, tc := range terms() {
		t.Run(tc.in, func(t *testing.T) {
			assert := assert.New(t)
			p := &parser{in: tc.in}
			out, err := p.parseTerm()
			assert.NoError(err, "term %d:%s did not parse", i, tc.in)
			assert.Equal(tc.parsed, out, "term %d:%s is incorrect", i, tc.in)
		})
	}
	assert := assert.New(t)
	p := parser{in: "?a ?b ?c ?d"}
	out, err := p.parseTerm()
	assert.Error(err, "%s", p.in)
	assert.Nil(out, "%s", p.in)
	assert.EqualError(err, "unable to parse term: line 1 column 4: unparsed text: '?b ?c ?d'")
}

func Test_SetLiteral_ValueDeduper(t *testing.T) {
	// These should all be considered different
	// by the value de-duper in set literals.
	differentTypes := []string{
		"2018",   // int
		"2018.0", // float
		"'2018'", // year
		`"2018"`, // string
		"#2018",  // kid
		"<2018>", // entity
	}
	t.Run("NotSame", func(t *testing.T) {
		for idx, a := range differentTypes {
			for _, b := range differentTypes[idx+1:] {
				term, err := ParseTerm(fmt.Sprintf("{ %s, %s }", a, b))
				if assert.NoError(t, err, "error parsing { %s, %s }", a, b) {
					assert.NotNil(t, term)
					set := term.(*LiteralSet)
					assert.Equal(t, 2, len(set.Values), "unexpected number of values for { %s, %s }", a, b)
				}
			}
		}
	})
	t.Run("Same", func(t *testing.T) {
		for _, a := range differentTypes {
			term, err := ParseTerm(fmt.Sprintf("{ %s, %s }", a, a))
			if assert.NoError(t, err, "error parsing { %s, %s }", a, a) {
				assert.NotNil(t, term)
				set := term.(*LiteralSet)
				assert.Equal(t, 1, len(set.Values))
			}
		}
	})
}

func Test_ParseError(t *testing.T) {
	err := &ParseError{
		ParseType: "query",
		Input:     "SELECTT * WHERE {}",
		Offset:    6,
		Line:      1,
		Column:    7,
		Details:   `expected: "*"`,
	}
	assert.EqualError(t, err, `unable to parse query: line 1 column 7: expected: "*"`)
}

func Test_ExpectedText(t *testing.T) {
	// its not possible to construct a goparsify.Error from outside the package,
	// so we force the parser to generate one.
	_, err := goparsify.Run("Bob", "Alice")
	assert.Error(t, err)
	assert.Equal(t, "Bob", expectedText(err.(*goparsify.Error)))
}

func Test_ParseQueryFormatError(t *testing.T) {
	_, err := Parse("bob", "where is Bob?")
	assert.EqualError(t, err, "query format 'bob' is not valid")
}

func Test_Coordinates(t *testing.T) {
	tests := []struct {
		input   string
		pos     int
		expLine int
		expCol  int
	}{
		{"Hello World", 0, 1, 1},
		{"Hello World", 5, 1, 6},
		{"Hello World", 11, 1, 12},
		{"Hello World", 12, 1, 12},
		{"Hello World\n\n\n", 14, 1, 12}, // in the trailing whitespace
		{"Hello\nWorld", 5, 1, 6},
		{"Hello\nWorld", 6, 2, 1},
		{"世界\n世界", 0, 1, 1}, // each of these chars is 3 bytes in utf8
		{"世界\n世界", 3, 1, 2},
		{"世界\n世界", 6, 1, 3},
		{"世界\n世界", 7, 2, 1},
		{"世界\n世界", 10, 2, 2},
		{"世界\n世界", 13, 2, 3},
		{"Hello\nWorld\n", 10, 2, 5},
		{"Hello\nWorld\n", 11, 2, 6},
		{"Hello\nWorld\n", 12, 2, 6},
		{"Hello\nWorld\n", 13, 2, 6},
		{"Hello\nWorld\n", 24, 2, 6}, // way past the end of input
		{"\n\n\nHello\nWorld\n", 4, 4, 2},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%s_%d", test.input, test.pos), func(t *testing.T) {
			line, col := coordinates(test.input, test.pos)
			assert.Equal(t, test.expLine, line, "Incorrect calculated line")
			assert.Equal(t, test.expCol, col, "Incorrect calculated column")
		})
	}
}
