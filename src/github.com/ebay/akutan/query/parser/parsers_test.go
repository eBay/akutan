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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vektah/goparsify"
)

func Test_WithWhitespace(t *testing.T) {
	ws := func(update *bool) func(*goparsify.State) {
		return func(*goparsify.State) {
			*update = true
		}
	}
	var wsBobCalled, wsAliceCalled bool
	p := goparsify.Any(withWhitespace(ws(&wsAliceCalled), "Alice"), withWhitespace(ws(&wsBobCalled), "Bob"))
	goparsify.Run(p, "Eve")
	assert.True(t, wsBobCalled)
	assert.True(t, wsAliceCalled)
}

func Test_SparqlWS(t *testing.T) {
	trimLeft := func(in string) string {
		s := goparsify.State{Input: in}
		sparqlWS(&s)
		return in[s.Pos:]
	}
	assert.Equal(t, "", trimLeft(" \t\r\n \t"))
	assert.Equal(t, "bob ", trimLeft("bob "))
	assert.Equal(t, "hello", trimLeft("# hello word\n\thello"))
	assert.Equal(t, "hello", trimLeft("\t# hello word\r\n    hello"))
	assert.Equal(t, "", trimLeft("#"))
	assert.Equal(t, "", trimLeft("#\n"))
}

func Test_IgnoreCaseParser(t *testing.T) {
	parse := func(p goparsify.Parser, input string, expToken string, expError string) {
		res, err := goparsify.Run(p.Map(func(n *goparsify.Result) {
			n.Result = n.Token
		}), input, sparqlWS)
		if expError == "" {
			assert.NoError(t, err)
			assert.Equal(t, expToken, res)
		} else {
			assert.EqualError(t, err, expError)
		}
	}
	parse(ignoreCase("HELLO"), "hello", "hello", "")
	parse(ignoreCase("HELLO"), "HELLO", "HELLO", "")
	parse(ignoreCase("Bob"), "bOb", "bOb", "")
	parse(ignoreCase("Bob123"), "BOB123", "BOB123", "")
	parse(ignoreCase("WHERE"), "   \t WHERE", "WHERE", "")
	parse(ignoreCase("SELECT"), "ASK", "", "offset 0: expected SELECT")
	parse(ignoreCase("WHERE"), "   \t WHEREx", "", "left unparsed: x")
}
