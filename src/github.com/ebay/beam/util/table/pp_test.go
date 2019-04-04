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

package table

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_PrettyPrint_utf8(t *testing.T) {
	assert := assert.New(t)
	var buf strings.Builder
	PrettyPrint(&buf, [][]string{
		{"Beyoncé"},
		{"A longer thing"},
	}, RightJustify)
	assert.Equal(`
        Beyoncé |
 A longer thing |
`, "\n"+buf.String())

	buf.Reset()
	PrettyPrint(&buf, [][]string{
		{"Beyoncé"},
		{"shrt"},
	}, RightJustify)
	assert.Equal(`
 Beyoncé |
    shrt |
`, "\n"+buf.String())
}

func Test_PrettyPrintJustify(t *testing.T) {
	table := [][]string{
		{"employee", "manager"},
		{"bob", "alice"},
	}
	// default is left
	t.Run("Left", func(t *testing.T) {
		buf := strings.Builder{}
		PrettyPrint(&buf, table, HeaderRow)
		assert.Equal(t, `
 employee | manager |
 -------- | ------- |
 bob      | alice   |
`, "\n"+buf.String())
	})
	t.Run("Right", func(t *testing.T) {
		buf := strings.Builder{}
		PrettyPrint(&buf, table, HeaderRow|RightJustify)
		assert.Equal(t, `
 employee | manager |
 -------- | ------- |
      bob |   alice |
`, "\n"+buf.String())
	})
}

func Test_SkipEmtpy(t *testing.T) {
	assert := assert.New(t)
	headers := [][]string{
		{"model", "make"},
	}
	buf := strings.Builder{}
	PrettyPrint(&buf, headers, SkipEmpty|HeaderRow)
	assert.Equal("", buf.String())

	buf.Reset()
	PrettyPrint(&buf, headers, SkipEmpty|FooterRow)
	assert.Equal("", buf.String())

	buf.Reset()
	PrettyPrint(&buf, headers, SkipEmpty)
	assert.Equal(" model | make |\n", buf.String())
}

func Test_MultilineCell(t *testing.T) {
	assert := assert.New(t)
	table := [][]string{
		{"car", "rating"},
		{"Ford\nRaptor", "4"},
	}
	buf := strings.Builder{}
	PrettyPrint(&buf, table, HeaderRow)
	assert.Equal(`
 car    | rating |
 ------ | ------ |
 Ford   | 4      |
 Raptor |        |
`, "\n"+buf.String())
}
func Test_charsWide(t *testing.T) {
	assert := assert.New(t)
	tests := []struct {
		s string
		w int
	}{
		{"Aeyonce", 7},
		{"Beyoncé", 7},
		{"Ceyonce\u0301", 7},
		{"Deyonc\u00e9", 7},
	}
	for _, test := range tests {
		assert.Equal(test.w, charsWide(test.s), "Incorrect width for %#v", test.s)
	}
}
