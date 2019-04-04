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

package unicode

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Normalize(t *testing.T) {
	type tc struct {
		name string
		in   string
		exp  string
	}
	tests := []tc{
		{"empty", "", ""},
		{"two_runes", "Beyonce\u0301", "Beyonc\u00e9"},
		{"one_rune", "Beyonc\u00e9", "Beyonc\u00e9"},
		{"simple_text", "?bob", "?bob"},
		{"ordering_of_marks", "eq\u0307\u0323uivalent", "eq\u0323\u0307uivalent"}, // q+◌̣+◌ == q+◌̇+◌̣
		{"superscript_digit", "e\u2079", "e\u2079"},                               // superscript nine(⁹) should not be normalized to digit 9
		{"digits", "95125", "95125"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.exp, Normalize(test.in))
		})
	}
}
