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

package diskview

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_parseHashRange(t *testing.T) {
	tests := []struct {
		name   string
		exp    string
		expErr string
		first  string
		last   string
	}{
		{
			name:  "full",
			exp:   "0x0000000000000000 to ∞",
			first: "0x0000000000000000",
			last:  "0xffffffffffffffff",
		},
		{
			name:  "first half",
			exp:   "0x0000000000000000 to 0x8000000000000000",
			first: "0x0000000000000000",
			last:  "0x7fffffffffffffff",
		},
		{
			name:  "second half",
			exp:   "0x8000000000000000 to ∞",
			first: "0x8000000000000000",
			last:  "0xffffffffffffffff",
		},
		{
			name:  "empty",
			exp:   "0x0000000000000000 to 0x0000000000000001",
			first: "0x0000000000000000",
			last:  "0x0000000000000000",
		},
		{
			name:   "first > last",
			expErr: "first hash cannot be greater than last hash",
			first:  "0x7fffffffffffffff",
			last:   "0x0000000000000000",
		},
		{
			name:   "first hash bad",
			expErr: "invalid first hash: parseHex64: bad input format (need 0x%016x)",
			first:  "0x000",
			last:   "0x7ff",
		},
		{
			name:   "last hash bad",
			expErr: "invalid last hash: parseHex64: bad input format (need 0x%016x)",
			first:  "0x0000000000000000",
			last:   "0xabcdefghijklmnop",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			rng, err := parseHashRange(test.first, test.last)
			if test.expErr == "" {
				assert.NoError(err)
				assert.Equal(test.exp, fmt.Sprintf("%v to %v", rng.Start, rng.End))
			} else {
				assert.Equal("", test.exp, "Test cannot expect both value and error")
				assert.EqualError(err, test.expErr)
				assert.Nil(rng.Start)
				assert.Nil(rng.End)
			}
		})
	}
}

func Test_parseHex64(t *testing.T) {
	tests := []struct {
		exp    uint64
		expErr bool
		input  string
	}{
		{
			exp:   0,
			input: "0x0000000000000000",
		},
		{
			exp:   math.MaxUint64,
			input: "0xffffffffffffffff",
		},
		{
			exp:   0xc000000000000000,
			input: "0xc000000000000000",
		},
		{ // uppercase is OK
			exp:   0xffffffffffffffff,
			input: "0xFFFFFFFFFFFFFFFF",
		},
		{
			expErr: true,
			input:  "0x000",
		},
		{
			expErr: true,
			input:  "0000000000000000",
		},
		{
			expErr: true,
			input:  "000000000000000000",
		},
		{
			expErr: true,
			input:  "0x00000000000000000",
		},
		{
			expErr: true,
			input:  "0x0000000000000000-",
		},
		{
			expErr: true,
			input:  "0xabcdefghijklmnop",
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			assert := assert.New(t)
			actual, err := parseHex64(test.input)
			if test.expErr {
				assert.Equal(uint64(0), test.exp, "Test cannot expect both value and error")
				assert.Equal(errParseHex64, err)
				assert.Equal(uint64(0), actual)
			} else {
				assert.NoError(err)
				assert.Equal(test.exp, actual)
			}
		})
	}
}
