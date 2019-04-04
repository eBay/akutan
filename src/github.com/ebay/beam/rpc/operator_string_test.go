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

package rpc

import (
	"testing"

	"github.com/ebay/beam/util/cmp"
	"github.com/stretchr/testify/assert"
)

type stringTest struct {
	obj Operator
	exp string
}

var stringTests = []stringTest{
	{obj: OpEqual, exp: "="},
	{obj: OpNotEqual, exp: "!="},
	{obj: OpLess, exp: "<"},
	{obj: OpLessOrEqual, exp: "<="},
	{obj: OpGreater, exp: ">"},
	{obj: OpGreaterOrEqual, exp: ">="},
	{obj: OpRangeIncExc, exp: "rangeIncExc"},
	{obj: OpRangeIncInc, exp: "rangeIncInc"},
	{obj: OpRangeExcInc, exp: "rangeExcInc"},
	{obj: OpRangeExcExc, exp: "rangeExcExc"},
	{obj: OpPrefix, exp: "prefix"},
	{obj: OpIn, exp: "in"},
	{obj: Operator(2350), exp: "unknown(2350)"},
}

func TestStringKey(t *testing.T) {
	assert := assert.New(t)
	for i, t := range stringTests {
		assert.Equal(t.exp, t.obj.String(), "obj%d: invalid string", i)
		assert.Equal(t.exp, cmp.GetKey(t.obj), "obj%d: invalid key", i)
	}
}
