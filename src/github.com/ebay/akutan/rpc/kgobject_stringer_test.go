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
	"math"
	"testing"

	"github.com/ebay/akutan/util/cmp"
	"github.com/stretchr/testify/assert"
)

var kgObjectStringerTests = []struct {
	obj            KGObject
	expectedString string
	expectedKey    string
}{
	{KGObject{}, "(nil)", "nil"},
	{AString("Bob", 0), `"Bob"`, `string:"Bob"`},
	{AString("Bob", 42), `"Bob"@#42`, `string:"Bob"@#42`},
	{AInt64(42, 0), "42", "int64:42"},
	{AInt64(42, 42), "42^^#42", "int64:42^^#42"},
	{AFloat64(42.42, 0), "42.42", "float64:42.42"},
	{AFloat64(42.42, 1), "42.42^^#1", "float64:42.42^^#1"},
	{AFloat64(42.0, 0), "42.0", "float64:42.0"},
	{AFloat64(42.0, 1), "42.0^^#1", "float64:42.0^^#1"},
	{AFloat64(math.MaxFloat64, 0), "1.7976931348623157e+308", "float64:1.7976931348623157e+308"},
	{AFloat64(math.Inf(1), 0), "+Inf", "float64:+Inf"},
	{AFloat64(math.Inf(-1), 0), "-Inf", "float64:-Inf"},
	{AFloat64(math.NaN(), 0), "NaN", "float64:NaN"},
	{ABool(true, 0), "true", "bool:true"},
	{ABool(true, 1), "true^^#1", "bool:true^^#1"},
	// Note that "2018" alone is ambiguous: it could be an int or a timestamp.
	{ATimestampY(2018, 0), "2018", "timestamp:2018"},
	{ATimestampYM(2018, 8, 0), "2018-08", "timestamp:2018-08"},
	{ATimestampYM(2018, 8, 1), "2018-08^^#1", "timestamp:2018-08^^#1"},
	{AKID(42), "#42", "kid:#42"},
}

func Test_KGObject_String(t *testing.T) {
	for _, c := range kgObjectStringerTests {
		assert.Equal(t, c.expectedString, c.obj.String())
	}
}

func Test_KGObject_Key(t *testing.T) {
	for _, c := range kgObjectStringerTests {
		assert.Equal(t, c.expectedKey, cmp.GetKey(c.obj), "String: %v", c.obj.String())
	}
}
