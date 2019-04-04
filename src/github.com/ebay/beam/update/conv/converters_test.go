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

package conv

import (
	"fmt"
	"testing"
	"time"

	"github.com/ebay/beam/api"
	"github.com/ebay/beam/logentry"
	"github.com/ebay/beam/logentry/logread"
	"github.com/ebay/beam/query/parser"
	"github.com/ebay/beam/rpc"
	"github.com/stretchr/testify/assert"
)

// kidTest is used in the ParserToRPC and ParserToLog tests.
type kidTest struct {
	name  string
	in    parser.Term
	exp   uint64
	fail  bool
	panic bool
}

func kidTests() []kidTest {
	return []kidTest{
		{
			name: "LiteralID",
			in:   &parser.LiteralID{Value: 30},
			exp:  30,
		},
		{
			name: "QName found",
			in:   &parser.QName{Value: "rdf:type"},
			exp:  10002,
		},
		{
			name: "QName not found",
			in:   &parser.QName{Value: "rdfs:subClassOf"},
			fail: true,
		},
		{
			name: "Entity found",
			in:   &parser.Entity{Value: "product"},
			exp:  10003,
		},
		{
			name: "Entity not found",
			in:   &parser.Entity{Value: "earth"},
			fail: true,
		},
		{
			name: "Variable found",
			in:   &parser.Variable{Name: "var1"},
			exp:  10006,
		},
		{
			name: "Variable not found",
			in:   &parser.Variable{Name: "not_a_var"},
			fail: true,
		},
		{
			name:  "Operator",
			in:    &parser.Operator{Value: rpc.OpEqual},
			panic: true,
		},
	}
}

// kgObjectTest is used in the parserToRPCConverter and parserToLogConverter
// tests.
type kgObjectTest struct {
	name  string
	in    parser.Term
	exp   rpc.KGObject
	fail  bool
	panic bool
}

func kgObjectTests() []kgObjectTest {
	kidTests := kidTests()
	tests := make([]kgObjectTest, len(kidTests))
	for i, test := range kidTests {
		var exp rpc.KGObject
		if !test.fail && !test.panic {
			exp = rpc.AKID(test.exp)
		}
		tests[i] = kgObjectTest{
			name:  fmt.Sprintf("kidTest %v", test.name),
			in:    test.in,
			exp:   exp,
			fail:  test.fail,
			panic: test.panic,
		}
	}
	return append(tests, []kgObjectTest{
		{
			name: "bool without unit",
			in:   &parser.LiteralBool{Value: true},
			exp:  rpc.ABool(true, 0),
		},
		{
			name: "bool with unit",
			in:   &parser.LiteralBool{Value: true, Unit: parser.Unit{Value: "inch"}},
			exp:  rpc.ABool(true, 10004),
		},
		{
			name: "bool with missing unit",
			in:   &parser.LiteralBool{Value: true, Unit: parser.Unit{Value: "stone"}},
			fail: true,
		},
		{
			name: "float without unit",
			in:   &parser.LiteralFloat{Value: 5.0},
			exp:  rpc.AFloat64(5.0, 0),
		},
		{
			name: "float with unit",
			in:   &parser.LiteralFloat{Value: 5.0, Unit: parser.Unit{Value: "inch"}},
			exp:  rpc.AFloat64(5.0, 10004),
		},
		{
			name: "float with missing unit",
			in:   &parser.LiteralFloat{Value: 5.0, Unit: parser.Unit{Value: "stone"}},
			fail: true,
		},
		{
			name: "int without unit",
			in:   &parser.LiteralInt{Value: 5},
			exp:  rpc.AInt64(5, 0),
		},
		{
			name: "int with unit",
			in:   &parser.LiteralInt{Value: 5, Unit: parser.Unit{Value: "inch"}},
			exp:  rpc.AInt64(5, 10004),
		},
		{
			name: "int with missing unit",
			in:   &parser.LiteralInt{Value: 5, Unit: parser.Unit{Value: "stone"}},
			fail: true,
		},
		{
			name: "string without language",
			in:   &parser.LiteralString{Value: "hello"},
			exp:  rpc.AString("hello", 0),
		},
		{
			name: "string with language",
			in:   &parser.LiteralString{Value: "hello", Language: parser.Language{Value: "en"}},
			exp:  rpc.AString("hello", 10005),
		},
		{
			name: "string with missing language",
			in:   &parser.LiteralString{Value: "hello", Language: parser.Language{Value: "la"}},
			fail: true,
		},
		{
			name: "time without unit",
			in:   &parser.LiteralTime{Value: time.Unix(53, 2), Precision: api.Second},
			exp:  rpc.ATimestampYMDHMS(1970, 1, 1, 0, 0, 53, 0),
		},
		{
			name: "time with unit",
			in: &parser.LiteralTime{Value: time.Unix(53, 2), Precision: api.Second,
				Unit: parser.Unit{Value: "inch"}},
			exp: rpc.ATimestampYMDHMS(1970, 1, 1, 0, 0, 53, 10004),
		},
		{
			name: "time with missing unit",
			in: &parser.LiteralTime{Value: time.Unix(53, 2), Precision: api.Second,
				Unit: parser.Unit{Value: "stone"}},
			fail: true,
		},
	}...)
}

func Test_ParserToRPC(t *testing.T) {
	converter := &ParserToRPC{
		ExternalIDs: map[string]uint64{
			"rdf:type": 10002,
			"product":  10003,
			"inch":     10004,
			"en":       10005,
		},
		Variables: map[string]uint64{
			"var1": 10006,
		},
	}
	t.Run("KID", func(t *testing.T) {
		for _, test := range kidTests() {
			t.Run(test.name, func(t *testing.T) {
				t.Logf("ParserToRPC.KID(%#v)", test.in)
				if test.panic {
					assert.Panics(t, func() { converter.KID(test.in) })
					return
				}
				out, ok := converter.KID(test.in)
				assert.Equal(t, !test.fail, ok)
				assert.Equal(t, test.exp, out)
			})
		}
	})
	t.Run("KGObject", func(t *testing.T) {
		for _, test := range kgObjectTests() {
			t.Run(test.name, func(t *testing.T) {
				t.Logf("ParserToRPC.KGObject(%#v)", test.in)
				if test.panic {
					assert.Panics(t, func() { converter.KGObject(test.in) })
					return
				}
				out, ok := converter.KGObject(test.in)
				assert.Equal(t, !test.fail, ok)
				assert.Equal(t, test.exp, out)
				assert.True(t, test.exp.Equal(out), "KGObject.Equal")
			})
		}
	})
}

func Test_ParserToLog(t *testing.T) {
	converters := map[string]*ParserToLog{
		"KIDs": &ParserToLog{
			ExternalIDs: map[string]logentry.KIDOrOffset{
				"rdf:type": logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 10002}},
				"product":  logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 10003}},
				"inch":     logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 10004}},
				"en":       logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 10005}},
			},
			Variables: map[string]logentry.KIDOrOffset{
				"var1": logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 10006}},
			},
		},
		"offsets": &ParserToLog{
			ExternalIDs: map[string]logentry.KIDOrOffset{
				"rdf:type": logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Offset{Offset: 2}},
				"product":  logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Offset{Offset: 3}},
				"inch":     logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Offset{Offset: 4}},
				"en":       logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Offset{Offset: 5}},
			},
			Variables: map[string]logentry.KIDOrOffset{
				"var1": logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Offset{Offset: 6}},
			},
		},
	}
	for name, converter := range converters {
		t.Run(name+"/MustKIDOrOffset", func(t *testing.T) {
			for _, test := range kidTests() {
				t.Run(test.name, func(t *testing.T) {
					t.Logf("ParserToLog.MustKIDOrOffset(%#v) [%v]", test.in, name)
					if test.panic || test.fail {
						assert.Panics(t, func() { converter.MustKIDOrOffset(test.in) })
						return
					}
					out := converter.MustKIDOrOffset(test.in)
					assert.Equal(t, test.exp, logread.KIDof(10, &out))
				})
			}
		})
		t.Run(name+"/MustKGObject", func(t *testing.T) {
			for _, test := range kgObjectTests() {
				t.Run(test.name, func(t *testing.T) {
					t.Logf("ParserToLog.MustKGObject(%#v) [%v]", test.in, name)
					if test.panic || test.fail {
						assert.Panics(t, func() { converter.MustKGObject(test.in) })
						return
					}
					out := logread.ToRPCKGObject(10, converter.MustKGObject(test.in))
					assert.Equal(t, test.exp, out)
					assert.True(t, test.exp.Equal(out), "KGObject.Equal")
				})
			}
		})
	}
}
