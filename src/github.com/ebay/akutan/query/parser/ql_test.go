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
	"math"
	"time"

	"github.com/ebay/akutan/api"
	"github.com/ebay/akutan/rpc"
)

var (
	en              = Language{Value: "en"}
	jpn             = Language{Value: "jpn"}
	xsdDecimal      = Unit{Value: "xsd:decimal"}
	xsdDate         = Unit{Value: "xsd:date"}
	m               = Unit{Value: "m"}
	literalTime0    = &LiteralTime{NoUnit, time.Date(0, time.Month(1), 1, 0, 0, 0, 0, zero.Location()), api.Year}
	literalTime2017 = &LiteralTime{NoUnit, time.Date(2017, time.Month(1), 1, 0, 0, 0, 0, zero.Location()), api.Year}
	literalTime2018 = &LiteralTime{NoUnit, time.Date(2018, time.Month(1), 1, 0, 0, 0, 0, zero.Location()), api.Year}
)

type testLiteralSet struct {
	in     string
	parsed Literal
}

type testTermSet struct {
	in     string
	parsed Term
}

func literals() []testLiteralSet {
	ls := []testLiteralSet{}
	ls = append(ls, testLiteralSet{"true", &LiteralBool{Unit: NoUnit, Value: true}})
	ls = append(ls, testLiteralSet{"true", &LiteralBool{Unit: NoUnit, Value: true}})
	ls = append(ls, testLiteralSet{"#1", &LiteralID{Value: 1}})
	ls = append(ls, testLiteralSet{"1", &LiteralInt{Unit: NoUnit, Value: 1}})
	ls = append(ls, testLiteralSet{"1^^xsd:decimal", &LiteralInt{Unit: xsdDecimal, Value: 1}})
	ls = append(ls, testLiteralSet{"9223372036854775807", &LiteralInt{Unit: NoUnit, Value: math.MaxInt64}})
	ls = append(ls, testLiteralSet{"-9223372036854775808", &LiteralInt{Unit: NoUnit, Value: math.MinInt64}})
	ls = append(ls, testLiteralSet{"-1.0", &LiteralFloat{Unit: NoUnit, Value: -1.0}})
	ls = append(ls, testLiteralSet{"0.0", &LiteralFloat{Unit: NoUnit, Value: 0.0}})
	ls = append(ls, testLiteralSet{"1.0^^<m>", &LiteralFloat{Unit: m, Value: 1.0}})
	ls = append(ls, testLiteralSet{"1.7976931348623157e+308", &LiteralFloat{Unit: NoUnit, Value: math.MaxFloat64}})
	ls = append(ls, testLiteralSet{`""`, &LiteralString{Language: NoLanguage, Value: ""}})
	ls = append(ls, testLiteralSet{`"aaa"@en`, &LiteralString{Language: en, Value: "aaa"}})
	ls = append(ls, testLiteralSet{"'2018'", &LiteralTime{Unit: NoUnit, Value: time.Date(2018, time.Month(1), 1, 0, 0, 0, 0, zero.Location()), Precision: api.Year}})
	ls = append(ls, testLiteralSet{"'2018-08'", &LiteralTime{Unit: NoUnit, Value: time.Date(2018, time.Month(8), 1, 0, 0, 0, 0, zero.Location()), Precision: api.Month}})
	ls = append(ls, testLiteralSet{"'2018-08-02'", &LiteralTime{Unit: NoUnit, Value: time.Date(2018, time.Month(8), 2, 0, 0, 0, 0, zero.Location()), Precision: api.Day}})
	ls = append(ls, testLiteralSet{"'2018-08-02 17'", &LiteralTime{Unit: NoUnit, Value: time.Date(2018, time.Month(8), 2, 17, 0, 0, 0, zero.Location()), Precision: api.Hour}})
	ls = append(ls, testLiteralSet{"'2018-08-02 17:21'", &LiteralTime{Unit: NoUnit, Value: time.Date(2018, time.Month(8), 2, 17, 21, 0, 0, zero.Location()), Precision: api.Minute}})
	ls = append(ls, testLiteralSet{"'2018-08-02 17:21:26'", &LiteralTime{Unit: NoUnit, Value: time.Date(2018, time.Month(8), 2, 17, 21, 26, 0, zero.Location()), Precision: api.Second}})
	ls = append(ls, testLiteralSet{"'2018-08-02 17:21:26.12345'", &LiteralTime{Unit: NoUnit, Value: time.Date(2018, time.Month(8), 2, 17, 21, 26, 12345, zero.Location()), Precision: api.Nanosecond}})
	ls = append(ls, testLiteralSet{"'2018-08-02 17:21:26.12345'^^xsd:date", &LiteralTime{Unit: xsdDate, Value: time.Date(2018, time.Month(8), 2, 17, 21, 26, 12345, zero.Location()), Precision: api.Nanosecond}})
	return ls
}

func terms() []testTermSet {
	ts := []testTermSet{}
	for _, tls := range literals() {
		ts = append(ts, testTermSet{tls.in, tls.parsed})
	}
	ts = append(ts, testTermSet{"?v", &Variable{Name: "v"}})
	ts = append(ts, testTermSet{"<gt>", &Operator{Value: rpc.OpGreater}})
	ts = append(ts, testTermSet{"<Samsung>", &Entity{Value: "Samsung"}})
	ts = append(ts, testTermSet{"<%26>", &Entity{Value: "%26"}})
	ts = append(ts, testTermSet{"<AT%26T>", &Entity{Value: "AT%26T"}})
	ts = append(ts, testTermSet{"<StorageCapacity_1.0-10GB>", &Entity{Value: "StorageCapacity_1.0-10GB"}})
	ts = append(ts, testTermSet{"<%_-.,:>", &Entity{Value: "%_-.,:"}})
	ts = append(ts, testTermSet{"rdfs:label", &QName{Value: "rdfs:label"}})
	ts = append(ts, testTermSet{"foaf:Knows", &QName{Value: "foaf:Knows"}})
	ts = append(ts, testTermSet{"foo:Complex_example(-%26)", &QName{Value: "foo:Complex_example(-%26)"}})
	// Literal sets with non-distinct values.
	ts = append(ts, testTermSet{"{<Samsung>,<Apple>,<Samsung>}",
		&LiteralSet{[]Term{
			&Entity{Value: "Samsung"},
			&Entity{Value: "Apple"}}}})
	ts = append(ts, testTermSet{"{rdfs:label,rdfs:type,rdfs:label}",
		&LiteralSet{[]Term{
			&QName{Value: "rdfs:label"},
			&QName{Value: "rdfs:type"}}}})
	ts = append(ts, testTermSet{"{true,false,true}",
		&LiteralSet{[]Term{
			&LiteralBool{NoUnit, true},
			&LiteralBool{NoUnit, false}}}})
	ts = append(ts, testTermSet{"{0.5,1.0,0.5}",
		&LiteralSet{[]Term{
			&LiteralFloat{NoUnit, 0.5},
			&LiteralFloat{NoUnit, 1.0}}}})
	ts = append(ts, testTermSet{"{0.5^^<m>,1.0,0.5^^<m>}",
		&LiteralSet{[]Term{
			&LiteralFloat{m, 0.5},
			&LiteralFloat{NoUnit, 1.0}}}})
	ts = append(ts, testTermSet{"{#1,#2,#1}",
		&LiteralSet{[]Term{
			&LiteralID{Value: 1},
			&LiteralID{Value: 2}}}})
	ts = append(ts, testTermSet{"{0,1,0}",
		&LiteralSet{[]Term{
			&LiteralInt{NoUnit, 0},
			&LiteralInt{NoUnit, 1}}}})
	ts = append(ts, testTermSet{"{0^^<m>,1,0^^<m>}",
		&LiteralSet{[]Term{
			&LiteralInt{m, 0},
			&LiteralInt{NoUnit, 1}}}})
	ts = append(ts, testTermSet{`{"one","two","one"}`,
		&LiteralSet{[]Term{
			&LiteralString{NoLanguage, "one"},
			&LiteralString{NoLanguage, "two"}}}})
	ts = append(ts, testTermSet{`{"one"@en,"two","one"@en}`,
		&LiteralSet{[]Term{
			&LiteralString{en, "one"},
			&LiteralString{NoLanguage, "two"}}}})
	ts = append(ts, testTermSet{"{'2018','2017','2018'}",
		&LiteralSet{[]Term{
			literalTime2018,
			literalTime2017}}})
	ts = append(ts, testTermSet{"{'2018'^^<xsd:date>,'2017','2018'^^<xsd:date>}",
		&LiteralSet{[]Term{
			&LiteralTime{xsdDate, literalTime2018.Value, literalTime2018.Precision},
			literalTime2017}}})
	// Literals sets that are distinct.
	ts = append(ts, testTermSet{"{rdfs:label,<rdfs:label>}",
		&LiteralSet{[]Term{
			&QName{Value: "rdfs:label"},
			&Entity{Value: "rdfs:label"}}}})
	ts = append(ts, testTermSet{"{0.5^^<m>,0.5}",
		&LiteralSet{[]Term{
			&LiteralFloat{m, 0.5},
			&LiteralFloat{NoUnit, 0.5}}}})
	ts = append(ts, testTermSet{"{1^^<m>,1}",
		&LiteralSet{[]Term{
			&LiteralInt{m, 1},
			&LiteralInt{NoUnit, 1}}}})
	ts = append(ts, testTermSet{`{"one"@jpn,"one"}`,
		&LiteralSet{[]Term{
			&LiteralString{jpn, "one"},
			&LiteralString{NoLanguage, "one"}}}})
	ts = append(ts, testTermSet{"{'2018'^^<xsd:date>,'2018'}",
		&LiteralSet{[]Term{
			&LiteralTime{xsdDate, literalTime2018.Value, literalTime2018.Precision},
			literalTime2018}}})
	ts = append(ts, testTermSet{`{0.0,#0,0,"0",'0'}`,
		&LiteralSet{[]Term{
			&LiteralFloat{NoUnit, 0.0},
			&LiteralID{Value: 0},
			&LiteralInt{NoUnit, 0},
			&LiteralString{NoLanguage, "0"},
			literalTime0}}})
	return ts
}
