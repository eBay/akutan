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

package api

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var ifb = &insertFactBuilder{}
var ts = time.Date(2018, time.Month(8), 8, 16, 44, 6, 999999999, time.UTC)

func Test_String(t *testing.T) {
	type stringTest struct {
		obj fmt.Stringer
		exp string
	}
	tests := []stringTest{
		{
			obj: KGValue{Value: &KGValue_Bool{Bool: &KGBool{Value: true}}},
			exp: "true",
		}, {
			obj: KGValue{Value: &KGValue_Bool{Bool: &KGBool{Value: false, Unit: &KGID{QName: "<metric>"}}}},
			exp: "false^^<metric>",
		}, {
			obj: KGValue{Value: &KGValue_Int64{Int64: &KGInt64{Value: 42}}},
			exp: "42",
		}, {
			obj: KGValue{Value: &KGValue_Int64{Int64: &KGInt64{Value: 42, Unit: &KGID{QName: "<feet>"}}}},
			exp: "42^^<feet>",
		}, {
			obj: KGValue{Value: &KGValue_Float64{Float64: &KGFloat64{Value: 42}}},
			exp: "42.000000",
		}, {
			obj: KGValue{Value: &KGValue_Float64{Float64: &KGFloat64{Value: 42, Unit: &KGID{QName: "<feet>"}}}},
			exp: "42.000000^^<feet>",
		}, {
			obj: KGValue{Value: &KGValue_Str{Str: &KGString{Value: "Alice"}}},
			exp: `"Alice"`,
		}, {
			obj: KGValue{Value: &KGValue_Str{Str: &KGString{Value: "Alice", Lang: &KGID{QName: "en"}}}},
			exp: `"Alice"@en`,
		}, {
			obj: KGValue{Value: &KGValue_Timestamp{Timestamp: &KGTimestamp{Value: ts, Precision: Day}}},
			exp: `2018-08-08`,
		}, {
			obj: KGValue{Value: &KGValue_Node{Node: &KGID{QName: "foaf:knows", BeamId: 123}}},
			exp: `foaf:knows`,
		}, {
			obj: KGValue{Value: &KGValue_Node{Node: &KGID{QName: "<product>", BeamId: 123}}},
			exp: `<product>`,
		}, {
			obj: KGValue{Value: nil},
			exp: `(nil)`,
		}, {
			obj: &QueryFactsRequest{Index: 12345, Query: "?s ?p ?o\n#12345 <entity> rdfs:type \"literal\""},
			exp: `QueryFactsRequest{
 Index: 12345
 Query: '?s ?p ?o; #12345 <entity> rdfs:type "literal"'
}`,
		}, {
			obj: &InsertFactsResult{
				Index:      12345,
				VarResults: []uint64{678, 901, 234, 567},
				FactIds:    []uint64{890, 123, 456, 789, 123, 456},
			},
			exp: `InsertFactsResult{
 Index: 12345
 VarResults: 678, 901, 234, 567
 FactIDs:
  890
  123
  456
  789
  123
  456
}`,
		}, {
			obj: &InsertFactsRequest{
				NewSubjectVars: []string{"a", "b", "c"},
				Facts: []InsertFact{
					ifb.captureFactID("id1").sVar("s").pVar("p").oVar("o").build(),
					ifb.sID(123).pID(456).oBool(true).build(),
					ifb.sID(123).pID(456).oBool(true).oUID(12).build(),
					ifb.sID(123).pID(456).oBool(false).build(),
					ifb.sID(123).pID(456).oBool(false).oUID(12).build(),
					ifb.sID(123).pID(456).oFloat64(1).build(),
					ifb.sID(123).pID(456).oFloat64(1).oUID(12).build(),
					ifb.sID(123).pID(456).oInt64(1).build(),
					ifb.sID(123).pID(456).oInt64(1).oUID(12).build(),
					ifb.sID(123).pID(456).oID(789).build(),
					ifb.sID(123).pID(456).oString("Literal String").build(),
					ifb.sID(123).pID(456).oString("Literal String").oLID(34).build(),
					ifb.sID(123).pID(456).oTime(ts, Nanosecond).build(),
					ifb.sID(123).pID(456).oTime(ts, Nanosecond).oUID(12).build(),
				},
			},
			exp: `InsertFactsRequest{
 NewSubjectVars: a, b, c
 Facts:
  ?id1: ?s ?p ?o
  #123 #456 true
  #123 #456 true
  #123 #456 false
  #123 #456 false
  #123 #456 1.000000
  #123 #456 1.000000
  #123 #456 1
  #123 #456 1
  #123 #456 #789
  #123 #456 'Literal String'
  #123 #456 'Literal String'
  #123 #456 2018-08-08T16:44:06.999999999
  #123 #456 2018-08-08T16:44:06.999999999
}`,
		}, {
			obj: Year,
			exp: "y",
		}, {
			obj: Month,
			exp: "y-m",
		}, {
			obj: Day,
			exp: "y-m-d",
		}, {
			obj: Hour,
			exp: "y-m-d h",
		}, {
			obj: Minute,
			exp: "y-m-d h:m",
		}, {
			obj: Second,
			exp: "y-m-d h:m:s",
		}, {
			obj: Nanosecond,
			exp: "y-m-d h:m:s.n",
		}, {
			obj: ResolvedFact{Index: 12345, Id: 6, Subject: 7, Predicate: 8, Object: KGObject{UnitID: 9, Value: &KGObject_ABool{ABool: true}}},
			exp: `{12345 6 7 8 true}`,
		}, {
			obj: ResolvedFact{Index: 12345, Id: 6, Subject: 7, Predicate: 8, Object: KGObject{UnitID: 9, Value: &KGObject_AFloat64{AFloat64: 1.0}}},
			exp: `{12345 6 7 8 1.000000}`,
		}, {
			obj: ResolvedFact{Index: 12345, Id: 6, Subject: 7, Predicate: 8, Object: KGObject{UnitID: 9, Value: &KGObject_AInt64{AInt64: 1}}},
			exp: `{12345 6 7 8 1}`,
		}, {
			obj: ResolvedFact{Index: 12345, Id: 6, Subject: 7, Predicate: 8, Object: KGObject{Value: &KGObject_AKID{AKID: 1}}},
			exp: `{12345 6 7 8 #1}`,
		}, {
			obj: ResolvedFact{Index: 12345, Id: 6, Subject: 7, Predicate: 8, Object: KGObject{LangID: 9, Value: &KGObject_AString{AString: "Literal String"}}},
			exp: `{12345 6 7 8 'Literal String'}`,
		}, {
			obj: ResolvedFact{Index: 12345, Id: 6, Subject: 7, Predicate: 8, Object: KGObject{UnitID: 9, Value: &KGObject_ATimestamp{ATimestamp: &KGTimestamp{Precision: Nanosecond, Value: ts}}}},
			exp: `{12345 6 7 8 2018-08-08T16:44:06.999999999}`,
		}}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("%T_%d", tc.obj, i), func(t *testing.T) {
			s := tc.obj.String()
			assert.Equal(t, tc.exp, s)
		})
	}
}

type insertFactBuilder struct {
	fact InsertFact
}

func (b *insertFactBuilder) captureFactID(name string) *insertFactBuilder {
	b.fact.FactIDVar = name
	return b
}

func (b *insertFactBuilder) sVar(name string) *insertFactBuilder {
	b.fact.Subject.Value = &KIDOrVar_Var{Var: name}
	return b
}

func (b *insertFactBuilder) sID(kid uint64) *insertFactBuilder {
	b.fact.Subject.Value = &KIDOrVar_Kid{Kid: kid}
	return b
}

func (b *insertFactBuilder) pVar(name string) *insertFactBuilder {
	b.fact.Predicate.Value = &KIDOrVar_Var{Var: name}
	return b
}

func (b *insertFactBuilder) pID(kid uint64) *insertFactBuilder {
	b.fact.Predicate.Value = &KIDOrVar_Kid{Kid: kid}
	return b
}

func (b *insertFactBuilder) oVar(name string) *insertFactBuilder {
	b.fact.Object.Value = &KGObjectOrVar_Var{Var: name}
	return b
}

func (b *insertFactBuilder) oID(kid uint64) *insertFactBuilder {
	b.fact.Object.Value = &KGObjectOrVar_Object{
		Object: &KGObject{
			Value: &KGObject_AKID{AKID: kid},
		},
	}
	return b
}

func (b *insertFactBuilder) oString(value string) *insertFactBuilder {
	b.fact.Object.Value = &KGObjectOrVar_Object{
		Object: &KGObject{
			Value: &KGObject_AString{AString: value},
		},
	}
	return b
}

func (b *insertFactBuilder) oFloat64(value float64) *insertFactBuilder {
	b.fact.Object.Value = &KGObjectOrVar_Object{
		Object: &KGObject{
			Value: &KGObject_AFloat64{AFloat64: value},
		},
	}
	return b
}

func (b *insertFactBuilder) oInt64(value int64) *insertFactBuilder {
	b.fact.Object.Value = &KGObjectOrVar_Object{
		Object: &KGObject{
			Value: &KGObject_AInt64{AInt64: value},
		},
	}
	return b
}

func (b *insertFactBuilder) oTime(value time.Time, precision Precision) *insertFactBuilder {
	b.fact.Object.Value = &KGObjectOrVar_Object{
		Object: &KGObject{
			Value: &KGObject_ATimestamp{ATimestamp: &KGTimestamp{Precision: precision, Value: value}},
		},
	}
	return b
}

func (b *insertFactBuilder) oBool(value bool) *insertFactBuilder {
	b.fact.Object.Value = &KGObjectOrVar_Object{
		Object: &KGObject{
			Value: &KGObject_ABool{ABool: value},
		},
	}
	return b
}

func (b *insertFactBuilder) oUID(value uint64) *insertFactBuilder {
	v, ok := b.fact.Object.Value.(*KGObjectOrVar_Object)
	if !ok {
		panic(fmt.Sprintf("OVar does not include unit id"))
	}
	v.Object.UnitID = value
	return b
}

func (b *insertFactBuilder) oLID(value uint64) *insertFactBuilder {
	v, ok := b.fact.Object.Value.(*KGObjectOrVar_Object)
	if !ok {
		panic(fmt.Sprintf("OVar does not include lang id"))
	}
	v.Object.LangID = value
	return b
}

func (b *insertFactBuilder) build() InsertFact {
	if b.fact.Subject.Value == nil {
		panic("Attempted to build fact with nil subject")
	}
	if b.fact.Predicate.Value == nil {
		panic("Attempted to build fact with nil predicate")
	}
	if b.fact.Object.Value == nil {
		panic("Attempted to build fact with nil object")
	}
	f := b.fact
	b.fact = InsertFact{}
	return f
}
