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

package impl

import (
	"testing"
	"time"

	"github.com/ebay/beam/api"
	"github.com/ebay/beam/logentry"
	"github.com/ebay/beam/logentry/logwrite"
	"github.com/ebay/beam/msg/kgobject"
	"github.com/stretchr/testify/assert"
)

func Test_TimestampPrecisionSame(t *testing.T) {
	assert.Equal(t, logentry.TimestampPrecision_value, api.Precision_value, "API & Logentry defintions are expected to have the same names/values for Precision")
}

func Test_IdLimit(t *testing.T) {
	i := api.InsertFactsRequest{
		NewSubjectVars: make([]string, 1000),
	}
	assert.EqualError(t, validateInsertFactsRequest(&i), "a single insert is limited to 999 new Ids, this request has 1000 new Subjects & 0 new Facts which is too many")
	i = api.InsertFactsRequest{
		NewSubjectVars: make([]string, 500),
		Facts:          make([]api.InsertFact, 500),
	}
	assert.EqualError(t, validateInsertFactsRequest(&i), "a single insert is limited to 999 new Ids, this request has 500 new Subjects & 500 new Facts which is too many")
}

func Test_ObjectRequire(t *testing.T) {
	i := api.InsertFactsRequest{
		Facts: []api.InsertFact{
			{},
		},
	}
	assert.EqualError(t, validateInsertFactsRequest(&i), "fact[0] Object doesn't specify a variable or an Object value, one or the other is required")
	i.Facts[0].Object.Value = &api.KGObjectOrVar_Var{Var: "?bob"}
	i.NewSubjectVars = []string{"?bob"}
	assert.NoError(t, validateInsertFactsRequest(&i))
}

func Test_NewSubjectVarUsed(t *testing.T) {
	i := api.InsertFactsRequest{
		NewSubjectVars: []string{"bob"},
		Facts: []api.InsertFact{
			{
				Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Var{Var: "bob"}},
				Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 3}},
				Object: api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{
					Object: &api.KGObject{Value: &api.KGObject_AKID{AKID: 1}}}},
			},
		},
	}
	assert.NoError(t, validateInsertFactsRequest(&i))

	i.NewSubjectVars[0] = "alice"
	assert.EqualError(t, validateInsertFactsRequest(&i), "fact[0] specifies a subject variable 'bob', but that isn't delcared")

	i.Facts[0].Subject = api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 1}}
	assert.EqualError(t, validateInsertFactsRequest(&i), "the following variables were declared but not used, all variables must get used: [alice]")
}

func Test_FactVarsUnique(t *testing.T) {
	i := api.InsertFactsRequest{
		Facts: []api.InsertFact{
			{
				FactIDVar: "?fact",
				Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 3}},
				Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 3}},
				Object: api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{
					Object: &api.KGObject{Value: &api.KGObject_AKID{AKID: 1}}}},
			},
			{
				FactIDVar: "?fact",
				Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Var{Var: "?fact"}},
				Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 3}},
				Object: api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{
					Object: &api.KGObject{Value: &api.KGObject_AKID{AKID: 1}}}},
			},
		}}
	assert.EqualError(t, validateInsertFactsRequest(&i), "fact[1] specifies a FactID Variable '?fact', but that was already declared as variable")
	i.Facts[1].FactIDVar = ""
	assert.NoError(t, validateInsertFactsRequest(&i))

	i.NewSubjectVars = []string{"?fact"}
	assert.EqualError(t, validateInsertFactsRequest(&i), "fact[0] specifies a FactID Variable '?fact', but that was already declared as variable")
}

func Test_VarUsed(t *testing.T) {
	i := api.InsertFactsRequest{
		Facts: []api.InsertFact{
			{
				FactIDVar: "?fact",
				Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 3}},
				Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 3}},
				Object: api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{
					Object: &api.KGObject{Value: &api.KGObject_AKID{AKID: 1}}}},
			},
		}}
	assert.EqualError(t, validateInsertFactsRequest(&i), "the following variables were declared but not used, all variables must get used: [?fact]")
}

func Test_FactIDVarCantBeNewSubjectVar(t *testing.T) {
	i := api.InsertFactsRequest{
		NewSubjectVars: []string{"bob"},
		Facts: []api.InsertFact{
			{
				FactIDVar: "bob",
				Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 1}},
				Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 2}},
				Object: api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{
					Object: &api.KGObject{Value: &api.KGObject_AKID{AKID: 3}}}},
			},
		},
	}
	assert.EqualError(t, validateInsertFactsRequest(&i), "fact[0] specifies a FactID Variable 'bob', but that was already declared as variable")
}

func Test_PredicateObjectVarNotFact(t *testing.T) {
	i := api.InsertFactsRequest{
		Facts: []api.InsertFact{
			{
				FactIDVar: "?fact",
				Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 3}},
				Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 3}},
				Object: api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{
					Object: &api.KGObject{Value: &api.KGObject_AKID{AKID: 1}}}},
			},
			{
				Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 3}},
				Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Var{Var: "?fact"}},
				Object: api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{
					Object: &api.KGObject{Value: &api.KGObject_AKID{AKID: 1}}}},
			},
		}}
	assert.EqualError(t, validateInsertFactsRequest(&i), "fact[1] specifies a predicate variable '?fact', but that variable is a fact_id and can't be bound to predicate")

	i = api.InsertFactsRequest{
		Facts: []api.InsertFact{
			{
				FactIDVar: "?fact",
				Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 3}},
				Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 3}},
				Object: api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{
					Object: &api.KGObject{Value: &api.KGObject_AKID{AKID: 1}}}},
			},
			{
				Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 3}},
				Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 3}},
				Object:    api.KGObjectOrVar{Value: &api.KGObjectOrVar_Var{Var: "?fact"}},
			},
		}}
	assert.EqualError(t, validateInsertFactsRequest(&i), "fact[1] specifies a object variable '?fact', but that variable is a fact_id and can't be bound to object")
}

func Test_ConvertAPIInsertToLogCommandNoVars(t *testing.T) {
	i := api.InsertFactsRequest{
		Facts: []api.InsertFact{
			{
				Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 1}},
				Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 2}},
				Object: api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{
					Object: &api.KGObject{Value: &api.KGObject_AKID{AKID: 3}}}},
			},
			{
				Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 4}},
				Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 5}},
				Object: api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{
					Object: &api.KGObject{Value: &api.KGObject_AKID{AKID: 4}}}},
			},
			{
				Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 5}},
				Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 6}},
				Object: api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{
					Object: &api.KGObject{
						LangID: 5,
						Value:  &api.KGObject_AString{AString: "Bob"}}}},
			},
			{
				Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 6}},
				Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 7}},
				Object: api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{
					Object: &api.KGObject{
						UnitID: 8,
						Value:  &api.KGObject_AInt64{AInt64: 42}}}},
			},
		},
	}
	logEntry, vars := convertAPIInsertToLogCommand(&i)
	lfb := new(logwrite.InsertFactBuilder)
	exp := logentry.InsertTxCommand{
		Facts: []logentry.InsertFact{
			lfb.SID(1).PID(2).OKID(3).Fact(),
			lfb.SID(4).PID(5).OKID(4).Fact(),
			lfb.SID(5).PID(6).OString("Bob", 5).Fact(),
			lfb.SID(6).PID(7).OInt64(42, 8).Fact(),
		}}
	assert.Equal(t, exp, logEntry)
	assert.Empty(t, vars)
}

func Test_ConvertAPIInsertToLogCommandWithVars(t *testing.T) {
	i := api.InsertFactsRequest{
		NewSubjectVars: []string{"?bob"},
		Facts: []api.InsertFact{
			{
				FactIDVar: "?f",
				Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Var{Var: "?bob"}},
				Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 2}},
				Object: api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{
					Object: &api.KGObject{Value: &api.KGObject_AKID{AKID: 3}}}},
			},
			{
				Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Var{Var: "?f"}},
				Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 2}},
				Object: api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{
					Object: &api.KGObject{Value: &api.KGObject_AKID{AKID: 4}}}},
			},
			{
				Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 5}},
				Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 2}},
				Object:    api.KGObjectOrVar{Value: &api.KGObjectOrVar_Var{Var: "?bob"}},
			},
		},
	}
	logEntry, vars := convertAPIInsertToLogCommand(&i)
	lfb := new(logwrite.InsertFactBuilder)
	exp := logentry.InsertTxCommand{
		Facts: []logentry.InsertFact{
			lfb.FactID(2).SOffset(1).PID(2).OKID(3).Fact(),
			lfb.FactID(3).SOffset(2).PID(2).OKID(4).Fact(),
			lfb.FactID(4).SID(5).PID(2).OOffset(1).Fact(),
		}}
	assert.Equal(t, exp, logEntry)
	assert.Equal(t, map[string]int32{"?bob": 1, "?f": 2}, vars)
}

func Test_TooManyOffsets(t *testing.T) {
	insert := api.InsertFactsRequest{
		Facts: make([]api.InsertFact, 1000),
	}
	for i := range insert.Facts {
		insert.Facts[i] = api.InsertFact{
			Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 1}},
			Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 2}},
			Object: api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{
				Object: &api.KGObject{Value: &api.KGObject_AKID{AKID: 3}}}},
		}
	}
	assert.Panics(t, func() { convertAPIInsertToLogCommand(&insert) })
}

func Test_ConvertKGObject(t *testing.T) {
	type tc struct {
		in  api.KGObjectOrVar
		exp logentry.KGObject
	}
	obj := func(obj api.KGObject) api.KGObjectOrVar {
		return api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{Object: &obj}}
	}
	vr := func(name string) api.KGObjectOrVar {
		return api.KGObjectOrVar{Value: &api.KGObjectOrVar_Var{Var: name}}
	}
	vars := map[string]int32{
		"bob": 3,
	}
	tests := []tc{
		{vr("bob"), logwrite.AKIDOffset(3)},
		{obj(kgobject.AKID(44)), logwrite.AKID(44)},
		{obj(kgobject.AString("Bob", 0)), logwrite.AString("Bob", 0)},
		{obj(kgobject.AString("Bob", 5)), logwrite.AString("Bob", 5)},
		{obj(kgobject.AInt64(42, 0)), logwrite.AInt64(42, 0)},
		{obj(kgobject.AInt64(42, 3)), logwrite.AInt64(42, 3)},
		{obj(kgobject.AFloat64(42.2, 0)), logwrite.AFloat64(42.2, 0)},
		{obj(kgobject.AFloat64(42.2, 4)), logwrite.AFloat64(42.2, 4)},
		{obj(kgobject.ABool(true, 0)), logwrite.ABool(true, 0)},
		{obj(kgobject.ABool(true, 3)), logwrite.ABool(true, 3)},
		{obj(kgobject.ATimestampYM(2018, 6, 44)), logwrite.ATimestamp(time.Date(2018, time.Month(6), 1, 0, 0, 0, 0, time.UTC), logentry.Month, 44)},
		{obj(kgobject.ATimestampYM(2018, 6, 0)), logwrite.ATimestamp(time.Date(2018, time.Month(6), 1, 0, 0, 0, 0, time.UTC), logentry.Month, 0)},
	}
	for _, test := range tests {
		t.Run(test.in.String(), func(t *testing.T) {
			act := convertKGObjectOrVar(&test.in, vars)
			assert.Equal(t, test.exp, act)
		})
	}
}
