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

package exec

import (
	"testing"

	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/rpc"
	"github.com/stretchr/testify/assert"
)

func Test_JoinFactSets(t *testing.T) {
	fs1 := FactSet{
		Facts: []rpc.Fact{{Index: 1}, {Index: 2}},
	}
	fs2 := FactSet{
		Facts: []rpc.Fact{{Index: 3}, {Index: 4}, {Index: 5}},
	}
	exp := FactSet{
		Facts: []rpc.Fact{{Index: 1}, {Index: 2}, {Index: 3}, {Index: 4}, {Index: 5}},
	}
	joined := joinFactSets(fs1, fs2)
	assert.Equal(t, exp, joined)

	// manipulating the source FactSets shouldn't affect the joined ones
	fs2.Facts[0].Index = 33
	assert.Equal(t, exp, joined)
}

func Test_FactSet_String(t *testing.T) {
	fs := FactSet{
		Facts: []rpc.Fact{{Index: 1, Id: 2, Subject: 3, Predicate: 4, Object: rpc.AKID(5)}},
	}
	assert.Equal(t, "{Facts:[{idx:1 id:2 s:3 p:4 o:#5}]}", fs.String())
}

func Test_Value_String(t *testing.T) {
	tests := map[Value]string{
		Value{KGObject: rpc.AString("Bob", 0)}:                   `"Bob"`,
		Value{KGObject: rpc.AString("Bob", 10), LangExtID: "en"}: `"Bob"@#10 en`,
		Value{KGObject: rpc.AInt64(42, 0)}:                       `42`,
		Value{KGObject: rpc.AInt64(42, 10), UnitExtID: "<KiB>"}:  `42^^#10 <KiB>`,
		Value{KGObject: rpc.AKID(42)}:                            `#42`,
		Value{KGObject: rpc.AKID(42), ExtID: "<bob>"}:            `<bob> #42`,
	}
	for val, expected := range tests {
		assert.Equal(t, expected, val.String())
	}
}

func Test_Value_SetExtID(t *testing.T) {
	v := Value{KGObject: rpc.AKID(42)}
	v.SetExtID("bob")
	assert.Equal(t, "<bob>", v.ExtID)

	v = Value{KGObject: rpc.AKID(42)}
	v.SetExtID("type:product")
	assert.Equal(t, "type:product", v.ExtID)

	v = Value{KGObject: rpc.AKID(42)}
	v.SetExtID("")
	assert.Equal(t, "", v.ExtID)
}

func Test_Value_SetLangExtID(t *testing.T) {
	v := Value{KGObject: rpc.AString("Bob", 10)}
	v.SetLangExtID("en")
	assert.Equal(t, "en", v.LangExtID)

	v = Value{KGObject: rpc.AString("Bob", 0)}
	v.SetLangExtID("")
	assert.Equal(t, "", v.LangExtID)
}

func Test_Value_SetUnitExtID(t *testing.T) {
	v := Value{KGObject: rpc.AInt64(42, 10)}
	v.SetUnitExtID("inches")
	assert.Equal(t, "<inches>", v.UnitExtID)

	v = Value{KGObject: rpc.AInt64(42, 10)}
	v.SetUnitExtID("type:inches")
	assert.Equal(t, "type:inches", v.UnitExtID)

	v = Value{KGObject: rpc.AInt64(42, 0)}
	v.SetUnitExtID("")
	assert.Equal(t, "", v.UnitExtID)
}

func Test_VariableSetFromTerms(t *testing.T) {
	v := &plandef.Variable{Name: "v"}
	bv := &plandef.Variable{Name: "bv"}
	b := &plandef.Binding{Var: bv}

	assert.Equal(t, variableSet{nil, bv, nil, nil},
		variableSetFromTerms([]plandef.Term{nil, b, nil, nil}))

	assert.Equal(t, variableSet{v, bv, nil, nil},
		variableSetFromTerms([]plandef.Term{v, b, nil, nil}))

	assert.Equal(t, variableSet{nil, v, bv, nil},
		variableSetFromTerms([]plandef.Term{nil, v, b, nil}))

	assert.Equal(t, variableSet{nil, bv, v, nil},
		variableSetFromTerms([]plandef.Term{nil, b, v, nil}))

	assert.Equal(t, variableSet{nil, bv, nil, v},
		variableSetFromTerms([]plandef.Term{nil, b, nil, v}))

	assert.Equal(t, variableSet{v, v, v, bv},
		variableSetFromTerms([]plandef.Term{v, v, v, b}))
}

func Test_VariableSet(t *testing.T) {
	vi := &plandef.Variable{Name: "id"}
	vs := &plandef.Variable{Name: "s"}
	vp := &plandef.Variable{Name: "p"}
	vo := &plandef.Variable{Name: "o"}
	type test struct {
		vs         variableSet
		expColumns Columns
	}
	for _, test := range []test{
		{variableSet{nil, nil, nil, nil}, Columns{}},
		{variableSet{vi, nil, nil, nil}, Columns{vi}},
		{variableSet{nil, vi, nil, nil}, Columns{vi}},
		{variableSet{nil, nil, vi, nil}, Columns{vi}},
		{variableSet{nil, nil, nil, vi}, Columns{vi}},
		{variableSet{vi, vs, nil, nil}, Columns{vi, vs}},
		{variableSet{vi, nil, vs, nil}, Columns{vi, vs}},
		{variableSet{nil, vs, nil, vp}, Columns{vs, vp}},
		{variableSet{nil, nil, vs, vp}, Columns{vs, vp}},
		{variableSet{vi, vs, vp, nil}, Columns{vi, vs, vp}},
		{variableSet{nil, vs, vp, vo}, Columns{vs, vp, vo}},
		{variableSet{vi, nil, vp, vo}, Columns{vi, vp, vo}},
		{variableSet{vi, vs, nil, vo}, Columns{vi, vs, vo}},
		{variableSet{vi, vs, vp, nil}, Columns{vi, vs, vp}},
		{variableSet{vi, vs, vp, vo}, Columns{vi, vs, vp, vo}},
	} {
		assert.Equal(t, test.expColumns, test.vs.columns())
		assert.Equal(t, len(test.expColumns), test.vs.count())
	}

	f := rpc.Fact{Index: 1, Id: 2, Subject: 3, Predicate: 4, Object: rpc.AKID(5)}
	vID := Value{KGObject: rpc.AKID(2)}
	vS := Value{KGObject: rpc.AKID(3)}
	vP := Value{KGObject: rpc.AKID(4)}
	vO := Value{KGObject: rpc.AKID(5)}

	resultOf := func(v1, v2, v3, v4 *plandef.Variable, expRes ...Value) {
		vs := variableSet{v1, v2, v3, v4}
		actual := vs.resultsOf(&f)
		assert.Equal(t, expRes, actual)
	}
	resultOf(nil, nil, nil, nil)
	resultOf(vi, nil, nil, nil, vID)
	resultOf(nil, vs, nil, vo, vS, vO)
	resultOf(nil, nil, vp, nil, vP)
	resultOf(nil, nil, vp, vo, vP, vO)
	resultOf(vi, vs, vp, vo, vID, vS, vP, vO)
	resultOf(vi, nil, nil, vo, vID, vO)
}
