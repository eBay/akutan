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

package plandef

import (
	"testing"

	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/cmp"
	"github.com/stretchr/testify/assert"
)

var lookupTests = []struct {
	op        Operator
	expString string
	expKey    string
}{
	{
		op: &InferPO{
			ID:        &Variable{Name: "id"},
			Subject:   &Variable{Name: "subject"},
			Predicate: &OID{Value: 54},
			Object:    &Literal{Value: rpc.AString("object", 0)},
		},
		expString: `InferPO(?id ?subject #54 "object")`,
		expKey:    `InferPO ?id ?subject #54 string:"object"`,
	},
	{
		op: &InferSP{
			ID:        &Variable{Name: "id"},
			Subject:   &OID{Value: 12},
			Predicate: &OID{Value: 54},
			Object:    &Variable{Name: "object"},
		},
		expString: "InferSP(?id #12 #54 ?object)",
		expKey:    "InferSP ?id #12 #54 ?object",
	},
	{
		op: &InferSPO{
			ID:        &Variable{Name: "id"},
			Subject:   &OID{Value: 12},
			Predicate: &OID{Value: 54},
			Object:    &OID{Value: 76},
		},
		expString: "InferSPO(?id #12 #54 #76)",
		expKey:    "InferSPO ?id #12 #54 #76",
	},
	{
		op: &LookupPO{
			ID:        &Variable{Name: "id"},
			Subject:   &Variable{Name: "subject"},
			Predicate: &OID{Value: 54},
			Object:    &Literal{Value: rpc.AString("object", 10)},
		},
		expString: `LookupPO(?id ?subject #54 "object"@#10)`,
		expKey:    `LookupPO ?id ?subject #54 string:"object"@#10`,
	},
	{
		op: &LookupPOCmp{
			ID:        &Variable{Name: "id"},
			Subject:   &Variable{Name: "subject"},
			Predicate: &OID{Value: 54},
			Object:    &Variable{Name: "object"},
			Cmp: SelectClause{
				Comparison: rpc.OpGreater,
				Literal1:   &Literal{Value: rpc.AString("object", 0)},
			},
		},
		expString: `LookupPOCmp(?id ?subject #54 ?object > "object")`,
		expKey:    `LookupPOCmp ?id ?subject #54 ?object > string:"object"`,
	},
	{
		op: &LookupPOCmp{
			ID:        &Variable{Name: "id"},
			Subject:   &Variable{Name: "subject"},
			Predicate: &OID{Value: 54},
			Object:    &Variable{Name: "object"},
			Cmp: SelectClause{
				Comparison: rpc.OpRangeIncExc,
				Literal1:   &Literal{Value: rpc.AString("start", 0)},
				Literal2:   &Literal{Value: rpc.AString("end", 0)},
			},
		},
		expString: `LookupPOCmp(?id ?subject #54 ?object in ["start", "end"))`,
		expKey:    `LookupPOCmp ?id ?subject #54 ?object rangeIncExc string:"start" string:"end"`,
	},
	{
		op: &LookupS{
			ID:        &Variable{Name: "id"},
			Subject:   &OID{Value: 12},
			Predicate: &Variable{Name: "predicate"},
			Object:    &Variable{Name: "object"},
		},
		expString: "LookupS(?id #12 ?predicate ?object)",
		expKey:    "LookupS ?id #12 ?predicate ?object",
	},
	{
		op: &LookupSP{
			ID:        &Variable{Name: "id"},
			Subject:   &OID{Value: 12},
			Predicate: &OID{Value: 54},
			Object:    &Variable{Name: "object"},
		},
		expString: "LookupSP(?id #12 #54 ?object)",
		expKey:    "LookupSP ?id #12 #54 ?object",
	},
	{
		op: &LookupSPO{
			ID:        &Variable{Name: "id"},
			Subject:   &OID{Value: 12},
			Predicate: &OID{Value: 54},
			Object:    &OID{Value: 76},
		},
		expString: "LookupSPO(?id #12 #54 #76)",
		expKey:    "LookupSPO ?id #12 #54 #76",
	},
}

func Test_lookups_String(t *testing.T) {
	for i, tc := range lookupTests {
		assert.Equal(t, tc.expString, tc.op.String(),
			"test %d", i)
	}
}

func Test_lookups_Key(t *testing.T) {
	for i, tc := range lookupTests {
		assert.Equal(t, tc.expKey, cmp.GetKey(tc.op),
			"test %d, expected string %s", i, tc.expString)
	}
}

func Test_lookups_Terms(t *testing.T) {
	for _, tc := range lookupTests {
		type HasTerms interface {
			Terms() []Term
		}
		op, ok := tc.op.(HasTerms)
		if assert.True(t, ok, "%T should implement Terms()", op) {
			terms := op.Terms()
			assert.Len(t, terms, 4, "Terms() should return 4 values for expString: %s", tc.expString)
			set := make(map[string]struct{}, 4)
			for _, term := range terms {
				set[cmp.GetKey(term)] = struct{}{}
			}
			assert.Len(t, set, 4, "Terms() should return 4 distinct values for expString: %s", tc.expString)
		}
	}
}
