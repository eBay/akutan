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

package planner

import (
	"testing"

	"github.com/ebay/beam/query/parser"
	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/util/cmp"
	"github.com/stretchr/testify/assert"
)

func Test_lookupOperator_StringKey(t *testing.T) {
	assert := assert.New(t)
	op := &lookupOperator{
		id:        &plandef.Variable{Name: "id"},
		subject:   &plandef.Variable{Name: "subject"},
		predicate: &plandef.Variable{Name: "predicate"},
		object:    &plandef.Variable{Name: "object"},
		infer:     false,
	}
	assert.Equal("Lookup(?id ?subject ?predicate ?object)", op.String())
	assert.Equal("Lookup ?id ?subject ?predicate ?object", cmp.GetKey(op))
	op.infer = true
	assert.Equal("Infer(?id ?subject ?predicate ?object)", op.String())
	assert.Equal("Infer ?id ?subject ?predicate ?object", cmp.GetKey(op))
}

func Test_implementLookup_infers(t *testing.T) {
	implementGenerates(t, []generatesTest{
		{
			query:   `?s <locatedIn> <Europe>`,
			pattern: `InferPO(_ ?s <locatedIn> <Europe>)`,
		},
		{
			query:   `<Rome> <locatedIn> <Europe>`,
			pattern: `InferSPO(_ <Rome> <locatedIn> <Europe>)`,
		},
		{
			query:   `<Rome> <locatedIn> ?place`,
			pattern: `InferSP(_ <Rome> <locatedIn> ?place)`,
		},
	})
}

func Test_implementLookupPOCmp(t *testing.T) {
	implementGenerates(t, []generatesTest{
		{
			query: `
				?place <population> ?pop
				?pop <gt> 20
			`,
			pattern: `LookupPOCmp(_ ?place <population> ?pop > 20)`,
		},
	})
}

func Test_noLookupPOCmpForNotEqual(t *testing.T) {
	assert := assert.New(t)
	q := parseQuery(t, parser.QueryFactPattern, `
		?place <locatedin> <rome>
		?place <population> ?pop
		?pop <notEqual> 20
	`)
	qt, err := createLogicalTree(q)
	assert.NoError(err)
	space := runSearch(t, qt)
	assert.NoError(contains(space, `
		SelectLit ?pop != 20
			InnerJoin ?place
				Infer(_ ?place <locatedin> <rome>)
				Lookup(_ ?place <population> ?pop)
	`))
	assert.NoError(excludes(space, `LookupPOCmp(_ ?place <population> ?pop != 20)`))
}

func Test_Prepare_regression201806261143_generateInferSP(t *testing.T) {
	implementGenerates(t, []generatesTest{
		{
			query:   `<postcard1> <type> ?place`,
			pattern: `InferSP(_ <postcard1> <type> ?place)`,
		},
	})
}
