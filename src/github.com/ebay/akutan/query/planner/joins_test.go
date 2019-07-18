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

	"github.com/ebay/akutan/query/parser"
	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/query/planner/search"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/cmp"
	"github.com/stretchr/testify/assert"
)

func Test_joinAssocCommute(t *testing.T) {
	query := `
		?place <locatedin> <europe>
		?place <color> <blue>
		?place <type> <city>
	`
	exploreGenerates(t, []generatesTest{
		{
			query: query,
			pattern: `
InnerJoin ?place
	Infer(_ ?place <locatedin> <europe>)
	InnerJoin ?place
		Infer(_ ?place <type> <city>)
		Infer(_ ?place <color> <blue>)
			`,
		},
		{
			query: query,
			pattern: `
InnerJoin ?place
	Infer(_ ?place <locatedin> <europe>)
	InnerJoin ?place
		Infer(_ ?place <color> <blue>)
		Infer(_ ?place <type> <city>)
			`,
		},
		{
			query: query,
			pattern: `
InnerJoin ?place
	Infer(_ ?place <color> <blue>)
	InnerJoin ?place
		Infer(_ ?place <locatedin> <europe>)
		Infer(_ ?place <type> <city>)
			`,
		},
		{
			query: query,
			pattern: `
InnerJoin ?place
	Infer(_ ?place <color> <blue>)
	InnerJoin ?place
		Infer(_ ?place <type> <city>)
		Infer(_ ?place <locatedin> <europe>)
			`,
		},
		{
			query: query,
			pattern: `
InnerJoin ?place
	Infer(_ ?place <type> <city>)
	InnerJoin ?place
		Infer(_ ?place <color> <blue>)
		Infer(_ ?place <locatedin> <europe>)
			`,
		},
		{
			query: query,
			pattern: `
InnerJoin ?place
	Infer(_ ?place <type> <city>)
	InnerJoin ?place
		Infer(_ ?place <locatedin> <europe>)
		Infer(_ ?place <color> <blue>)
			`,
		},
	})
}

func Test_ImplementLeftJoin(t *testing.T) {
	query := `
	?place <locatedin> <europe>
	?place <color>? <blue>
	?place <type> <city>
`
	implementGenerates(t, []generatesTest{
		{
			query: query,
			pattern: `
LoopJoin (left) ?place
	LoopJoin (inner) ?place
		InferPO(_ ?place <locatedin> <europe>)
		InferSPO(_ $place <type> <city>)
	InferSPO(_ $place <color> <blue>)
`,
		},
		{
			query: query,
			pattern: `
LoopJoin (left) ?place
	HashJoin (inner) ?place
		InferPO(_ ?place <locatedin> <europe>)
		InferPO(_ ?place <type> <city>)
	InferSPO(_ $place <color> <blue>)
`,
		},
	})
	// Because of the work around in preImplementLeftLoopJoin we never get a
	// starting tree with a leftJoin logical operator. This means we don't
	// currently generate a plan that uses a HashJoin (left). When the work
	// around is removed then there are more potential plans to add here. For
	// example we'd see something like
	//
	// HashJoin (left) ?place
	//      HashJoin (inner) ?place
	//          InferPO(_ ?place <locatedin> <europe>)
	//          InferPO(_ ?place <type> <city>)
	//      InferPO(_ ?place <color> <blue>)
}

func Test_ImplementLeftJoinRules(t *testing.T) {
	// this runs the planner against a starting expression rather than
	// a query so we're not at the mercy of the initial tree building
	varS := &plandef.Variable{Name: "s"}
	leftJoin := leftJoinOperator{variables: plandef.VarSet{varS}}
	leftIn := lookupOperator{
		id:        new(plandef.DontCare),
		subject:   varS,
		predicate: &plandef.OID{Value: 1},
		object:    &plandef.OID{Value: 2},
	}
	rightIn := lookupOperator{
		id:        new(plandef.DontCare),
		subject:   &plandef.OID{Value: 12},
		predicate: &plandef.OID{Value: 11},
		object:    varS,
	}
	space := runSearch(t, search.NewExpr(&leftJoin, search.NewExpr(&leftIn), search.NewExpr(&rightIn)))
	assert.NoError(t, contains(space, `
HashJoin (left) ?s
	LookupPO(_ ?s #1 #2)
	LookupSP(_ #12 #11 ?s)
`))
	assert.NoError(t, contains(space, `
LoopJoin (left) ?s
	LookupPO(_ ?s #1 #2)
	LookupSPO(_ #12 #11 $s)
`))
	assert.NoError(t, excludes(space, `
HashJoin (left) ?s
	LookupSP(_ #12 #11 ?s)
	LookupPO(_ ?s #1 #2)
`))
}

func Test_Prepare_regression201806271108(t *testing.T) {
	assert := assert.New(t)
	query := parseQuery(t, parser.QueryFactPattern, `
		?p <type> <product>
		?p2 <type> ?p
		?p2 <type> <blue>
	`)
	space, err := Parse(query, new(mockStats), testSearchOpts)
	assert.NoError(err)
	space.Explore()
	assert.NoError(contains(space, `
InnerJoin ?p
	Infer(_ ?p <type> <product>)
	Infer(_ ?p2 <type> ?p)
`))
	assert.NoError(contains(space, `
InnerJoin ?p2
	Infer(_ ?p2 <type> <blue>)
	Infer(_ ?p2 <type> ?p)
`))
	assert.NoError(excludes(space, `
Join
	Infer(_ ?p <type> <product>)
	Infer(_ ?p2 <type> <blue>)
`))
}

func Test_BindLookup(t *testing.T) {
	vars := plandef.VarSet{
		// note this need to be in alpha order to be a valid VarSet
		&plandef.Variable{Name: "m"},
		&plandef.Variable{Name: "p"},
		&plandef.Variable{Name: "t"},
		&plandef.Variable{Name: "z"}}

	// base case one of the fields in the lookup can be converted to a binding
	l := lookupOperator{
		id:        new(plandef.DontCare),
		subject:   &plandef.Variable{Name: "x"},
		predicate: vars[1],
		object:    &plandef.Literal{Value: rpc.AString("Bob", 0)},
	}
	b, wasBound := bindLookup(&l, vars)
	assert.True(t, wasBound)
	assert.Equal(t, &lookupOperator{
		id:        new(plandef.DontCare),
		subject:   &plandef.Variable{Name: "x"},
		predicate: &plandef.Binding{Var: vars[1]},
		object:    &plandef.Literal{Value: rpc.AString("Bob", 0)},
	}, b)

	// b already has bindings from 'vars' applied, so this shouldn't change anything
	b2, wasBound := bindLookup(b, vars)
	assert.False(t, wasBound)
	assert.Equal(t, b, b2)

	// none of the variables in vars[:1] are used in the lookup, this shouldn't do anything
	b3, wasBound := bindLookup(&l, vars[:1])
	assert.False(t, wasBound)
	assert.Equal(t, &l, b3)

	// all fields in the lookup get bound
	all := lookupOperator{
		id:        new(plandef.DontCare),
		subject:   vars[1],
		predicate: vars[2],
		object:    vars[3],
	}
	allBound, wasBound := bindLookup(&all, vars)
	assert.True(t, wasBound)
	assert.Equal(t, &lookupOperator{
		id:        new(plandef.DontCare),
		subject:   &plandef.Binding{Var: vars[1]},
		predicate: &plandef.Binding{Var: vars[2]},
		object:    &plandef.Binding{Var: vars[3]},
	}, allBound)
}

func Test_JoinsStringKey(t *testing.T) {
	vars := plandef.VarSet{
		&plandef.Variable{Name: "alice"},
		&plandef.Variable{Name: "bob"},
	}
	joinOp := &innerJoinOperator{variables: vars}
	assert.Equal(t, "InnerJoin ?alice ?bob", joinOp.String())
	assert.Equal(t, "InnerJoin ?alice ?bob", cmp.GetKey(joinOp))

	leftJoinOp := &leftJoinOperator{variables: vars}
	assert.Equal(t, "LeftJoin ?alice ?bob", leftJoinOp.String())
	assert.Equal(t, "LeftJoin ?alice ?bob", cmp.GetKey(leftJoinOp))
}
