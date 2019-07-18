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
	"context"
	"os"
	"testing"

	"github.com/ebay/akutan/query/parser"
	"github.com/ebay/akutan/query/planner/search"
	"github.com/ebay/akutan/util/graphviz"
	"github.com/stretchr/testify/assert"
)

// our operators local to this package need to implement search.Operator
var _ = []search.Operator{
	new(lookupOperator),
	new(leftJoinOperator),
	new(innerJoinOperator),
}

type generatesTest struct {
	query   string
	pattern string
}

func exploreGenerates(t *testing.T, tests []generatesTest) {
	assert := assert.New(t)
	for _, test := range tests {
		assert.NotPanics(func() {
			query := parseQuery(t, parser.QueryFactPattern, test.query)
			space, err := Parse(query, new(mockStats), testSearchOpts)
			assert.NoError(err)
			space.Explore()
			assert.True(space.Contains(test.pattern),
				"query does not result in pattern: %+v\n%v", test, space)
		}, "panic in exploreGenerates test: %+v", test)
	}
}

func implementGenerates(t *testing.T, tests []generatesTest) {
	assert := assert.New(t)
	for _, test := range tests {
		assert.NotPanics(func() {
			query := parseQuery(t, parser.QueryFactPattern, test.query)
			space, err := Parse(query, new(mockStats), testSearchOpts)
			assert.NoError(err)
			space.Explore()
			space.Implement()
			assert.True(space.Contains(test.pattern),
				"query does not result in pattern: %+v\n%v", test, space)
		}, "panic in implementGenerates test: %+v", test)
	}
}

var query1 = mustParse(`
	?place <locatedin> <europe>
	?place <color> <blue>
	?place <population> ?pop
	?pop <gt> 1000000
`)

func Test_Prepare(t *testing.T) {
	assert := assert.New(t)
	space, plan, err := Prepare(context.Background(), query1, nil, testSearchOpts)
	assert.NoError(err)
	assert.NotNil(space)
	// This unit test is quite fragile because the exact plan is sensitive to
	// changes in the cost estimates. Any reasonable plan is good enough.
	assert.Contains([]string{
		`
LoopJoin (inner) ?place
	LoopJoin (inner) ?place
		LookupPOCmp(_ ?place <population> ?pop > 1000000)
		InferSPO(_ $place <locatedin> <europe>)
	InferSPO(_ $place <color> <blue>)
`, `
HashJoin (inner) ?place
	LookupPOCmp(_ ?place <population> ?pop > 1000000)
	HashJoin (inner) ?place
		InferPO(_ ?place <locatedin> <europe>)
		InferPO(_ ?place <color> <blue>)
`, `
HashJoin (inner) ?place
	HashJoin (inner) ?place
		InferPO(_ ?place <locatedin> <europe>)
		InferPO(_ ?place <color> <blue>)
	LookupPOCmp(_ ?place <population> ?pop > 1000000)
`,
	}, "\n"+plan.String())
}

func Test_Prepare_regression201806261143_noBestPlan(t *testing.T) {
	assert := assert.New(t)
	query := parseQuery(t, parser.QueryFactPattern, `
		<postcard1> ?pred <europe>
	`)
	space, err := Parse(query, new(mockStats), testSearchOpts)
	assert.NoError(err)
	space.Explore()
	space.Implement()
	space.PredictCosts()
	assert.Equal(`Group 1 [vars: ?pred]
	Infer(_ <postcard1> ?pred <europe>)
`, space.String())
	_, err = space.BestPlan()
	assert.Error(err)
	assert.Regexp("no physical plan", err.Error())
}

// Queries from http_query.go
func Test_Prepare_http_query(t *testing.T) {
	assert := assert.New(t)
	type Test struct {
		query         string
		parseErrRegex string
		planErrRegex  string
	}
	tests := []Test{
		{
			query: `
				?s <locatedin> <europe>
				?s <type> <product>
				?s <color> <red>
			`,
		},
		{
			query: `
				<postcard1> <type> ?t
				?t <isconcrete> ?yes
			`,
		},
		{
			query: `
					?p <type> <product>
					?p2 <type> ?p
			`,
		},
		{
			query: `
				?t <isconcrete> <yes>
				?p <type> ?t
				?p <color> <blue>
			`,
		},
		{
			query: `
				<letter> <isconcrete> <yes>
				<postcard1> <locatedin> <europe>
			`,
			parseErrRegex: "query may not contain Cartesian product",
		},
		{
			query: `
				<postcard1> <locatedin> <europe>
			`,
		},
		{
			query: `
				?p <type> <postcard>
				?p <size> ?sz
				?sz <gte> 7
			`,
		},
		{
			query: `
				?p <type> <postcard>
				?p <size> ?sz
				?sz <gte> 7
				?sz <lt> 50
			`,
		},
	}

	for _, test := range tests {
		query := parseQuery(t, parser.QueryFactPattern, test.query)
		space, err := Parse(query, new(mockStats), testSearchOpts)
		if test.parseErrRegex == "" {
			assert.NoError(err, "query:\n%v", test.query)
		} else {
			assert.Error(err, "query:\n%v", test.query)
			assert.Regexp(test.parseErrRegex, err.Error(), "query:\n%v", test.query)
			continue
		}
		space.Explore()
		space.Implement()
		space.PredictCosts()
		_, err = space.BestPlan()
		if test.planErrRegex == "" {
			if !assert.NoError(err, "query:\n%v\nspace:\n%v", test.query, space.String()) {
				err := graphviz.Create("error.svg", space.Graphviz, graphviz.Options{})
				assert.NoError(err, "Error creating error.svg")
				cwd, _ := os.Getwd()
				assert.Fail("INFO", "space written to %s/error.svg", cwd)
			}
		} else {
			assert.Error(err, "query:\n%v", test.query)
			assert.Regexp(test.planErrRegex, err.Error(), "query:\n%v", test.query)
			continue
		}
	}
}

func Test_Explore_SpaceCrash_Query(t *testing.T) {
	assert := assert.New(t)
	q := parseQuery(t, parser.QueryFactPattern, `
		?src <name> <bob_12345>
		?src <knows> ?f
		?dest <knows> ?f
		?dest <name> ?name
		?dest rdf:type ?type
		?src <notEqual> ?dest
		?dest p:a ?a
		?dest p:b ?b`)
	space, err := Parse(q, new(mockStats), search.Options{})
	assert.NoError(err)
	space.Explore()
	assert.NoError(space.CheckInvariants())
}
