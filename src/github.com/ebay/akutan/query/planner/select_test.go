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
	"github.com/stretchr/testify/assert"
)

func Test_pushDownSelect_Join_OneInput(t *testing.T) {
	assert := assert.New(t)
	query := parseQuery(t, parser.QueryFactPattern, `
		?pop <gt> 100
		?pop <lt> 200
		?place <population> ?pop
		?place <color> <blue>
	`)
	space, err := Parse(query, new(mockStats), testSearchOpts)
	assert.NoError(err)
	space.Explore()
	assert.NoError(contains(space, `
SelectLit ?pop in (100, 200)
	InnerJoin ?place
		Infer(_ ?place <color> <blue>)
		Lookup(_ ?place <population> ?pop)
	`))
	assert.NoError(excludes(space, `
InnerJoin ?place
	SelectLit ?pop in (100, 200)
		Infer(_ ?place <color> <blue>)
	Lookup(_ ?place <population> ?pop)
	`))
	assert.NoError(contains(space, `
InnerJoin ?place
	SelectLit ?pop in (100, 200)
		Lookup(_ ?place <population> ?pop)
	Infer(_ ?place <color> <blue>)
	`))
	assert.NoError(contains(space, `
InnerJoin ?place
	Infer(_ ?place <color> <blue>)
	SelectLit ?pop in (100, 200)
		Lookup(_ ?place <population> ?pop)
	`))
}

func Test_pushDownSelect_Join_BothInputs(t *testing.T) {
	assert := assert.New(t)
	query := parseQuery(t, parser.QueryFactPattern, `
	?pop <gt> 100
	?place <population> ?pop
	?place <color> ?pop
	?place <color> <blue>
`)
	space, err := Parse(query, new(mockStats), testSearchOpts)
	assert.NoError(err)
	space.Explore()
	assert.NoError(contains(space, `
SelectLit ?pop > 100
	InnerJoin ?place 
		Lookup(_ ?place <color> <blue>)
		InnerJoin ?place ?pop
			Lookup(_ ?place <population> ?pop)
			Lookup(_ ?place <color> ?pop)
	`))
	assert.NoError(contains(space, `
InnerJoin ?place
	Lookup(_ ?place <color> <blue>)
	InnerJoin ?place ?pop
		SelectLit ?pop > 100
			Lookup(_ ?place <population> ?pop)
		SelectLit ?pop > 100
			Lookup(_ ?place <color> ?pop)
`))
}

func Test_pushDownSelect_LoopJoin(t *testing.T) {
	assert := assert.New(t)
	varS := &plandef.Variable{Name: "s"}
	varV := &plandef.Variable{Name: "v"}
	leftJoin := &leftJoinOperator{variables: plandef.VarSet{varS}}
	leftIn := lookupOperator{
		id:        new(plandef.DontCare),
		subject:   varS,
		predicate: &plandef.OID{Value: 10},
		object:    &plandef.OID{Value: 11},
	}
	rightIn := lookupOperator{
		id:        new(plandef.DontCare),
		subject:   varS,
		predicate: &plandef.OID{Value: 20},
		object:    varV,
	}
	selection := plandef.SelectLit{
		Test: varV,
		Clauses: []plandef.SelectClause{
			{Comparison: rpc.OpGreater,
				Literal1: &plandef.Literal{Value: rpc.AInt64(10, 0)}},
		},
	}
	// ?v is available on the right input to the join
	startingExpr := search.NewExpr(
		&selection,
		search.NewExpr(leftJoin, search.NewExpr(&leftIn), search.NewExpr(&rightIn)),
	)
	space := runSearch(t, startingExpr)
	assert.NoError(contains(space, `
SelectLit ?v > 10
	LeftJoin ?s
		Lookup(_ ?s #10 #11)
		Lookup(_ ?s #20 ?v)
	`))
	assert.NoError(contains(space, `
LeftJoin ?s
	Lookup(_ ?s #10 #11)
	SelectLit ?v > 10
		Lookup(_ ?s #20 ?v)
	`))
	assert.NoError(contains(space, `
LoopJoin (left) ?s
	Lookup(_ ?s #10 #11)
	SelectLit ?v > 10
		Lookup(_ $s #20 ?v)
	`))
	assert.NoError(contains(space, `
HashJoin (left) ?s
	Lookup(_ ?s #10 #11)
	LookupPOCmp(_ ?s #20 ?v > 10)
	`))

	assert.NoError(excludes(space, `
LeftJoin ?s
	SelectLit ?v > 10
		Lookup(_ ?s #10 #11)
	Lookup(_ ?s #20 ?v)
	`))

	// same test, but with the join inputs flipped (so ?v is available on the
	// left input to the join)
	startingExpr = search.NewExpr(
		&selection,
		search.NewExpr(leftJoin, search.NewExpr(&rightIn), search.NewExpr(&leftIn)),
	)
	space = runSearch(t, startingExpr)
	assert.NoError(contains(space, `
SelectLit ?v > 10
	LeftJoin ?s
		Lookup(_ ?s #20 ?v)
		Lookup(_ ?s #10 #11)
	`))
	assert.NoError(contains(space, `
LeftJoin ?s
	SelectLit ?v > 10
		Lookup(_ ?s #20 ?v)
	Lookup(_ ?s #10 #11)
	`))

	// ?v is available on both inputs to the join
	leftIn = lookupOperator{
		id:        new(plandef.DontCare),
		subject:   varS,
		predicate: &plandef.OID{Value: 21},
		object:    varV,
	}
	leftJoin = &leftJoinOperator{variables: plandef.VarSet{varS, varV}}
	startingExpr = search.NewExpr(
		&selection,
		search.NewExpr(leftJoin, search.NewExpr(&leftIn), search.NewExpr(&rightIn)),
	)
	space = search.NewSpace(startingExpr, &planner{stats: new(mockStats)}, testSearchOpts)
	space.Explore()
	assert.NoError(contains(space, `
SelectLit ?v > 10
	LeftJoin ?s ?v
		Lookup(_ ?s #21 ?v)
		Lookup(_ ?s #20 ?v)
	`))
	assert.NoError(contains(space, `
LeftJoin ?s ?v
	SelectLit ?v > 10
		Lookup(_ ?s #21 ?v)
	SelectLit ?v > 10
		Lookup(_ ?s #20 ?v)
	`))
}

func Test_pushDownSelect_select(t *testing.T) {
	assert := assert.New(t)
	query := parseQuery(t, parser.QueryFactPattern, `
		?pop <gt> 100
		?col <gt> 10
		?place <population> ?pop
		?place <color> ?col
	`)
	space, err := Parse(query, new(mockStats), testSearchOpts)
	assert.NoError(err)
	space.Explore()
	assert.NoError(contains(space, `
SelectLit ?pop > 100
	SelectLit ?col > 10
		InnerJoin ?place
			Lookup(_ ?place <color> ?col)
			Lookup(_ ?place <population> ?pop)
	`))
	assert.NoError(contains(space, `
SelectLit ?col > 10
	SelectLit ?pop > 100
		InnerJoin ?place
			Lookup(_ ?place <color> ?col)
			Lookup(_ ?place <population> ?pop)
	`))
}

func Test_pushDownSelectVar(t *testing.T) {
	assert := assert.New(t)
	q := `
		?src <name> <bob_12345>
		?src <knows> ?f
		?dest <knows> ?f
		?src <notEqual> ?dest`
	expr, err := createLogicalTree(parseQuery(t, parser.QueryFactPattern, q))
	assert.NoError(err)
	space := runSearch(t, expr)
	assert.NoError(contains(space, `
InnerJoin ?src
	Infer(_ ?src <name> <bob_12345>)
	SelectVar ?src != ?dest
		InnerJoin ?f
			Infer(_ ?src <knows> ?f)
			Infer(_ ?dest <knows> ?f)
`))
	assert.NoError(contains(space, `
SelectVar ?src != ?dest
	InnerJoin ?f
		InnerJoin ?src 
			Infer(_ ?src <name> <bob_12345>)
			Infer(_ ?src <knows> ?f)
		Infer(_ ?dest <knows> ?f)
`))
}

func Test_pushDownSelectLitVar(t *testing.T) {
	assert := assert.New(t)
	q := `
		?src <style> ?f
		?src <size> ?sSize
		?sSize <gt> 42
		?src <notEqual> ?f`
	expr, err := createLogicalTree(parseQuery(t, parser.QueryFactPattern, q))
	assert.NoError(err)
	space := runSearch(t, expr)
	assert.NoError(contains(space, `
SelectVar ?src != ?f
	SelectLit ?sSize > 42
		InnerJoin ?src
			Infer(_ ?src <style> ?f)
			Lookup(_ ?src <size> ?sSize)
`))
	assert.NoError(contains(space, `
InnerJoin ?src
	SelectVar ?src != ?f
		Infer(_ ?src <style> ?f)
	SelectLit ?sSize > 42
		Lookup(_ ?src <size> ?sSize)
`))
	assert.NoError(contains(space, `
SelectLit ?sSize > 42
	SelectVar ?src != ?f
		InnerJoin ?src
			Infer(_ ?src <style> ?f)
			Lookup(_ ?src <size> ?sSize)
`))
}
