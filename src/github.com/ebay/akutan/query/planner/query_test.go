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
	"fmt"
	"strings"
	"testing"

	"github.com/ebay/akutan/query/parser"
	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/query/planner/search"
	"github.com/ebay/akutan/rpc"
	"github.com/stretchr/testify/assert"
)

var query2 = &parser.Query{Where: parser.WhereClause{
	// All OIDs
	{
		ID:        &parser.LiteralID{Value: 1},
		Subject:   &parser.LiteralID{Value: 2},
		Predicate: &parser.LiteralID{Value: 3},
		Object:    &parser.LiteralID{Value: 4},
	},
	// All distinct vars
	{
		ID:        &parser.Variable{Name: "varid"},
		Subject:   &parser.Variable{Name: "varsub"},
		Predicate: &parser.Variable{Name: "varpred"},
		Object:    &parser.Variable{Name: "varobj"},
	},
	// All same var
	{
		ID:        &parser.Variable{Name: "place"},
		Subject:   &parser.Variable{Name: "place"},
		Predicate: &parser.Variable{Name: "place"},
		Object:    &parser.Variable{Name: "place"},
	},
	// Entirely nil
	{
		ID:        &parser.Nil{},
		Subject:   &parser.Nil{},
		Predicate: &parser.Nil{},
		Object:    &parser.Nil{},
	},
	// Literal
	{
		ID:        &parser.Nil{},
		Subject:   &parser.Nil{},
		Predicate: &parser.Nil{},
		Object:    &parser.LiteralInt{Unit: parser.NoUnit, Value: 1000},
	},
}}

var query3 = mustParse(`
	?place <population> ?pop
	?pop <gt> 1000000
	?pop <lt> 2000000
`)

// population predicate used with literal.
// 52 predicate used with selection.
// color predicate unknown, must use inference.
var query4 = mustParse(`
	?place <population> ?pop
	?place2 <population> 1352
	?place #42 ?mol
	?mol <gt> 1
	?place <color> ?col
`)

var query5 = mustParse(`
	?place <population> ?pop
	?person <from> ?town
	?town <locatedin> ?place
`)

var query6 = mustParse(`
	?place <population> ?pop
	?person <from> ?place
`)

var query7 = mustParse(`
	?place <population> ?pop
	?person <from> ?town
`)

var query8 = mustParse(`
	?place <population> ?pop
	?pop <in> {100, 200}
	?place <in> {<rome>, <london>}
`)

var query9 = mustParse(`
	?place <population> ?pop
	?person <from> ?place
	?place <locatedin>? <europe>
`)

func Test_createLogicalTree(t *testing.T) {
	assert := assert.New(t)
	expr, err := createLogicalTree(query1)
	assert.NoError(err)
	assert.Equal(`
SelectLit ?pop > 1000000
	InnerJoin ?place
		InnerJoin ?place
			Infer(_ ?place <locatedin> <europe>)
			Infer(_ ?place <color> <blue>)
		Lookup(_ ?place <population> ?pop)
`, "\n"+expr.String(), "actual:\n%v", expr)
}

func Test_extractTerms(t *testing.T) {
	assert := assert.New(t)
	vars := new(variableScope)
	test := func(query *parser.Query, queryIdx int, expected string) *termLine {
		tl, err := extractTerms(query.Where[queryIdx], vars)
		assert.NoError(err)
		assert.NotNil(tl, "term line is nil for query line: %d", queryIdx+1)
		assert.Equal(expected, tl.String(), "term line is incorrect for query line: %d", queryIdx+1)
		return tl
	}
	_ = test(query2, 0, "#1 #2 #3 #4")
	_ = test(query2, 1, "?varid ?varsub ?varpred ?varobj")
	tl2 := test(query2, 2, "?place ?place ?place ?place")

	assert.Equal(fmt.Sprintf("%p", tl2.id),
		fmt.Sprintf("%p", tl2.subject))
	assert.Equal(fmt.Sprintf("%p", tl2.id),
		fmt.Sprintf("%p", tl2.predicate))
	assert.Equal(fmt.Sprintf("%p", tl2.id),
		fmt.Sprintf("%p", tl2.object))

	tl3 := test(query2, 3, "_ _ _ _")
	assert.Len(map[plandef.Term]bool{
		tl3.id:        true,
		tl3.subject:   true,
		tl3.predicate: true,
		tl3.object:    true,
	}, 4, "expected distinct *DontCare for id, subject, predicate, object")
	test(query2, 4, "_ _ _ 1000")

	testUnknown := func(invalidate func(*parser.Quad) string) {
		t.Helper()
		quad := &parser.Quad{
			ID:        &parser.Nil{},
			Subject:   &parser.Nil{},
			Predicate: &parser.Nil{},
			Object:    &parser.LiteralID{Value: 0},
		}
		expected := invalidate(quad)
		tl, err := extractTerms(quad, vars)
		assert.Nil(tl, "term line for invalid query is not nil")
		assert.EqualError(err, expected)
	}
	testUnknown(func(quad *parser.Quad) string {
		quad.ID = &parser.Entity{Value: "entity"}
		return `unexpected term type for ID: &parser.Entity{ID:0x0, Value:"entity"}`
	})
	testUnknown(func(quad *parser.Quad) string {
		quad.Subject = &parser.Entity{Value: "entity"}
		return `unexpected term type for Subject: &parser.Entity{ID:0x0, Value:"entity"}`
	})
	testUnknown(func(quad *parser.Quad) string {
		quad.Predicate = &parser.Entity{Value: "entity"}
		return `unexpected term type for Predicate: &parser.Entity{ID:0x0, Value:"entity"}`
	})
	testUnknown(func(quad *parser.Quad) string {
		quad.Object = &parser.Entity{Value: "entity"}
		return `unexpected term type for Object: &parser.Entity{ID:0x0, Value:"entity"}`
	})
}

func Test_extractOps(t *testing.T) {
	assert := assert.New(t)
	ops, err := extractOps(query3, new(variableScope))
	assert.NoError(err)
	assert.Len(ops.selectionByVar, 1)
	assert.Len(ops.leaves, 1)
	assert.Len(ops.specificityForLookup, 1)
	assert.Contains(ops.selectionByVar, "pop")
	assert.Equal("[SelectLit ?pop > 1000000, SelectLit ?pop < 2000000]", ops.selectionByVar["pop"].String())
	assert.Equal("Infer(_ ?place <population> ?pop)", ops.leaves[0].String())

	ops, err = extractOps(query9, new(variableScope))
	assert.NoError(err)
	assert.Len(ops.leaves, 3)
	assert.Len(ops.specificityForLookup, 3)
	assert.Empty(ops.selectionByVar)
	assert.Equal(parser.MatchRequired, ops.specificityOf(ops.leaves[0]))
	assert.Equal(parser.MatchRequired, ops.specificityOf(ops.leaves[1]))
	assert.Equal(parser.MatchOptional, ops.specificityOf(ops.leaves[2]))

	q := parseQuery(t, parser.QueryFactPattern, `
		?src <type> p:a
		?dest <type> p:b
		?src <knows> ?dest
		?src <notEqual> ?dest
		?src <gt> 42
	`)
	ops, err = extractOps(q, new(variableScope))
	assert.NoError(err)
	if assert.Contains(ops.selectionByVar, "src") {
		assert.Equal("[SelectLit ?src > 42, SelectVar ?src != ?dest]", ops.selectionByVar["src"].String())
	}
}

func Test_opSet_RequiredAndOptionalLeaves(t *testing.T) {
	assert := assert.New(t)
	ops, err := extractOps(query9, new(variableScope))
	assert.NoError(err)
	assert.Len(ops.leaves, 3)
	req, opt := ops.requiredAndOptionalLeaves()
	assert.Len(req, 2)
	assert.Len(opt, 1)
	assert.Equal("Infer(_ ?place <population> ?pop)", req[0].String())
	assert.Equal("Infer(_ ?person <from> ?place)", req[1].String())
	assert.Equal("Infer(_ ?place <locatedin> <europe>)", opt[0].String())
}

func Test_extractOps_setLiteral(t *testing.T) {
	assert := assert.New(t)
	ops, err := extractOps(query8, new(variableScope))
	assert.NoError(err)
	assert.Len(ops.selectionByVar, 0)
	assert.Len(ops.leaves, 3)
	var enumerates []*plandef.Enumerate
	for _, leaf := range ops.leaves {
		if leaf, isEnum := leaf.(*plandef.Enumerate); isEnum {
			enumerates = append(enumerates, leaf)
		}
	}
	if assert.Len(enumerates, 2) {
		assert.Equal("Enumerate ?pop {100, 200}", enumerates[0].String())
		assert.Equal("Enumerate ?place {<rome>, <london>}", enumerates[1].String())
	}
}

func Test_squashSelection(t *testing.T) {
	assert := assert.New(t)
	type test struct {
		in  []plandef.SelectClause
		out []plandef.SelectClause
	}
	tests := []test{
		{
			in: []plandef.SelectClause{
				{Comparison: rpc.OpGreater, Literal1: i64lit(100)},
				{Comparison: rpc.OpLess, Literal1: i64lit(200)},
			},
			out: []plandef.SelectClause{
				{Comparison: rpc.OpRangeExcExc, Literal1: i64lit(100), Literal2: i64lit(200)},
			},
		},
		{
			in: []plandef.SelectClause{
				{Comparison: rpc.OpGreater, Literal1: i64lit(100)},
				{Comparison: rpc.OpLessOrEqual, Literal1: i64lit(200)},
			},
			out: []plandef.SelectClause{
				{Comparison: rpc.OpRangeExcInc, Literal1: i64lit(100), Literal2: i64lit(200)},
			},
		},
		{
			in: []plandef.SelectClause{
				{Comparison: rpc.OpGreaterOrEqual, Literal1: i64lit(100)},
				{Comparison: rpc.OpLessOrEqual, Literal1: i64lit(200)},
			},
			out: []plandef.SelectClause{
				{Comparison: rpc.OpRangeIncInc, Literal1: i64lit(100), Literal2: i64lit(200)},
			},
		},
		{
			in: []plandef.SelectClause{
				{Comparison: rpc.OpGreaterOrEqual, Literal1: i64lit(100)},
				{Comparison: rpc.OpLess, Literal1: i64lit(200)},
			},
			out: []plandef.SelectClause{
				{Comparison: rpc.OpRangeIncExc, Literal1: i64lit(100), Literal2: i64lit(200)},
			},
		},
	}
	for _, test := range tests {
		for _, perm := range [][]int{{0, 1}, {1, 0}} {
			in := make([]*plandef.SelectLit, len(test.in))
			for i := range in {
				in[i] = &plandef.SelectLit{Clauses: []plandef.SelectClause{test.in[perm[i]]}}
			}
			actual := squashSelectLit(in)
			assert.True(len(actual) < 2)
			assert.Equal(selectStr(test.out), selectStr(actual[0].Clauses),
				"input   : %#v", in)
		}
	}
}

func i64lit(v int64) *plandef.Literal {
	return &plandef.Literal{
		Value: rpc.AInt64(v, 0),
	}
}

func selectStr(clauses []plandef.SelectClause) string {
	var strs []string
	for _, c := range clauses {
		strs = append(strs, c.String())
	}
	return strings.Join(strs, " ")
}

func Test_removeInference(t *testing.T) {
	assert := assert.New(t)
	ops, err := extractOps(query4, new(variableScope))
	assert.NoError(err)
	removeInference(ops)
	assert.Len(ops.leaves, 4)
	assert.Equal("Lookup(_ ?place <population> ?pop)", ops.leaves[0].String())
	assert.Equal("Lookup(_ ?place2 <population> 1352)", ops.leaves[1].String())
	assert.Equal("Lookup(_ ?place #42 ?mol)", ops.leaves[2].String())
	assert.Equal("Infer(_ ?place <color> ?col)", ops.leaves[3].String())
}

// assertBuildJoinTree will run buildJoinTree on the supplied query and assert
// that the expected tree is created. It returns the tree so that the caller can
// do additional tests on it if it needs to.
func assertBuildJoinTree(t *testing.T, q *parser.Query, expTree string) *search.IntoExpr {
	t.Helper()
	ops, err := extractOps(q, new(variableScope))
	assert.NoError(t, err)
	expr, err := buildJoinTree(ops)
	if assert.NoError(t, err) {
		assert.Equal(t, expTree, "\n"+expr.String())
	}
	return expr
}

func Test_buildJoinTree_easy(t *testing.T) {
	assertBuildJoinTree(t, query6, `
InnerJoin ?place
	Infer(_ ?place <population> ?pop)
	Infer(_ ?person <from> ?place)
`)
}

func Test_buildJoinTree_hard(t *testing.T) {
	assertBuildJoinTree(t, query5, `
InnerJoin ?town
	InnerJoin ?place
		Infer(_ ?place <population> ?pop)
		Infer(_ ?town <locatedin> ?place)
	Infer(_ ?person <from> ?town)
`)
}

func Test_buildJoinTree_LeftJoin(t *testing.T) {
	tree := assertBuildJoinTree(t, query9, `
LeftJoin ?place
	InnerJoin ?place
		Infer(_ ?place <population> ?pop)
		Infer(_ ?person <from> ?place)
	Infer(_ ?place <locatedin> <europe>)
`)
	leftLoopJoined := preImplementLeftLoopJoin(tree)
	assert.Equal(t, `
LoopJoin (left) ?place
	InnerJoin ?place
		Infer(_ ?place <population> ?pop)
		Infer(_ ?person <from> ?place)
	Infer(_ $place <locatedin> <europe>)
`, "\n"+leftLoopJoined.String())
}

func Test_buildJoinTree_LeftJoinHard(t *testing.T) {
	q := mustParse(`
		?place <locatedin>? ?region
		?place <population> ?pop
		?person <from> ?place
		?region <size> ?regionSize
	`)
	tree := assertBuildJoinTree(t, q, `
LeftJoin ?place
	InnerJoin ?place
		Infer(_ ?place <population> ?pop)
		Infer(_ ?person <from> ?place)
	InnerJoin ?region
		Infer(_ ?place <locatedin> ?region)
		Infer(_ ?region <size> ?regionSize)
`)
	leftLoopJoined := preImplementLeftLoopJoin(tree)
	assert.Equal(t, `
LoopJoin (left) ?place
	InnerJoin ?place
		Infer(_ ?place <population> ?pop)
		Infer(_ ?person <from> ?place)
	InnerJoin ?region
		Infer(_ $place <locatedin> ?region)
		Infer(_ ?region <size> ?regionSize)
`, "\n"+leftLoopJoined.String())
}

func Test_buildJoinTree_ManyLeftJoins(t *testing.T) {
	// This query is carefully ordered to exercise the case
	// where the left joins can't all be added in order in
	// a single pass. This also covers a left join joining
	// a left join
	q := parseQuery(t, parser.QueryFactPattern, `
		?region <size>? ?regionSize
		?place <locatedin>? ?region
		?place <population>? ?pop
		?person <from> ?place
	`)
	tree := assertBuildJoinTree(t, q, `
LeftJoin ?region
	LeftJoin ?place
		LeftJoin ?place
			Infer(_ ?person <from> ?place)
			Infer(_ ?place <locatedin> ?region)
		Infer(_ ?place <population> ?pop)
	Infer(_ ?region <size> ?regionSize)
`)
	leftLoopJoined := preImplementLeftLoopJoin(tree)
	assert.Equal(t, `
LoopJoin (left) ?region
	LoopJoin (left) ?place
		LoopJoin (left) ?place
			Infer(_ ?person <from> ?place)
			Infer(_ $place <locatedin> ?region)
		Infer(_ $place <population> ?pop)
	Infer(_ $region <size> ?regionSize)
`, "\n"+leftLoopJoined.String())
}

// Tests the query pattern  that requires a binding from a left join to be
// pushed way down into the query
func Test_buildJoinTree_LeftJoinBindings(t *testing.T) {
	q := parseQuery(t, parser.QueryFactPattern, `
		?src <name> <bob_12345>
		?src www:attribute? ?a
		?a www:name ?n
		?a www:value ?v
	`)
	tree := assertBuildJoinTree(t, q, `
LeftJoin ?src
	Infer(_ ?src <name> <bob_12345>)
	InnerJoin ?a
		InnerJoin ?a
			Infer(_ ?src www:attribute ?a)
			Infer(_ ?a www:name ?n)
		Infer(_ ?a www:value ?v)
`)
	leftJoined := preImplementLeftLoopJoin(tree)
	assert.Equal(t, `
LoopJoin (left) ?src
	Infer(_ ?src <name> <bob_12345>)
	InnerJoin ?a
		InnerJoin ?a
			Infer(_ $src www:attribute ?a)
			Infer(_ ?a www:name ?n)
		Infer(_ ?a www:value ?v)
`, "\n"+leftJoined.String())
}

func Test_preImplementLeftLoopJoin_Bindings(t *testing.T) {
	// This test is super annoying to write because the parser
	// doesn't currently support set literals, so testing that
	// the enumerate op got correctly bound is a PITA because you
	// have to manually build the expression tree.
	varS := &plandef.Variable{Name: "s"}
	set := plandef.Enumerate{
		Output: varS,
		Values: []plandef.FixedTerm{&plandef.OID{Value: 42}},
	}
	lookup := lookupOperator{
		id:        new(plandef.DontCare),
		subject:   varS,
		predicate: &plandef.OID{Value: 2},
		object:    &plandef.Literal{Value: rpc.AKID(43)},
	}
	join := leftJoinOperator{
		variables: plandef.VarSet{varS},
	}
	expr := search.NewExpr(&join, search.NewExpr(&set), search.NewExpr(&lookup))
	leftLooped := preImplementLeftLoopJoin(expr)
	assert.Equal(t, `
LoopJoin (left) ?s
	Enumerate ?s {#42}
	Lookup(_ $s #2 #43)
`, "\n"+leftLooped.String())

	// And the other way around.
	expr = search.NewExpr(&join, search.NewExpr(&lookup), search.NewExpr(&set))
	leftLooped = preImplementLeftLoopJoin(expr)
	assert.Equal(t, `
LoopJoin (left) ?s
	Lookup(_ ?s #2 #43)
	Enumerate $s {#42}
`, "\n"+leftLooped.String())
}

func Test_createLogicalTree_SelectVar(t *testing.T) {
	assert := assert.New(t)
	q := mustParse(`
		?src <name> <bob_12345>
		?src <knows> ?f
		?dest <knows> ?f
		?dest <notEqual> ?src`)
	expr, err := createLogicalTree(q)
	assert.NoError(err)
	assert.Equal(`
SelectVar ?dest != ?src
	InnerJoin ?f
		InnerJoin ?src
			Infer(_ ?src <name> <bob_12345>)
			Infer(_ ?src <knows> ?f)
		Infer(_ ?dest <knows> ?f)
`, "\n"+expr.String())
}

func Test_createLogicalTree_SelectLit_Squashed(t *testing.T) {
	assert := assert.New(t)
	q := mustParse(`
		?src <name> <bob_12345>
		?src <size> ?s
		?s <gt> 10
		?s <lt> 20`)
	expr, err := createLogicalTree(q)
	assert.NoError(err)
	assert.Equal(`
SelectLit ?s in (10, 20)
	InnerJoin ?src
		Infer(_ ?src <name> <bob_12345>)
		Lookup(_ ?src <size> ?s)
`, "\n"+expr.String())
}

func Test_createLogicalTree_NotAllOptional(t *testing.T) {
	assert := assert.New(t)
	q := parseQuery(t, parser.QueryFactPattern, `
		?src <name>? <bob_12345>
		?src <knows>? ?f
	`)
	_, err := createLogicalTree(q)
	assert.EqualError(err, "query can't consist of all optional matches")
}

func Test_createLogicalTree_NotOptionalCartesianJoin(t *testing.T) {
	assert := assert.New(t)
	q := mustParse(`
		?src <name> <bob_12345>
		?dest <name>? ?name
	`)
	_, _, err := Prepare(context.Background(), q, nil, testSearchOpts)
	assert.EqualError(err, "query may not contain Cartesian product: all query lines must be connected via some join")
}

func Test_createLogicalTree_NotDoubleLeftJoined(t *testing.T) {
	assert := assert.New(t)
	q := parseQuery(t, parser.QueryFactPattern, `
	?alice <knows> ?eve
	?alice www:attribute? ?attr
	?eve www:attribute? ?attr
	?attr www:value ?v
	`)
	_, _, err := Prepare(context.Background(), q, nil, testSearchOpts)
	assert.EqualError(err, "invalid query: variable(s) ?attr are used in 2 different optional trees")

	q = parseQuery(t, parser.QueryFactPattern, `
	?alice <knows> ?eve
	?alice www:attribute? ?attr1
	?eve www:attribute? ?attr2
	?attr1 www:value ?v
	?attr2 www:value ?v
	`)
	_, _, err = Prepare(context.Background(), q, nil, testSearchOpts)
	// This error message while correct is probably not the one that would
	// immediately spring to mind as the correct one. It'd probably be more
	// useful if this error mentioned ?v instead. But as the subtree for ?attr1
	// ends up consuming the final 2 query rows, the error ends up being
	// on ?attr2. Ideally we'd fix this, but it seems increasingly likely that
	// the implicit subtree in the current syntax should be changed to be an
	// explicit subtree, similar to how sparql does it. Given this I don't think
	// its worth investing in make this better at this point.
	assert.EqualError(err, "invalid query: variable(s) ?attr2 are used in 2 different optional trees")
}

func Test_buildJoinTree_errors(t *testing.T) {
	assert := assert.New(t)
	_, err := buildJoinTree(&opSet{})
	assert.Error(err)
	assert.Regexp("no lookups", err.Error())
	ops, err := extractOps(query7, new(variableScope))
	assert.NoError(err)
	_, err = buildJoinTree(ops)
	assert.Error(err)
	assert.Regexp("Cartesian product", err.Error())
}

func Test_createLogicalTree_setLiterals(t *testing.T) {
	assert := assert.New(t)
	expr, err := createLogicalTree(query8)
	assert.NoError(err)
	assert.Equal(`
InnerJoin ?place
	InnerJoin ?pop
		Infer(_ ?place <population> ?pop)
		Enumerate ?pop {100, 200}
	Enumerate ?place {<rome>, <london>}
`, "\n"+expr.String(), "actual:\n%v", expr)
}

func Test_createLogicalTree_Select(t *testing.T) {
	assert := assert.New(t)
	query := parseQuery(t, parser.QuerySparql, `SELECT * WHERE {
		?place <population> ?pop
		?pop <in> {100, 200}
	}`)
	expr, err := createLogicalTree(query)
	assert.NoError(err)
	assert.Equal(`
ExternalIDs
	InnerJoin ?pop
		Infer(_ ?place <population> ?pop)
		Enumerate ?pop {100, 200}
`, "\n"+expr.String(), "actual:\n%v", expr)

	query = parseQuery(t, parser.QuerySparql, `select ?place {
							?place <population> 1000
						}`)
	expr, err = createLogicalTree(query)
	assert.NoError(err)
	assert.Equal(`
ExternalIDs
	Project ?place
		Lookup(_ ?place <population> 1000)
`, "\n"+expr.String())

	query = parseQuery(t, parser.QuerySparql, `select (count(?place) as ?places) {
							?place <population> 2000
						}`)
	expr, err = createLogicalTree(query)
	assert.NoError(err)
	assert.Equal(`
ExternalIDs
	Project (COUNT(?place) AS ?places)
		Lookup(_ ?place <population> 2000)
`, "\n"+expr.String())

	query = parseQuery(t, parser.QuerySparql, `select (count(*) as ?places) {
							?place <population> 2000
						}`)
	expr, err = createLogicalTree(query)
	assert.NoError(err)
	assert.Equal(`
ExternalIDs
	Project (COUNT(*) AS ?places)
		Lookup(_ ?place <population> 2000)
`, "\n"+expr.String())

	query = parseQuery(t, parser.QuerySparql, `select ?place ?pop {
							?place <population> ?pop
							?pop <in> {100, 200}
						} ORDER BY ?place DESC(?pop)`)
	expr, err = createLogicalTree(query)
	assert.NoError(err)
	assert.Equal(`
Project ?place ?pop
	OrderBy ASC(?place) DESC(?pop)
		ExternalIDs
			InnerJoin ?pop
				Infer(_ ?place <population> ?pop)
				Enumerate ?pop {100, 200}
`, "\n"+expr.String())

	query = parseQuery(t, parser.QuerySparql, `select * {
							?place <population> ?pop
							?pop <in> {100, 200}
						} ORDER BY ASC(?place)`)
	expr, err = createLogicalTree(query)
	assert.NoError(err)
	assert.Equal(`
OrderBy ASC(?place)
	ExternalIDs
		InnerJoin ?pop
			Infer(_ ?place <population> ?pop)
			Enumerate ?pop {100, 200}
`, "\n"+expr.String())

	query = parseQuery(t, parser.QuerySparql, `select * {
							?place <population> ?pop
							?pop <in> {100, 200}
						} ORDER BY ASC(?place)
						  LIMIT 5 OFFSET 0`)
	expr, err = createLogicalTree(query)
	assert.NoError(err)
	assert.Equal(`
LimitOffset (Lmt 5 Off 0)
	OrderBy ASC(?place)
		ExternalIDs
			InnerJoin ?pop
				Infer(_ ?place <population> ?pop)
				Enumerate ?pop {100, 200}
`, "\n"+expr.String())

	query = parseQuery(t, parser.QuerySparql, `select ?place {
							?place <population> ?pop
							?pop <in> {100, 200}
						} ORDER BY ASC(?place)
						LIMIT 5 OFFSET 0`)
	expr, err = createLogicalTree(query)
	assert.NoError(err)
	assert.Equal(`
LimitOffset (Lmt 5 Off 0)
	Project ?place
		OrderBy ASC(?place)
			ExternalIDs
				InnerJoin ?pop
					Infer(_ ?place <population> ?pop)
					Enumerate ?pop {100, 200}
`, "\n"+expr.String())

	query = parseQuery(t, parser.QuerySparql, `select * {
							?place <population> ?pop
							?pop <in> {100, 200}
						} LIMIT 10 OFFSET 0`)
	expr, err = createLogicalTree(query)
	assert.NoError(err)
	assert.Equal(`
ExternalIDs
	LimitOffset (Lmt 10 Off 0)
		InnerJoin ?pop
			Infer(_ ?place <population> ?pop)
			Enumerate ?pop {100, 200}
`, "\n"+expr.String())

	query = parseQuery(t, parser.QuerySparql, `select ?place ?pop {
							?place <population> ?pop
							?pop <in> {100, 200}
						} LIMIT 10 OFFSET 0`)
	expr, err = createLogicalTree(query)
	assert.NoError(err)
	assert.Equal(`
ExternalIDs
	LimitOffset (Lmt 10 Off 0)
		Project ?place ?pop
			InnerJoin ?pop
				Infer(_ ?place <population> ?pop)
				Enumerate ?pop {100, 200}
`, "\n"+expr.String())

	query = parseQuery(t, parser.QuerySparql, `select * {
							?place <population> ?pop
							?pop <in> {100, 200}
						} LIMIT 10`)
	expr, err = createLogicalTree(query)
	assert.NoError(err)
	assert.Equal(`
ExternalIDs
	LimitOffset (Lmt 10)
		InnerJoin ?pop
			Infer(_ ?place <population> ?pop)
			Enumerate ?pop {100, 200}
`, "\n"+expr.String())

	query = parseQuery(t, parser.QuerySparql, `select * {
							?place <population> ?pop
							?pop <in> {100, 200}
						} OFFSET 50`)
	expr, err = createLogicalTree(query)
	assert.NoError(err)
	assert.Equal(`
ExternalIDs
	LimitOffset (Off 50)
		InnerJoin ?pop
			Infer(_ ?place <population> ?pop)
			Enumerate ?pop {100, 200}
`, "\n"+expr.String())

	query = parseQuery(t, parser.QuerySparql, `SELECT DISTINCT * WHERE {
	?place <population> ?pop
	?pop <in> {100, 200}
}`)
	expr, err = createLogicalTree(query)
	assert.NoError(err)
	assert.Equal(`
ExternalIDs
	Distinct
		InnerJoin ?pop
			Infer(_ ?place <population> ?pop)
			Enumerate ?pop {100, 200}
`, "\n"+expr.String(), "actual:\n%v", expr)

	query = parseQuery(t, parser.QuerySparql, `select distinct ?place {
	?place <population> 1000
}`)
	expr, err = createLogicalTree(query)
	assert.NoError(err)
	assert.Equal(`
ExternalIDs
	Distinct
		Project ?place
			Lookup(_ ?place <population> 1000)
`, "\n"+expr.String())

	query = parseQuery(t, parser.QuerySparql,
		`select distinct (count(?place) as ?places) {
			?place <population> 2000
	}`)
	expr, err = createLogicalTree(query)
	assert.NoError(err)
	assert.Equal(`
ExternalIDs
	Distinct
		Project (COUNT(?place) AS ?places)
			Lookup(_ ?place <population> 2000)
`, "\n"+expr.String())

	query = parseQuery(t, parser.QuerySparql, `select distinct * {
							?place <population> ?pop
							?pop <in> {100, 200}
						} ORDER BY ASC(?place)`)
	expr, err = createLogicalTree(query)
	assert.NoError(err)
	assert.Equal(`
Distinct
	OrderBy ASC(?place)
		ExternalIDs
			InnerJoin ?pop
				Infer(_ ?place <population> ?pop)
				Enumerate ?pop {100, 200}
`, "\n"+expr.String())

	query = parseQuery(t, parser.QuerySparql, `select distinct ?place ?pop {
							?place <population> ?pop
							?pop <in> {100, 200}
						} LIMIT 10 OFFSET 0`)
	expr, err = createLogicalTree(query)
	assert.NoError(err)
	assert.Equal(`
ExternalIDs
	LimitOffset (Lmt 10 Off 0)
		Distinct
			Project ?place ?pop
				InnerJoin ?pop
					Infer(_ ?place <population> ?pop)
					Enumerate ?pop {100, 200}
`, "\n"+expr.String())

	query = parseQuery(t, parser.QuerySparql, `select distinct ?place {
										?place <population> ?pop
										?pop <in> {100, 200}
									} ORDER BY ASC(?place)
									LIMIT 5 OFFSET 0`)
	expr, err = createLogicalTree(query)
	assert.NoError(err)
	assert.Equal(`
LimitOffset (Lmt 5 Off 0)
	Distinct
		Project ?place
			OrderBy ASC(?place)
				ExternalIDs
					InnerJoin ?pop
						Infer(_ ?place <population> ?pop)
						Enumerate ?pop {100, 200}
`, "\n"+expr.String())

}

func Test_createLogicalTree_Ask(t *testing.T) {
	assert := assert.New(t)
	query := parseQuery(t, parser.QuerySparql, `ASK WHERE {
		?place <population> ?pop
		?pop <in> {100, 200}
	}`)
	expr, err := createLogicalTree(query)
	assert.NoError(err)
	assert.Equal(`
Ask ?result
	InnerJoin ?pop
		Infer(_ ?place <population> ?pop)
		Enumerate ?pop {100, 200}
`, "\n"+expr.String(), "actual:\n%v", expr)
}

func Test_ToVars(t *testing.T) {
	assert := assert.New(t)
	varS := &plandef.Variable{Name: "s"}
	varO := &plandef.Variable{Name: "o"}
	varQ := &plandef.Variable{Name: "q"}
	bindS := &plandef.Binding{Var: varS}
	bindQ := &plandef.Binding{Var: varQ}
	v := termsToVars(varS, varO, bindS, bindQ, new(plandef.DontCare), new(plandef.Literal))
	assert.Equal(plandef.VarSet{varO, varQ, varS}, v)

	l := lookupOperator{id: new(plandef.DontCare), subject: varS, predicate: varQ, object: varO}
	assert.Equal(plandef.VarSet{varO, varQ, varS}, getJoinableVars(&l))

	e := plandef.Enumerate{Output: varS}
	assert.Equal(plandef.VarSet{varS}, getJoinableVars(&e))

	e = plandef.Enumerate{Output: bindQ}
	assert.Equal(plandef.VarSet{varQ}, getJoinableVars(&e))
}

func Test_Variables(t *testing.T) {
	assert := assert.New(t)
	v := new(variableScope)
	bob := v.named("bob")
	eve := v.named("eve")
	assert.True(bob == v.named("bob"))
	assert.True(eve == v.named("eve"))
	assert.False(v.named("eve") == v.named("bob"))
	assert.Equal("bob", v.named("bob").Name)
	assert.Equal("eve", v.named("eve").Name)
	assert.Equal(2, len(v.vars))
}
