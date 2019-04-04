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
	"log"
	"strings"
	"testing"

	"github.com/ebay/beam/query/parser"
	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/query/planner/search"
	"github.com/ebay/beam/rpc"
	"github.com/stretchr/testify/assert"
)

// Used by parseQuery to rewrite friendly names into OIDs.
var wellKnown = map[string]uint64{
	// use lowercase only
	"name":          200,
	"knows":         201,
	"bob_12345":     202,
	"www:attribute": 203,
	"www:name":      204,
	"www:value":     205,
	"p:a":           206,
	"p:b":           207,
	"rdf:type":      208,
	"style":         210,
	"locatedin":     100,
	"europe":        101,
	"population":    102,
	"color":         103,
	"blue":          104,
	"rome":          105,
	"london":        106,
	"from":          17,
	"postcard1":     50,
	"type":          51,
	"product":       52,
	"city":          53,
	"red":           54,
	"isconcrete":    55,
	"yes":           56,
	"letter":        57,
	"postcard":      58,
	"size":          59,
}

// mustParse parses a FactPattern query string and rewrites it using 'well
// known' data for the planner test cases.
func mustParse(input string) *parser.Query {
	query := parser.MustParse(parser.QueryFactPattern, input)
	parser.MustRewrite(context.Background(), &lookupsStub{}, 0, query)
	return query
}

// parseQuery parses a query string in the indicated format and rewrites it
// using 'well known' data for the planner test cases.
func parseQuery(t *testing.T, format, input string) *parser.Query {
	query, err := parser.Parse(format, input)
	assert.NoError(t, err)
	err = parser.Rewrite(context.Background(), &lookupsStub{}, 0, query)
	assert.NoError(t, err)
	return query
}

// lookupsStub finds subject ids that are well known (see above) and provides them to the query rewriter
type lookupsStub struct{}

func (ls *lookupsStub) LookupPO(_ context.Context, req *rpc.LookupPORequest, ch chan *rpc.LookupChunk) error {
	defer close(ch)
	emit := func(offset int, fact *rpc.Fact) {
		if fact == nil {
			ch <- new(rpc.LookupChunk)
			return
		}
		res := &rpc.LookupChunk{
			Facts: []rpc.LookupChunk_Fact{
				{
					Lookup: uint32(offset),
					Fact:   *fact,
				},
			},
		}
		ch <- res
	}
	for i, item := range req.Lookups {
		if id, ok := wellKnown[strings.ToLower(item.Object.ValString())]; ok {
			emit(i, &rpc.Fact{Id: 0, Subject: id})
		} else {
			emit(i, nil)
		}
	}
	return nil
}

// checkInvariants checks many properties over the logical and physical
// expressions in the search space. Nearly all unit tests invoke these checks
// via testSearchOpts below.
func checkInvariants(space *search.Space, root *search.Group) error {
	var checkGroup func(group *search.Group) error
	checkGroup = func(group *search.Group) error {
		// Check each group only once.
		checked := make(map[*search.Group]struct{})
		if _, ok := checked[group]; ok {
			return nil
		}
		checked[group] = struct{}{}

		// Check that logical properties are set.
		groupProp := group.LogicalProp.(*logicalProperties)
		if groupProp.variables == nil {
			return fmt.Errorf("group %v variables are unset:\n%v", group.ID, space)
		}
		if groupProp.resultSize == 0 {
			return fmt.Errorf("group %v expected result size is unset:\n%v", group.ID, space)
		}

		for _, expr := range group.Exprs {
			// Check input groups.
			for _, input := range expr.Inputs {
				err := checkGroup(input)
				if err != nil {
					return err
				}
			}
			// Check operator-specific invariants.
			switch op := expr.Operator.(type) {
			case *innerJoinOperator:
				err := checkJoinVars(space, group, expr, op.variables)
				if err != nil {
					return err
				}
			case *plandef.HashJoin:
				err := checkJoinVars(space, group, expr, op.Variables)
				if err != nil {
					return err
				}
			case *plandef.LoopJoin:
				err := checkJoinVars(space, group, expr, op.Variables)
				if err != nil {
					return err
				}
			case *plandef.SelectLit:
				inputVars := expr.Inputs[0].LogicalProp.(*logicalProperties).variables
				if !inputVars.Equal(groupProp.variables) {
					return fmt.Errorf("SelectLit inputs different variables from group")
				}
			}
		}
		return nil
	}
	return checkGroup(root)
}

// Helper to checkInvariants: checks that InnerJoin variables are set properly.
func checkJoinVars(space *search.Space, group *search.Group, expr *search.Expr, actualJoinVars plandef.VarSet) error {
	groupProp := group.LogicalProp.(*logicalProperties)
	expectedJoinVars := groupProp.variables
	for _, input := range expr.Inputs {
		inputVars := input.LogicalProp.(*logicalProperties).variables
		expectedJoinVars = expectedJoinVars.Intersect(inputVars)
		for _, v := range inputVars {
			if !groupProp.variables.Contains(v) {
				return fmt.Errorf("join's group missing var %v: %v\n%v", v, expr, space)
			}
		}
		for _, v := range actualJoinVars {
			if !inputVars.Contains(v) {
				return fmt.Errorf("input missing InnerJoin variable %v: %v\n%v", v, expr, space)
			}
		}
	}
	if !actualJoinVars.Equal(expectedJoinVars) {
		return fmt.Errorf("actual InnerJoin vars %v, expected %v in %v\n%v",
			actualJoinVars, expectedJoinVars, expr, space)
	}
	if len(actualJoinVars) == 0 {
		return fmt.Errorf("no variables to InnerJoin over: %v\n%v", expr, space)
	}
	return nil
}

// Options passed down to the generic query optimizer.
var testSearchOpts = search.Options{
	CheckInvariantsAfterMajorSteps: true,
	CheckInternalInvariants:        true,
	Invariants: func(space *search.Space, root *search.Group) {
		err := checkInvariants(space, root)
		if err != nil {
			log.Panicf("Invariants check failed: %v", err)
		}
	},
}

// Checks that space.Contains(pattern). If not, returns a descriptive message.
// This should be invoked as:
//     assert.NoError(contains(space, pattern))
func contains(space *search.Space, pattern string) error {
	if space.Contains(pattern) {
		return nil
	}
	return fmt.Errorf(`space should have contained pattern:
%v
Space:
%v`, pattern, space)
}

// Checks that !space.Contains(pattern). If not, returns a descriptive message.
// This should be invoked as:
//     assert.NoError(excludes(space, pattern))
func excludes(space *search.Space, pattern string) error {
	if !space.Contains(pattern) {
		return nil
	}
	return fmt.Errorf(`space should not have contained pattern:
%v
Space:
%v`, pattern, space)
}

// runSearch executes all the steps of search.Prepare but returns the Space
// itself for further inspection. It asserts that BestPlan does not return an
// error.
func runSearch(t *testing.T, queryTree *search.IntoExpr) *search.Space {
	t.Helper()
	space := search.NewSpace(queryTree, &planner{stats: new(mockStats)}, testSearchOpts)
	space.Explore()
	space.Implement()
	space.PredictCosts()
	_, err := space.BestPlan()
	assert.NoError(t, err)
	return space
}
