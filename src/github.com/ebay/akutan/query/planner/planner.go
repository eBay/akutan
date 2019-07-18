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

// Package planner is the KG/Akutan-specific query optimizer. It defines the
// logical and physical transformation rules for KG/Akutan's logical and physical
// operators, and it leverages the search subpackage to find the best
// implementation plan. This package also implements KG/Akutan-specific cost
// predictions and defines the logical operators (physical operators are defined
// in the plan subpackage).
package planner

import (
	"context"
	"fmt"

	"github.com/ebay/akutan/query/parser"
	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/query/planner/search"
	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
)

type planner struct {
	stats Stats
}

var _ search.Definition = (*planner)(nil)

var explorationRules = []search.ExplorationRule{
	{Name: "PushDownSelectLit", Apply: pushDownSelectLit},
	{Name: "PushDownSelectVar", Apply: pushDownSelectVar},
	{Name: "FlipJoin", Apply: flipJoin},
	{Name: "ReorderJoin", Apply: reorderJoin},
	// LoopJoin is an exploration rule since it generates a logical operator
	{Name: "LoopJoin", Apply: implementLoopJoin},
	{Name: "LeftLoopJoin", Apply: implementLeftLoopJoin},
}

var implementationRules = []search.ImplementationRule{
	{Name: "Lookup", Apply: implementLookup},
	{Name: "LookupPOCmp", Apply: implementLookupPOCmp},
	{Name: "HashJoin", Apply: implementHashJoin},
	{Name: "LeftHashJoin", Apply: implementLeftHashJoin},
}

func (*planner) ExplorationRules() []search.ExplorationRule {
	return explorationRules
}

func (*planner) ImplementationRules() []search.ImplementationRule {
	return implementationRules
}

func (planner *planner) LogicalProperties(op search.Operator, inputs []search.LogicalProperties) search.LogicalProperties {
	in := make([]*logicalProperties, len(inputs))
	for i := range inputs {
		in[i] = inputs[i].(*logicalProperties)
	}
	var lprop *logicalProperties
	switch op := op.(type) {
	case *lookupOperator:
		lprop = lookupLogicalProperties(op, in, planner.stats)
	case *innerJoinOperator:
		lprop = joinLogicalProperties(in, planner.stats)
	case *leftJoinOperator:
		lprop = leftJoinLogicalProperties(in, planner.stats)
	case *plandef.LoopJoin:
		lprop = loopJoinLogicalProperties(op, in, planner.stats)
	case *plandef.SelectLit:
		lprop = selectLitLogicalProperties(op, in, planner.stats)
	case *plandef.SelectVar:
		lprop = selectVarLogicalProperties(op, in, planner.stats)
	case *plandef.Enumerate:
		lprop = enumerateLogicalProperties(op, in, planner.stats)
	case *plandef.ExternalIDs:
		lprop = externalIdsLogicalProperties(op, in, planner.stats)
	case *plandef.Ask:
		lprop = askLogicalProperties(op)
	case *plandef.Projection:
		lprop = projectionLogicalProperties(op, in, planner.stats)
	case *plandef.OrderByOp:
		lprop = orderByLogicalProperties(op, in, planner.stats)
	case *plandef.LimitAndOffsetOp:
		lprop = limitAndOffsetLogicalProperties(op, in, planner.stats)
	case *plandef.DistinctOp:
		lprop = distinctLogicalProperties(op, in, planner.stats)
	default:
		log.Panicf("LogicalProperties not implemented for type %T. It should "+
			"be implemented for every logical operator and should not be "+
			"called for physical-only operators.", op)
	}
	return lprop
}

type logicalProperties struct {
	variables  plandef.VarSet
	resultSize int
}

func (lprop *logicalProperties) String() string {
	return fmt.Sprintf("vars: %v",
		lprop.variables)
}

func (lprop *logicalProperties) DetailString() string {
	size := "∞"
	if lprop.resultSize <= 1<<30 {
		size = fmt.Sprintf("%v", lprop.resultSize)
	}
	return fmt.Sprintf("vars: %v size: %s",
		lprop.variables, size)
}

func (*planner) MakePlanNode(op search.Operator, inputs []search.PlanNode, lprop search.LogicalProperties) search.PlanNode {
	in := make([]*plandef.Plan, len(inputs))
	for i := range inputs {
		in[i] = inputs[i].(*plandef.Plan)
	}
	return &plandef.Plan{
		Operator:  op.(plandef.Operator),
		Inputs:    in,
		Variables: lprop.(*logicalProperties).variables,
	}
}

// Parse is a lower-level interface for more control. It initializes a search
// space with the input query and returns the search space, which can be used to
// find an implementation plan. It returns an error if the query is malformed.
func Parse(query *parser.Query, stats Stats, searchOpts search.Options) (*search.Space, error) {
	queryTree, err := createLogicalTree(query)
	if err != nil {
		return nil, err
	}
	memo := search.NewSpace(queryTree, &planner{stats: stats}, searchOpts)
	return memo, nil
}

// Prepare finds an implementation plan for the given query. It returns an error
// if the query is malformed or cannot be implemented. It returns both the
// search space that the plan was selected from, and the selected plan.
func Prepare(ctx context.Context, query *parser.Query, stats Stats, searchOpts search.Options) (*search.Space, *plandef.Plan, error) {
	if stats == nil {
		stats = new(mockStats)
	}
	span, _ := opentracing.StartSpanFromContext(ctx, "Prepare.CreateLogicalTree")
	queryTree, err := createLogicalTree(query)
	span.Finish()
	if err != nil {
		return nil, nil, err
	}
	space, aplan, err := search.Prepare(ctx, queryTree, &planner{stats: stats}, searchOpts)
	if err != nil {
		return space, nil, err
	}
	return space, aplan.(*plandef.Plan), nil
}
