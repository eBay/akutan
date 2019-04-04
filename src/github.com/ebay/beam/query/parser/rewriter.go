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

package parser

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/ebay/beam/msg/facts"
	"github.com/ebay/beam/rpc"
	beamerrors "github.com/ebay/beam/util/errors"
	"github.com/ebay/beam/util/parallel"
	"github.com/ebay/beam/viewclient/lookups"
	opentracing "github.com/opentracing/opentracing-go"
)

// MustRewrite uses a set of rewrite rules in order to rewrite the query definition and panics if
// an error occurs.  It simplifies variable initialization.
func MustRewrite(ctx context.Context, lookup lookups.PO, idx uint64, in *Query) {
	err := Rewrite(ctx, lookup, idx, in)
	if err != nil {
		panic(fmt.Sprintf("unable to rewrite query: %v: %v", in, err))
	}
}

// Rewrite uses a set of rewrite rules in order to rewrite the query definition.  Currently we
// replace external ids with object ids with a lookup po request.
func Rewrite(ctx context.Context, lookup lookups.PO, idx uint64, in *Query) error {
	rw := rewriter{
		lookup: lookup,
		idx:    idx,
		in:     in,
	}
	return rw.rewrite(ctx)
}

// A rewriteRule is a named function used to update a query in place.
type rewriteRule struct {
	name    string
	rewrite func(context.Context, lookups.PO, uint64, *Query) error
}

// A rewriter provides a named type for in-place updates of a query tree.
type rewriter struct {
	in *Query

	lookup lookups.PO
	idx    uint64
}

// rewriteRules includes both semantic check rules as well as normalization rules
// that are applied to parsed queries.
var rewriteRules = []rewriteRule{
	{"Variable Scope Checks", variableScopeChecks},
	{"Aggregate Restrictions Check", aggregateRestrictionsCheck},
	{"comparison Operator types check", comparisonTypesCheck},
	{"Lookup External IDs", lookupExternalIDs},
	{"Normalize to LiteralIDs", convertToLiteralIDs},
	{"Literal Set Semantic Checks", literalSetSemantics},
}

// rewrite the query, updating the ast in place.
func (rw *rewriter) rewrite(ctx context.Context) error {
	for _, rule := range rewriteRules {
		span, ctx := opentracing.StartSpanFromContext(ctx, rule.name)
		err := rule.rewrite(ctx, rw.lookup, rw.idx, rw.in)
		if err != nil {
			span.SetTag("error", err.Error())
			span.Finish()
			return err
		}
		span.Finish()
	}
	return nil
}

// literalSetSemantics looks through the query for literal sets, and ensures:
//  - They appear after the <in> operator only.
//  - The line is a triple and not a quad.
//  - The set is not empty.
//  - All set element values are not variables.
//  - All set element values are of equal type.
func literalSetSemantics(_ context.Context, _ lookups.PO, _ uint64, query *Query) error {
	for _, line := range query.Where {
		if literalSet, ok := line.Object.(*LiteralSet); ok {
			op, ok := line.Predicate.(*Operator)
			if !ok || op.Value != rpc.OpIn {
				return fmt.Errorf("literal sets must use operator '%s' (found '%s')",
					rpc.OpIn, line.Predicate)
			}

			if _, ok := line.ID.(*Nil); !ok {
				return errors.New("literal sets may only be used on lines without fact IDs")
			}

			if len(literalSet.Values) == 0 {
				return errors.New("literal sets must not be empty")
			}

			for i := 1; i < len(literalSet.Values); i++ {
				if !literalSet.Values[0].equalType(literalSet.Values[i]) {
					return fmt.Errorf("literal set values must be of the same type, '%s' and '%s' differ",
						literalSet.Values[0],
						literalSet.Values[i])
				}
			}
		}
	}
	return nil
}

// lookupExternalIDs will find nodes in the query typed eid; lookup the external and replace (set) the node
// with the found id.
func lookupExternalIDs(ctx context.Context, lookup lookups.PO, idx uint64, in *Query) error {
	req := rpc.LookupPORequest{
		Index:   idx,
		Lookups: []rpc.LookupPORequest_Item{},
	}

	replace := make(map[string][]resolveItem) // All the items that need resolving, keyed by string value.
	count := 0
	for _, line := range in.Where {
		for _, r := range line.toResolve() {
			count++
			replacements, exists := replace[r.StringID]
			replace[r.StringID] = append(replacements, r)
			if !exists {
				req.Lookups = append(req.Lookups, rpc.LookupPORequest_Item{
					Predicate: facts.HasExternalID,
					Object:    rpc.AString(r.StringID, 0),
				})
			}
		}
	}

	span := opentracing.SpanFromContext(ctx)
	span.SetTag("entity count", count)
	lookupResCh := make(chan *rpc.LookupChunk, 4)
	lookupWait := parallel.GoCaptureError(func() error {
		return lookup.LookupPO(ctx, &req, lookupResCh)
	})
	for chunk := range lookupResCh {
		for _, fact := range chunk.Facts {
			stringID := req.Lookups[fact.Lookup].Object.ValString()
			for _, item := range replace[stringID] {
				*item.KID = fact.Fact.Subject
			}
			delete(replace, stringID)
		}
	}
	var err error
	if len(replace) > 0 {
		if len(replace) == 1 {
			for k := range replace {
				err = fmt.Errorf("invalid query: entity '%s' does not exist", k)
			}
		} else {
			s := strings.Builder{}
			entities := make([]string, 0, len(replace))
			for k := range replace {
				entities = append(entities, k)
			}
			sort.Strings(entities)
			for _, k := range entities {
				if s.Len() > 0 {
					s.WriteString(", ")
				}
				s.WriteByte('\'')
				s.WriteString(k)
				s.WriteByte('\'')
			}
			err = fmt.Errorf("invalid query: entities %s do not exist", s.String())
		}
	}
	return beamerrors.Any(lookupWait(), err)
}

// convertToLiteralIDs will go through and convert any QName or Entity instances
// into literalIDs. This should come after the lookupExternalIDs rewriter.
func convertToLiteralIDs(_ context.Context, _ lookups.PO, _ uint64, in *Query) error {
	for _, quad := range in.Where {
		quad.ID = convertToLiteralID(quad.ID)
		quad.Subject = convertToLiteralID(quad.Subject)
		quad.Predicate = convertToLiteralID(quad.Predicate)
		quad.Object = convertToLiteralID(quad.Object)
	}
	return nil
}

// convertToLiteralID will convert any QName or Entity instance into LiteralID.
// Returns the possibly updated Term.
func convertToLiteralID(term Term) Term {
	switch term := term.(type) {
	case *QName:
		return &LiteralID{Value: term.ID, Hint: term.String()}
	case *Entity:
		return &LiteralID{Value: term.ID, Hint: term.String()}
	case *LiteralSet:
		for i, v := range term.Values {
			term.Values[i] = convertToLiteralID(v)
		}
	}
	return term
}

// variableScopeChecks validates that the variables between the select, where &
// orderby sections are in scope when used.
func variableScopeChecks(_ context.Context, _ lookups.PO, _ uint64, in *Query) error {
	valid := make(map[string]bool)
	addIfVar := func(maybeVar interface{}) {
		if vr, isVar := maybeVar.(*Variable); isVar {
			valid[vr.Name] = true
		}
	}
	// all variables used in the where clause are valid for use anywhere else.
	for _, quad := range in.Where {
		addIfVar(quad.ID)
		addIfVar(quad.Subject)
		addIfVar(quad.Predicate)
		addIfVar(quad.Object)
	}
	// all variables used in the order by clause need to be in the where clause
	for _, order := range in.Modifiers.OrderBy {
		if !valid[order.On.Name] {
			return fmt.Errorf("invalid query: ORDER BY of %s but %s not in scope", order.On, order.On)
		}
	}
	// variables used in the select clause need to be in the where clause, or the
	// result of a expression binding earlier in the select list. For a binding the
	// variable its bound to can't already be in use.
	for _, sc := range in.Select.Items {
		switch t := sc.(type) {
		case *Variable:
			if !valid[t.Name] {
				return fmt.Errorf("invalid query: SELECT of %s, but %s not in scope", t, t)
			}
		case *BoundExpression:
			if valid[t.As.Name] {
				return fmt.Errorf("invalid query: %v binds to %v which is already used", t, t.As)
			}
			valid[t.As.Name] = true
		}
	}
	return nil
}

// aggregateRestrictionsCheck validates that aggregates expressions are not
// mixed with non-aggregate expressions or variables.
func aggregateRestrictionsCheck(_ context.Context, _ lookups.PO, _ uint64, in *Query) error {
	isAggExpr := func(s selectClauseItem) bool {
		switch e := s.(type) {
		case *BoundExpression:
			_, isAgg := e.Expr.(*AggregateExpr)
			return isAgg
		case *Variable:
			return false
		case Wildcard:
			return false
		default:
			panic(fmt.Sprintf("unexpected selectClauseItem type %T %#v", s, s))
		}
	}
	if len(in.Select.Items) < 2 {
		return nil
	}
	isAgg := isAggExpr(in.Select.Items[0])
	for _, s := range in.Select.Items {
		if isAgg != isAggExpr(s) {
			return fmt.Errorf("invalid query: can't mix aggregate and non-aggregate expressions. have %s and %s", in.Select.Items[0], s)
		}
	}
	return nil
}

// comparisonTypesCheck returns an error if it finds any invalid comparison lines.
// 1) If the Object is an entity the comparison must be == or !=
// 2) If Subject & Object are both variables, the comparison must be == or !=
// 3) The Subject must be a variable.
func comparisonTypesCheck(_ context.Context, _ lookups.PO, _ uint64, in *Query) error {
	for _, q := range in.Where {
		if op, isOp := q.Predicate.(*Operator); isOp {
			if _, isVar := q.Subject.(*Variable); !isVar {
				return errors.New("invalid query: comparison operators require a variable on the left side")
			}
			if op.Value == rpc.OpEqual || op.Value == rpc.OpNotEqual {
				continue
			}
			switch q.Object.(type) {
			case *Variable:
				return errors.New("invalid query: only <eq> and <notEqual> are allowed comparisons when the right side is a variable")
			case *Entity:
			case *QName:
			case *LiteralID:
			default:
				continue
			}
			return errors.New("invalid query: only <eq> and <notEqual> are allowed comparisons when the right side is an entity")
		}
	}
	return nil
}
