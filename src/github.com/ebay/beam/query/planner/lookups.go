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
	"fmt"
	"strings"

	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/query/planner/search"
	"github.com/ebay/beam/rpc"
)

type lookupOperator struct {
	id        plandef.Term
	subject   plandef.Term
	predicate plandef.Term
	infer     bool
	object    plandef.Term
}

func (node *lookupOperator) String() string {
	prefix := "Lookup"
	if node.infer {
		prefix = "Infer"
	}
	return fmt.Sprintf("%v(%v %v %v %v)",
		prefix,
		node.id,
		node.subject,
		node.predicate,
		node.object)
}

// Key implements cmp.Key.
func (node *lookupOperator) Key(b *strings.Builder) {
	if node.infer {
		b.WriteString("Infer ")
	} else {
		b.WriteString("Lookup ")
	}
	node.id.Key(b)
	b.WriteByte(' ')
	node.subject.Key(b)
	b.WriteByte(' ')
	node.predicate.Key(b)
	b.WriteByte(' ')
	node.object.Key(b)
}

func lookupLogicalProperties(op *lookupOperator, inputs []*logicalProperties, stats Stats) *logicalProperties {
	return &logicalProperties{
		variables:  getJoinableVars(op),
		resultSize: lookupResultSize(op, stats),
	}
}

func lookupResultSize(lookup *lookupOperator, stats Stats) int {
	if fixed(lookup.id) {
		return 1
	}

	var subjectOID uint64
	if subject, ok := lookup.subject.(*plandef.OID); ok {
		subjectOID = subject.Value
	}
	var predicateOID uint64
	if predicate, ok := lookup.predicate.(*plandef.OID); ok {
		predicateOID = predicate.Value
	}
	var objectVal rpc.KGObject
	switch object := lookup.object.(type) {
	case *plandef.OID:
		objectVal = rpc.AKID(object.Value)
	case *plandef.Literal:
		objectVal = object.Value
	}

	type Mask struct{ s, p, o bool }
	mask := Mask{s: fixed(lookup.subject), p: fixed(lookup.predicate), o: fixed(lookup.object)}
	nonInference := func() int {
		switch mask {
		case (Mask{s: false, p: false, o: false}):
			return stats.NumFacts()
		case (Mask{s: false, p: false, o: true}):
			return stats.NumFactsO(objectVal)
		case (Mask{s: false, p: true, o: false}):
			return stats.NumFactsP(predicateOID)
		case (Mask{s: false, p: true, o: true}):
			return stats.NumFactsPO(predicateOID, objectVal)
		case (Mask{s: true, p: false, o: false}):
			return stats.NumFactsS(subjectOID)
		case (Mask{s: true, p: false, o: true}):
			return stats.NumFactsSO(subjectOID, objectVal)
		case (Mask{s: true, p: true, o: false}):
			return stats.NumFactsSP(subjectOID, predicateOID)
		case (Mask{s: true, p: true, o: true}):
			return 1
		default:
			panic("missing case")
		}
	}
	if lookup.infer {
		return nonInference() * 5
	}
	return nonInference()
}

func lookupSCost(expr *search.Expr, stats Stats) *estCost {
	resultSize := expr.Group.LogicalProp.(*logicalProperties).resultSize
	return &estCost{
		diskBytes: resultSize * stats.BytesPerFact(),
		diskSeeks: stats.NumSPOPartitions(),
	}
}

func lookupSPCost(expr *search.Expr, stats Stats) *estCost {
	resultSize := expr.Group.LogicalProp.(*logicalProperties).resultSize
	return &estCost{
		diskBytes: resultSize * stats.BytesPerFact(),
		diskSeeks: 1,
	}
}

func inferSPCost(expr *search.Expr, stats Stats) *estCost {
	resultSize := expr.Group.LogicalProp.(*logicalProperties).resultSize
	return &estCost{
		diskBytes: resultSize * stats.BytesPerFact(),
		diskSeeks: 5,
	}
}

func lookupSPOCost(expr *search.Expr, stats Stats) *estCost {
	return &estCost{
		diskBytes: stats.BytesPerFact(),
		diskSeeks: 1,
	}
}

func inferSPOCost(expr *search.Expr, stats Stats) *estCost {
	resultSize := expr.Group.LogicalProp.(*logicalProperties).resultSize
	return &estCost{
		diskBytes: resultSize * 5 * stats.BytesPerFact(),
		diskSeeks: 5,
	}
}

func lookupPOCost(expr *search.Expr, stats Stats) *estCost {
	resultSize := expr.Group.LogicalProp.(*logicalProperties).resultSize
	return &estCost{
		diskBytes: resultSize * stats.BytesPerFact(),
		diskSeeks: 1,
	}
}

func inferPOCost(expr *search.Expr, stats Stats) *estCost {
	resultSize := expr.Group.LogicalProp.(*logicalProperties).resultSize
	return &estCost{
		diskBytes: resultSize * stats.BytesPerFact(),
		diskSeeks: 5,
	}
}

func lookupPOCmpCost(expr *search.Expr, stats Stats) *estCost {
	resultSize := expr.Group.LogicalProp.(*logicalProperties).resultSize
	return &estCost{
		diskBytes: resultSize * stats.BytesPerFact(),
		diskSeeks: stats.NumPOSPartitions(),
	}
}

func implementLookup(root *search.Expr) []*search.IntoExpr {
	lookup, ok := root.Operator.(*lookupOperator)
	if !ok {
		return nil
	}
	impl := lookupOperatorToImpl(lookup)
	if impl == nil {
		return nil
	}
	return []*search.IntoExpr{
		search.NewExpr(impl),
	}
}

func fixed(term plandef.Term) bool {
	_, ok := term.(plandef.FixedTerm)
	return ok
}

func lookupOperatorToImpl(lookup *lookupOperator) search.Operator {
	if fixed(lookup.id) {
		return nil
	}
	type Mask struct{ inf, s, p, o bool }
	mask := Mask{
		inf: lookup.infer,
		s:   fixed(lookup.subject),
		p:   fixed(lookup.predicate),
		o:   fixed(lookup.object),
	}
	switch mask {
	case (Mask{inf: true, s: true, p: true, o: true}):
		return &plandef.InferSPO{
			ID:        lookup.id.(plandef.FreeTerm),
			Subject:   lookup.subject.(plandef.FixedTerm),
			Predicate: lookup.predicate.(plandef.FixedTerm),
			Object:    lookup.object.(plandef.FixedTerm),
		}
	case (Mask{inf: true, s: true, p: true, o: false}):
		return &plandef.InferSP{
			ID:        lookup.id.(plandef.FreeTerm),
			Subject:   lookup.subject.(plandef.FixedTerm),
			Predicate: lookup.predicate.(plandef.FixedTerm),
			Object:    lookup.object.(plandef.FreeTerm),
		}
	case (Mask{inf: true, s: false, p: true, o: true}):
		return &plandef.InferPO{
			ID:        lookup.id.(plandef.FreeTerm),
			Subject:   lookup.subject.(plandef.FreeTerm),
			Predicate: lookup.predicate.(plandef.FixedTerm),
			Object:    lookup.object.(plandef.FixedTerm),
		}
	case (Mask{inf: false, s: true, p: true, o: true}):
		return &plandef.LookupSPO{
			ID:        lookup.id.(plandef.FreeTerm),
			Subject:   lookup.subject.(plandef.FixedTerm),
			Predicate: lookup.predicate.(plandef.FixedTerm),
			Object:    lookup.object.(plandef.FixedTerm),
		}
	case (Mask{inf: false, s: true, p: true, o: false}):
		return &plandef.LookupSP{
			ID:        lookup.id.(plandef.FreeTerm),
			Subject:   lookup.subject.(plandef.FixedTerm),
			Predicate: lookup.predicate.(plandef.FixedTerm),
			Object:    lookup.object.(plandef.FreeTerm),
		}
	// TODO: this is a temporary hack to demote InferS to LookupS so that we can test LookupS.
	case (Mask{inf: true, s: true, p: false, o: false}):
		fallthrough
	case (Mask{inf: false, s: true, p: false, o: false}):
		return &plandef.LookupS{
			ID:        lookup.id.(plandef.FreeTerm),
			Subject:   lookup.subject.(plandef.FixedTerm),
			Predicate: lookup.predicate.(plandef.FreeTerm),
			Object:    lookup.object.(plandef.FreeTerm),
		}
	case (Mask{inf: false, s: false, p: true, o: true}):
		return &plandef.LookupPO{
			ID:        lookup.id.(plandef.FreeTerm),
			Subject:   lookup.subject.(plandef.FreeTerm),
			Predicate: lookup.predicate.(plandef.FixedTerm),
			Object:    lookup.object.(plandef.FixedTerm),
		}
	default:
		return nil
	}
}

func implementLookupPOCmp(root *search.Expr) []*search.IntoExpr {
	selection, ok := root.Operator.(*plandef.SelectLit)
	if !ok {
		return nil
	}
	if len(selection.Clauses) > 1 {
		return nil
	}
	if !poCompable(selection.Clauses[0].Comparison) {
		return nil
	}
	var ret []*search.IntoExpr
	for _, expr := range root.Inputs[0].Exprs {
		lookup, ok := expr.Operator.(*lookupOperator)
		if !ok || lookup.infer {
			continue
		}
		if fixed(lookup.id) || fixed(lookup.subject) {
			continue
		}
		if fixed(lookup.predicate) && selection.Test == lookup.object {
			impl := search.NewExpr(
				&plandef.LookupPOCmp{
					ID:        lookup.id.(plandef.FreeTerm),
					Subject:   lookup.subject.(plandef.FreeTerm),
					Predicate: lookup.predicate.(plandef.FixedTerm),
					Object:    selection.Test,
					Cmp:       selection.Clauses[0]},
			)
			ret = append(ret, impl)
		}
	}
	return ret
}

// poCompable returns true if the supplied comparison operator is supported
// by LookupPOCmp
func poCompable(o rpc.Operator) bool {
	switch o {
	case rpc.OpEqual:
		return true
	case rpc.OpLess:
		return true
	case rpc.OpLessOrEqual:
		return true
	case rpc.OpGreater:
		return true
	case rpc.OpGreaterOrEqual:
		return true
	case rpc.OpRangeIncExc:
		return true
	case rpc.OpRangeIncInc:
		return true
	case rpc.OpRangeExcInc:
		return true
	case rpc.OpRangeExcExc:
		return true
	case rpc.OpPrefix:
		return true
	default:
		return false
	}
}
