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

	"github.com/ebay/beam/query/parser"
	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/query/planner/search"
	"github.com/ebay/beam/rpc"
	log "github.com/sirupsen/logrus"
)

// createLogicalTree extracts a query into a logical expression tree.
func createLogicalTree(query *parser.Query) (*search.IntoExpr, error) {
	vars := new(variableScope)
	ops, err := extractOps(query, vars)
	if err != nil {
		return nil, err
	}
	for _, selection := range ops.selectionByVar {
		selection.withLiteral = squashSelectLit(selection.withLiteral)
	}
	removeInference(ops)
	root, err := buildJoinTree(ops)
	if err != nil {
		return nil, err
	}
	// this is a temporarily work around, see the comment on the function for
	// more details.
	root = preImplementLeftLoopJoin(root)

	// Add all the selections at the root of the tree.
	for _, selection := range ops.selectionByVar {
		for _, lit := range selection.withLiteral {
			root = search.NewExpr(lit, root)
		}
		for _, vars := range selection.withVariable {
			root = search.NewExpr(vars, root)
		}
	}
	switch query.Type {
	case parser.LegacyPatternQuery:
		return root, nil
	case parser.SelectQuery:
		return buildSelectQueryOps(query, root, vars)
	case parser.AskQuery:
		askOp := &plandef.Ask{Out: vars.named("result")}
		return search.NewExpr(askOp, root), nil
	default:
		panic(fmt.Sprintf("Unexpected queryType of %s", query.Type))
	}
}

// buildSelectQueryOps builds select query operator as per the Sparql solution
// sequences order. See https://www.w3.org/TR/sparql11-query/#solutionModifiers
func buildSelectQueryOps(query *parser.Query, root *search.IntoExpr, vars *variableScope) (*search.IntoExpr, error) {
	resolvedExternalIDs := false
	if len(query.Modifiers.OrderBy) > 0 {
		orderByOp := &plandef.OrderByOp{
			OrderBy: make([]plandef.OrderCondition, 0, len(query.Modifiers.OrderBy)),
		}
		for _, cond := range query.Modifiers.OrderBy {
			orderByOp.OrderBy = append(orderByOp.OrderBy, plandef.OrderCondition{
				On:        vars.named(cond.On.Name),
				Direction: cond.Direction,
			})
		}
		root = search.NewExpr(orderByOp,
			search.NewExpr(&plandef.ExternalIDs{}, root))
		resolvedExternalIDs = true
	}

	isWildcard := false
	if len(query.Select.Items) == 1 {
		_, isWildcard = query.Select.Items[0].(parser.Wildcard)
	}
	if !isWildcard {
		projectVars := make(map[string]*plandef.Variable)
		projectOp := &plandef.Projection{
			Select: make([]plandef.ExprBinding, 0, len(query.Select.Items)),
		}
		for _, item := range query.Select.Items {
			switch s := item.(type) {
			case *parser.Variable:
				v := vars.named(s.Name)
				projectVars[v.Name] = v
				bind := plandef.ExprBinding{Expr: v, Out: v}
				projectOp.Select = append(projectOp.Select, bind)
			case *parser.BoundExpression:
				v := vars.named(s.As.Name)
				projectVars[v.Name] = v
				bind := plandef.ExprBinding{Expr: makeExpr(s.Expr, vars), Out: v}
				projectOp.Select = append(projectOp.Select, bind)
			default:
				panic(fmt.Sprintf("Unexpected parser.SelectClauseItem type %T %v", s, s))
			}
		}
		projectOp.Variables = plandef.NewVarSet(projectVars)
		root = search.NewExpr(projectOp, root)
	}

	if _, ok := query.Select.Keyword.(parser.Distinct); ok {
		root = search.NewExpr(&plandef.DistinctOp{}, root)
	}

	if query.Modifiers.Paging.Limit != nil || query.Modifiers.Paging.Offset != nil {
		limitAndOffsetOp := &plandef.LimitAndOffsetOp{
			Paging: plandef.LimitOffset{
				Limit:  query.Modifiers.Paging.Limit,
				Offset: query.Modifiers.Paging.Offset,
			},
		}
		root = search.NewExpr(limitAndOffsetOp, root)
	}

	if !resolvedExternalIDs {
		root = search.NewExpr(&plandef.ExternalIDs{}, root)
	}

	return root, nil
}

func makeExpr(expr parser.Expression, vars *variableScope) plandef.Expression {
	switch expr := expr.(type) {
	case *parser.AggregateExpr:
		aggr := plandef.AggregateExpr{Func: expr.Function}
		switch of := expr.Of.(type) {
		case parser.Wildcard:
			aggr.Of = plandef.WildcardExpr{}
		case *parser.Variable:
			aggr.Of = vars.named(of.Name)
		default:
			panic(fmt.Sprintf("Unexpected type for parser.AggregateTarget %T %v", of, of))
		}
		return &aggr
	default:
		panic(fmt.Sprintf("Unexpected type for parser.Expression %T %v", expr, expr))
	}
}

// Represents either a lookup/inference or a selection filter. See extractTerms.
type termLine struct {
	id        plandef.Term
	subject   plandef.Term
	predicate plandef.Term
	object    plandef.Term
}

func (line *termLine) String() string {
	return fmt.Sprintf("%v %v %v %v",
		line.id, line.subject, line.predicate, line.object)
}

// extractTerms convert a single query line (quad) into a term line
func extractTerms(line *parser.Quad, vars *variableScope) (*termLine, error) {
	kidTerm := func(fieldName string, n interface{}) (plandef.Term, error) {
		switch val := n.(type) {
		case *parser.LiteralID:
			return &plandef.OID{Value: val.Value, Hint: val.Hint}, nil
		case *parser.Variable:
			return vars.named(val.Name), nil
		case *parser.Nil:
			return new(plandef.DontCare), nil
		default:
			return nil, fmt.Errorf("unexpected term type for %s: %#v", fieldName, n)
		}
	}
	idTerm, err := kidTerm("ID", line.ID)
	if err != nil {
		return nil, err
	}
	subjectTerm, err := kidTerm("Subject", line.Subject)
	if err != nil {
		return nil, err
	}
	predicateTerm, err := kidTerm("Predicate", line.Predicate)
	if err != nil {
		return nil, err
	}

	var objectTerm plandef.Term
	switch object := line.Object.(type) {
	case *parser.Variable:
		objectTerm = vars.named(object.Name)
	case *parser.LiteralID:
		objectTerm = &plandef.OID{Value: object.Value, Hint: object.Hint}
	case parser.Literal:
		objectTerm = &plandef.Literal{Value: rpc.KGObjectFromAPI(*object.Literal())}
	case *parser.Nil:
		objectTerm = new(plandef.DontCare)
	default:
		return nil, fmt.Errorf("unexpected term type for Object: %#v", object)
	}

	return &termLine{
		id:        idTerm,
		subject:   subjectTerm,
		predicate: predicateTerm,
		object:    objectTerm,
	}, nil
}

// opSet represents an input query after the different operations are identified
// but before it's formed into a logical query tree.
type opSet struct {
	// If the variable is filtered at least once, this will map from the
	// variable's name to its selection operators. Note that a single variable
	// can contain multiple selection operators. If a selection operator has
	// multiple variables, it uses the left side variable as the map key.
	selectionByVar map[string]*selections
	// A list of leaf operators, such as lookup, inference or enumerations, in
	// no particular order
	leaves []search.Operator
	// specificityForLookup tracks how the lookup needs to be joined to the rest of the query.
	// There's one entry for each lookup taken from the Specificity of the query line that
	// the lookup was constructed from.
	specificityForLookup map[*lookupOperator]parser.MatchSpecificity
}

// selections contains all the filters in a query that have a particular
// variable on the left side. It's used as the value of the opSet.selectionByVar
// map.
type selections struct {
	// If there are multiple literal comparisons for a single variable, then
	// each one will be in its own SelectLit with a single SelectClause.
	// createLogicalTree will squash these down to a single SelectLit before
	// returning the final initial expression tree.
	withLiteral []*plandef.SelectLit
	// Unlike SelectLit, SelectVar can only store a single comparison. If there
	// are multiple variable comparisons, there will be a SelectVar instance for
	// each one.
	withVariable []*plandef.SelectVar
}

func (s *selections) String() string {
	items := make([]string, 0, len(s.withLiteral)+len(s.withVariable))
	for _, l := range s.withLiteral {
		items = append(items, l.String())
	}
	for _, v := range s.withVariable {
		items = append(items, v.String())
	}
	return "[" + strings.Join(items, ", ") + "]"
}

// specificityOf will return the MatchSpecificy associated with the supplied
// operator. For lookups this comes from the query line it was constructed from,
// for other operators it returns MatchRequired.
func (opset *opSet) specificityOf(operator search.Operator) parser.MatchSpecificity {
	switch op := operator.(type) {
	case *lookupOperator:
		return opset.specificityForLookup[op]
	default:
		return parser.MatchRequired
	}
}

// requiredAndOptionalLeaves returns 2 slices, one contains all the required
// leaf operators and the second contains all the optional match leaves. The
// returned slices are safe to be subsequently mutated by caller.
func (opset *opSet) requiredAndOptionalLeaves() (required []search.Operator, optional []search.Operator) {
	for _, op := range opset.leaves {
		s := opset.specificityOf(op)
		switch s {
		case parser.MatchRequired:
			required = append(required, op)
		case parser.MatchOptional:
			optional = append(optional, op)
		default:
			panic(fmt.Sprintf("requiredAndOptionalLeaves got unexpected Specificity: %v", s))
		}
	}
	return required, optional
}

// matchType specifies a particular beamql type, used with quadTypesMatch.
type matchType int

const (
	matchAny matchType = iota
	matchLiteral
	matchLiteralSet
	matchNil
	matchOperator
	matchVariable
)

// quadTypesMatch will compare the types in the quad fields against the specified types.
// Returns true if they all match, false otherwise.
func quadTypesMatch(quad *parser.Quad, id, subject, predicate, object matchType) bool {
	fieldMatch := func(f interface{}, match matchType) bool {
		if match == matchAny {
			return true
		}
		switch ft := f.(type) {
		case parser.Literal:
			return match == matchLiteral
		case *parser.LiteralSet:
			return match == matchLiteralSet
		case *parser.Nil:
			return match == matchNil
		case *parser.Operator:
			return match == matchOperator
		case *parser.Variable:
			return match == matchVariable
		default:
			panic(fmt.Sprintf("fieldMatch saw unexpected fieldType %T %v", ft, ft))
		}

	}
	return fieldMatch(quad.Subject, subject) &&
		fieldMatch(quad.Predicate, predicate) &&
		fieldMatch(quad.Object, object) &&
		fieldMatch(quad.ID, id)
}

// extractOps makes some sense of the query lines, splitting them into
// selections and leaves (which are made up of enumerations and lookups).
func extractOps(query *parser.Query, vars *variableScope) (*opSet, error) {
	ops := &opSet{
		selectionByVar:       make(map[string]*selections),
		leaves:               make([]search.Operator, 0, len(query.Where)),
		specificityForLookup: make(map[*lookupOperator]parser.MatchSpecificity, len(query.Where)),
	}

	for _, line := range query.Where {

		// Capture selection with literals lines into ops.selectionByVar.
		if quadTypesMatch(line, matchAny, matchVariable, matchOperator, matchLiteral) {
			subjectNode := line.Subject.(*parser.Variable)
			predicateNode := line.Predicate.(*parser.Operator)
			objectNode := line.Object.(parser.Literal)
			selectionsForVar, found := ops.selectionByVar[subjectNode.Name]
			if !found {
				selectionsForVar = &selections{}
				ops.selectionByVar[subjectNode.Name] = selectionsForVar
			}
			selection := &plandef.SelectLit{
				Test: vars.named(subjectNode.Name),
				Clauses: []plandef.SelectClause{{
					Comparison: predicateNode.Value,
					Literal1:   &plandef.Literal{Value: rpc.KGObjectFromAPI(*objectNode.Literal())},
				}},
			}
			selectionsForVar.withLiteral = append(selectionsForVar.withLiteral, selection)
			continue
		}
		// Capture selection with variables on both sides lines into ops.selectionByVar.
		if quadTypesMatch(line, matchAny, matchVariable, matchOperator, matchVariable) {
			predicateNode := line.Predicate.(*parser.Operator)
			if predicateNode.Value == rpc.OpEqual || predicateNode.Value == rpc.OpNotEqual {
				subjectNode := line.Subject.(*parser.Variable)
				objectNode := line.Object.(*parser.Variable)
				selectionsForVar, exists := ops.selectionByVar[subjectNode.Name]
				if !exists {
					selectionsForVar = &selections{}
					ops.selectionByVar[subjectNode.Name] = selectionsForVar
				}
				selection := &plandef.SelectVar{
					Left:     vars.named(subjectNode.Name),
					Operator: predicateNode.Value,
					Right:    vars.named(objectNode.Name),
				}
				selectionsForVar.withVariable = append(selectionsForVar.withVariable, selection)
				continue
			}
		}
		// Capture enumeration lines into ops.leaves.
		if quadTypesMatch(line, matchAny, matchVariable, matchOperator, matchLiteralSet) {
			predicateNode := line.Predicate.(*parser.Operator)
			if predicateNode.Value == rpc.OpIn {
				subjectNode := line.Subject.(*parser.Variable)
				objectNode := line.Object.(*parser.LiteralSet)
				op := &plandef.Enumerate{
					Output: vars.named(subjectNode.Name),
				}
				for _, value := range objectNode.Values {
					switch value := value.(type) {
					case *parser.LiteralID:
						op.Values = append(op.Values,
							&plandef.OID{Value: value.Value, Hint: value.Hint})
					case parser.Literal:
						op.Values = append(op.Values,
							&plandef.Literal{Value: rpc.KGObjectFromAPI(*value.Literal())})
					default:
						return nil, fmt.Errorf("encountered unexpected type %T (%#v) in set literal",
							value, value)
					}
				}
				ops.leaves = append(ops.leaves, op)
				continue
			}
		}

		// Capture remaining lookup/inference lines into ops.leaves.
		terms, err := extractTerms(line, vars)
		if err != nil {
			return nil, err
		}
		lookup := &lookupOperator{
			id:        terms.id,
			subject:   terms.subject,
			predicate: terms.predicate,
			infer:     true,
			object:    terms.object,
		}
		ops.specificityForLookup[lookup] = line.Specificity
		ops.leaves = append(ops.leaves, lookup)
	}
	return ops, nil
}

// squashSelectLit attempts to combine one or more SelectLits into fewer. For
// example, it might combine "> 100" and "< 200" into "in (100, 200)". (We think
// this should be eventually deferred to the plan implementation phase)
func squashSelectLit(selectLits []*plandef.SelectLit) []*plandef.SelectLit {
	// This combines the SelectLits into one, then tries to squash pairs
	// of comparisons into range clauses.
	if len(selectLits) < 2 {
		return selectLits
	}
	clauses := make([]plandef.SelectClause, 0, len(selectLits))
	for _, sel := range selectLits {
		clauses = append(clauses, sel.Clauses...)
	}
	for len(clauses) >= 2 {
		merged, ok := squashSelectPair(clauses[0], clauses[1])
		if !ok {
			break
		}
		clauses[0] = merged
		copy(clauses[1:], clauses[2:])
		clauses = clauses[:len(clauses)-1]
	}
	selectLits[0].Clauses = clauses
	return selectLits[:1]
}

// squashSelectPair is a helper to squashSelect. It attempts to combine two
// SelectClauses into one. The boolean returned is true when successful, false
// otherwise.
func squashSelectPair(left, right plandef.SelectClause) (plandef.SelectClause, bool) {
	type CompPair struct{ l, r rpc.Operator }
	P := func(l, r rpc.Operator) CompPair {
		if l > r {
			panic("Comparison pair declared backwards")
		}
		return CompPair{l: l, r: r}
	}
	if left.Comparison > right.Comparison {
		left, right = right, left
	}
	switch P(left.Comparison, right.Comparison) {
	case P(rpc.OpLess, rpc.OpGreater):
		return plandef.SelectClause{
			Comparison: rpc.OpRangeExcExc,
			Literal1:   right.Literal1,
			Literal2:   left.Literal1,
		}, true
	case P(rpc.OpLess, rpc.OpGreaterOrEqual):
		return plandef.SelectClause{
			Comparison: rpc.OpRangeIncExc,
			Literal1:   right.Literal1,
			Literal2:   left.Literal1,
		}, true
	case P(rpc.OpLessOrEqual, rpc.OpGreater):
		return plandef.SelectClause{
			Comparison: rpc.OpRangeExcInc,
			Literal1:   right.Literal1,
			Literal2:   left.Literal1,
		}, true
	case P(rpc.OpLessOrEqual, rpc.OpGreaterOrEqual):
		return plandef.SelectClause{
			Comparison: rpc.OpRangeIncInc,
			Literal1:   right.Literal1,
			Literal2:   left.Literal1,
		}, true
	// TODO: There are plenty of other pairs to implement.
	default:
		return plandef.SelectClause{}, false
	}
}

// removeInference implements an early optimization to avoid transitive lookups.
// If it's able, it sets infer to false on some of the Lookup operators.
func removeInference(ops *opSet) {
	// If we see a predicate alongside a literal or a variable used in a comparison,
	// assume the predicate is not transitive.
	nontransitivePredicates := make(map[uint64]struct{})
	// this only applies to leaves that are lookups
	lookups := make([]*lookupOperator, 0, len(ops.leaves))
	for _, leafOp := range ops.leaves {
		if lookup, isLookup := leafOp.(*lookupOperator); isLookup {
			lookups = append(lookups, lookup)
		}
	}
	for _, lookup := range lookups {
		predicate, ok := lookup.predicate.(*plandef.OID)
		if !ok {
			continue
		}
		objectIsLiteral := false
		switch object := lookup.object.(type) {
		case *plandef.Variable:
			selects, found := ops.selectionByVar[object.Name]
			objectIsLiteral = found && len(selects.withLiteral) > 0
		case *plandef.Literal:
			objectIsLiteral = true
		}
		if !objectIsLiteral {
			continue
		}
		nontransitivePredicates[predicate.Value] = struct{}{}
	}
	for _, lookup := range lookups {
		if predicate, ok := lookup.predicate.(*plandef.OID); ok {
			_, nontransitive := nontransitivePredicates[predicate.Value]
			if nontransitive {
				lookup.infer = false
			}
		}
	}
}

// subtree contains a single subtree of a larger query. These are used by
// buildJoinTree to track parts of the query tree until they are all merged into
// the final tree.
type subtree struct {
	// The root expression for this subtree.
	rootExpr *search.IntoExpr
	// joinVars are the variables that should be used to join this subtree to the
	// main tree.
	joinVars plandef.VarSet
	// availVars is the set of variables that are produced from this subtree.
	// Depending on the type of subtree this may have overlap with joinVars.
	// e.g. They do not overlap for leftJoin.
	availVars plandef.VarSet
}

// buildJoinTree builds a tree out of the lookups and enumerations. Each leaf
// node is a Lookup or Enumerate operator and each interior node is a LeftJoin
// or Join operator. The tree returned is not yet optimized in any particular
// order. Returns an error if there are no lookups or enumerations, or if the
// operations contain a Cartesian product (no variables in common).
func buildJoinTree(ops *opSet) (*search.IntoExpr, error) {
	if len(ops.leaves) == 0 {
		return nil, fmt.Errorf("query contained no lookups or enumerations")
	}
	// We need to process the optional match & required match leaves differently
	// so they are split out into 2 lists here.
	reqLeaves, optLeaves := ops.requiredAndOptionalLeaves()

	// Each optional match results in a subtree in this slice. Later on they are joined to
	// the overall join tree.
	var subtrees []subtree

	// MatchOptional for a query line `?foo <bar>? ?baz` means the edge from ?foo to ?baz
	// is optional. The join on ?foo to the rest of the query should be a left join
	// but the join on ?baz will depend on if there's an optional match for ?baz somewhere
	// else.

	// allSubtreeVars contains the union of availVars from all the subtrees.
	var allSubtreeVars plandef.VarSet

	// First we build all the subtrees that result from each optional query line.
	for _, optLeaf := range optLeaves {
		// Lookup is the only type of leaf that has an optional match, so this cast is safe.
		lookup := optLeaf.(*lookupOperator)
		subtree := subtree{
			rootExpr:  search.NewExpr(optLeaf),
			joinVars:  termsToVars(lookup.subject),
			availVars: termsToVars(lookup.id, lookup.predicate, lookup.object),
		}
		subtree, reqLeaves = buildInnerJoinTree(subtree, reqLeaves)
		subtrees = append(subtrees, subtree)

		// The query can't join the same var into different optional subtrees.
		// This make sure each sub tree uses a distinct set of variables inside
		// its subtree.
		doubleJoinedOn := allSubtreeVars.Intersect(subtree.availVars)
		if len(doubleJoinedOn) > 0 {
			return nil, fmt.Errorf("invalid query: variable(s) %v are used in 2 different optional trees", doubleJoinedOn)
		}
		allSubtreeVars = allSubtreeVars.Union(subtree.availVars)
	}

	// If reqLeaves is now empty, then there was no required match in the query, which is required.
	if len(reqLeaves) == 0 {
		return nil, fmt.Errorf("query can't consist of all optional matches")
	}
	// Now we build the tree of the remaining non-optional lines.
	requiredTree := subtree{
		rootExpr:  search.NewExpr(reqLeaves[0]),
		availVars: getJoinableVars(reqLeaves[0]),
	}
	reqLeaves = reqLeaves[1:]
	requiredTree, reqLeaves = buildInnerJoinTree(requiredTree, reqLeaves)
	if len(reqLeaves) > 0 {
		return nil, fmt.Errorf("query may not contain Cartesian product: all query lines must be connected via some join")
	}

	// Now we can join the sub trees with the primary tree.
	root, err := leftJoinSubTrees(requiredTree, subtrees)
	return root.rootExpr, err
}

// buildInnerJoinTree is used by buildJoinTree to build sub trees of the overall
// query. It will build a tree of joins starting with the supplied 'start'
// subtree and use the availVars to perform a join to leaves from
// 'requiredMatchLeaves'. It will join as many leaves as is possible from the
// supplied leaves. If the supplied start subtree was constructed from a leaf in
// 'requiredMatchLeaves' it should be removed from that slice before calling
// this. The supplied start subtree will end up being a leaf in the returned
// tree.
//
// The built inner joined subtree is returned, along with the leaves that were
// not consumed.
func buildInnerJoinTree(start subtree, requiredMatchLeaves []search.Operator) (subtree, []search.Operator) {
	joinedOne := true
	todo := requiredMatchLeaves
	root := start
	for joinedOne && len(todo) > 0 {
		var later []search.Operator
		joinedOne = false
		for _, leaf := range todo {
			leafVars := getJoinableVars(leaf)
			joinVars := root.availVars.Intersect(leafVars)
			if len(joinVars) > 0 {
				op := innerJoinOperator{variables: joinVars}
				root.rootExpr = search.NewExpr(&op, root.rootExpr, search.NewExpr(leaf))
				root.availVars = root.availVars.Union(leafVars)
				joinedOne = true
				continue
			}
			later = append(later, leaf)
		}
		todo = later
	}
	return root, todo
}

// leftJoinSubTrees will return a new tree consisting of the 'start' tree left
// joined to all the 'leftSubtrees'. An error is returned in the event that it
// can't join all the provided subtrees into the output tree.
func leftJoinSubTrees(start subtree, leftSubtrees []subtree) (subtree, error) {
	root := start
	for len(leftSubtrees) > 0 {
		joinedOne := false
		var later []subtree
		for _, leftTree := range leftSubtrees {
			if root.availVars.ContainsSet(leftTree.joinVars) {
				op := leftJoinOperator{
					variables: leftTree.joinVars,
				}
				root.rootExpr = search.NewExpr(&op, root.rootExpr, leftTree.rootExpr)
				root.availVars = root.availVars.Union(leftTree.availVars)
				joinedOne = true
			} else {
				later = append(later, leftTree)
			}
		}
		if !joinedOne {
			return subtree{}, fmt.Errorf("query may not contain Cartesian product: all query lines must be connected via some join")
		}
		leftSubtrees = later
	}
	return root, nil
}

// preImplementLeftLoopJoin is a work around to handle the lack of working push
// down binding operator. Trees with left joins are likely to end up needing to
// bind a variable from the left join multiple hops down the tree. We don't
// currently have a good way to handle this. As an interim work around we
// implement the left joins as left loop joins, including the binding updates
// needed before handing the tree off to the planner. This will at least let the
// planner do some optimizations, while still resulting in a plan that we can
// execute. Once there's a solution for the binding operator we should be able
// to delete this function all together.
func preImplementLeftLoopJoin(root *search.IntoExpr) *search.IntoExpr {
	var implLeftLoopJoin func(*search.IntoExpr, plandef.VarSet) *search.IntoExpr
	bindInputs := func(r *search.IntoExpr, bindableVars plandef.VarSet) *search.IntoExpr {
		for idx, input := range r.Inputs {
			r.Inputs[idx] = implLeftLoopJoin(input.(*search.IntoExpr), bindableVars)
		}
		return r
	}
	implLeftLoopJoin = func(r *search.IntoExpr, bindableVars plandef.VarSet) *search.IntoExpr {
		switch op := r.Operator.(type) {
		case *leftJoinOperator:
			r.Operator = &plandef.LoopJoin{Variables: op.variables, Specificity: parser.MatchOptional}
			r.Inputs[0] = implLeftLoopJoin(r.Inputs[0].(*search.IntoExpr), bindableVars)
			r.Inputs[1] = implLeftLoopJoin(r.Inputs[1].(*search.IntoExpr), bindableVars.Union(op.variables))
			return r

		case *innerJoinOperator:
			return bindInputs(r, bindableVars)

		case *lookupOperator:
			bound, _ := bindLookup(op, bindableVars)
			return search.NewExpr(bound)

		case *plandef.Enumerate:
			bound, _ := bindEnumerate(op, bindableVars)
			return search.NewExpr(bound)

		default:
			panic(fmt.Sprintf("preImplementLeftLoopJoin saw unexpected operator of type %T: %v", op, op))
		}
	}
	return implLeftLoopJoin(root, nil)
}

// getJoinableVars returns the variables used in the leaf operator.
// Returns the variables used in the lookup/enumeration.
func getJoinableVars(op search.Operator) plandef.VarSet {
	switch op := op.(type) {
	case *lookupOperator:
		return termsToVars(op.id, op.subject, op.predicate, op.object)
	case *plandef.Enumerate:
		return termsToVars(op.Output)
	}
	log.Panicf("getJoinableVars called with unexpected type %T", op)
	return nil
}

// termToVar returns the Variable if the term references one, otherwise it
// returns nil
func termToVar(t plandef.Term) *plandef.Variable {
	switch term := t.(type) {
	case *plandef.Variable:
		return term
	case *plandef.Binding:
		return term.Var
	}
	return nil
}

// termsToVars returns a VarSet of all the supplied terms that are or refer to a
// variable
func termsToVars(terms ...plandef.Term) plandef.VarSet {
	vars := make(map[string]*plandef.Variable)
	for _, t := range terms {
		tv := termToVar(t)
		if tv != nil {
			vars[tv.Name] = tv
		}
	}
	return plandef.NewVarSet(vars)
}

// variableScope is used to generate plandef.Variable's used in a single query.
// It handles the requirement to use the same plandef.Variable instance for a
// given variable name within a query.
type variableScope struct {
	vars map[string]*plandef.Variable
}

// named returns a Variable with the supplied name, creating it if needed.
func (v *variableScope) named(name string) *plandef.Variable {
	if v.vars == nil {
		v.vars = make(map[string]*plandef.Variable)
	}
	vr, exists := v.vars[name]
	if !exists {
		vr = &plandef.Variable{Name: name}
		v.vars[name] = vr
	}
	return vr
}
