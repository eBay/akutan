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

package search

import (
	"github.com/ebay/akutan/util/cmp"
)

// A Definition is the interface that users of this package need to define.
type Definition interface {
	// ExplorationRules returns the set of exploration rules that the optimizer may
	// apply in searching for a low-cost plan.
	ExplorationRules() []ExplorationRule
	// ImplementationRules returns the set of implementation rules that the
	// optimizer may apply in searching for a low-cost plan.
	ImplementationRules() []ImplementationRule
	// LocalCost returns the predicted costs of the operator itself, excluding
	// the costs of its inputs. It should return an infiniteCost for non-physical
	// operators. When invoked, each expr.Inputs group will have LogicalProperties
	// available but may not have Best set, and each of the groups Exprs may not have
	// LocalCost or CombinedCost set.
	LocalCost(expr *Expr) Cost
	// CombinedCost returns the cost of the operator combined with the costs of its inputs.
	// When invoked, each expr.Inputs group wil have Best set, and the Best expr
	// will have its LocalCost and CombinedCost set.
	// For most operators, this returns:
	//    expr.LocalCost + [group.Best.CombinedCost for group in expr.Inputs]
	CombinedCost(expr *Expr) Cost
	// LogicalProperties computes and returns the logical properties for a logical
	// expression. It should panic if invoked for a physical operators, as this is
	// only needed when creating a new logical equivalence class.
	LogicalProperties(op Operator, inputs []LogicalProperties) LogicalProperties
	// MakePlanNode creates a new PlanNode object. A simple implementation could
	// just create a struct out of the given arguments, but this function exists as
	// an opportunity for the user of this package to transform the output into a
	// more convenient format. For example, the given operator can be cast into a
	// more specific type.
	MakePlanNode(op Operator, inputs []PlanNode, lprop LogicalProperties) PlanNode
}

// An ExplorationRule defines a logical equivalence.
type ExplorationRule struct {
	Name string
	// Returns logical or physical expressions that are equivalent to the input.
	Apply func(*Expr) []*IntoExpr
}

// An ImplementationRule maps logical expressions to physical access plans.
type ImplementationRule struct {
	Name string
	// Returns physical expressions that are equivalent to the non-physical input.
	Apply func(*Expr) []*IntoExpr
}

// An Operator is a logical or physical (executable) function attached to an
// expression.
type Operator interface {
	// Returns a human-readable single-line string describing the operator.
	String() string
	// The identity of an Operator is given by its Key() output. Two distinct
	// operators or operators with distinct parameters must produce distinct
	// keys. Also, the returned key cannot change over the lifetime of the
	// Operator -- Operators should be immutable.
	cmp.Key
}

// LogicalProperties are properties that remain true across logically equivalent
// expressions. They are defined by the specific optimizer needs but are
// commonly used to represent the schema of the results and the expected result
// size.
type LogicalProperties interface {
	String() string
	DetailString() string
}

// Cost represents an estimated cost for an Expr, used to find a low-cost plan.
type Cost interface {
	// Returns a human-readable single-line description of the cost.
	String() string
	// Returns true if the expression can't be executed in any reasonable period of
	// time, false otherwise.
	Infinite() bool
	// Returns true if this cost is strictly lower than the given cost, false otherwise.
	// This method may assume the two Costs have been produced by the same Estimator
	// (for most Estimators, this and 'other' will have the same type).
	Less(other Cost) bool
}

// A PlanNode is part of a physically executable plan. It represents one node in
// the plan tree that is the result of the search. A PlanNode differs from an
// Expr in that it takes concrete inputs, whereas an Expr takes equivalence
// classes as inputs.
type PlanNode interface{}
