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

package plandef

import (
	"fmt"
	"strings"

	"github.com/ebay/akutan/query/parser"
)

// A HashJoin Operator takes two inputs and implements a join using a hash table.
type HashJoin struct {
	// The variables that are compared for equality in both inputs.
	Variables   VarSet
	Specificity parser.MatchSpecificity
}

func (op *HashJoin) anOperator() {}

func (op *HashJoin) String() string {
	return fmt.Sprintf("HashJoin %s %v", joinLabel(op.Specificity), op.Variables)
}

// Key implements cmp.Key.
func (op *HashJoin) Key(b *strings.Builder) {
	b.WriteString("HashJoin ")
	b.WriteString(joinLabel(op.Specificity))
	b.WriteByte(' ')
	op.Variables.Key(b)
}

func joinLabel(s parser.MatchSpecificity) string {
	switch s {
	case parser.MatchRequired:
		return "(inner)"
	case parser.MatchOptional:
		return "(left)"
	}
	return fmt.Sprintf("(%d)", int(s))
}

// A LoopJoin Operator takes two inputs and implements a join using a nested
// loop. It passes results from the first input into queries on the second input;
// these will appear as Binding terms in the second input.
type LoopJoin struct {
	// The variables that are compared for equality in both inputs.
	Variables   VarSet
	Specificity parser.MatchSpecificity
}

func (op *LoopJoin) anOperator() {}

func (op *LoopJoin) String() string {
	return fmt.Sprintf("LoopJoin %s %v", joinLabel(op.Specificity), op.Variables)
}

// Key implements cmp.Key.
func (op *LoopJoin) Key(b *strings.Builder) {
	b.WriteString("LoopJoin ")
	b.WriteString(joinLabel(op.Specificity))
	b.WriteByte(' ')
	op.Variables.Key(b)
}
