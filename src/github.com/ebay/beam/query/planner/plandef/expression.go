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

	"github.com/ebay/beam/query/parser"
	"github.com/ebay/beam/util/cmp"
)

// ExprBinding is a column in the Projection, it evaluates an expression and
// binds the result to a variable
type ExprBinding struct {
	Expr Expression
	Out  *Variable
}

// Key implements cmp.Key
func (b *ExprBinding) Key(k *strings.Builder) {
	k.WriteString("bind(")
	b.Expr.Key(k)
	k.WriteByte(' ')
	b.Out.Key(k)
	k.WriteByte(')')
}

func (b *ExprBinding) String() string {
	if b.Out == b.Expr {
		return b.Out.String()
	}
	return fmt.Sprintf("(%v AS %v)", b.Expr, b.Out)
}

// Expression represents an abstract calculable expression
type Expression interface {
	String() string
	cmp.Key
	anExpression()
}

var _ = []Expression{
	new(AggregateExpr),
	new(WildcardExpr),
	new(Variable),
}

// AggregateExpr is an Expression that is an aggregate calculation
type AggregateExpr struct {
	Func parser.AggregateFunction
	Of   Expression
}

func (*AggregateExpr) anExpression() {}

func (a *AggregateExpr) String() string {
	return fmt.Sprintf("%v(%v)", a.Func, a.Of)
}

// Key implements cmp.Key
func (a *AggregateExpr) Key(k *strings.Builder) {
	k.WriteString(a.Func.String())
	k.WriteByte('(')
	a.Of.Key(k)
	k.WriteByte(')')
}

// WildcardExpr represents a * Expression used in an aggregate expression.
type WildcardExpr struct {
}

func (w WildcardExpr) String() string {
	return "*"
}

// Key implements cmp.Key
func (w WildcardExpr) Key(k *strings.Builder) {
	k.WriteByte('*')
}

func (w WildcardExpr) anExpression() {}
