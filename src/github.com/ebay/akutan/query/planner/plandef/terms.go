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
	"strconv"
	"strings"

	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/cmp"
)

// A Term is an argument to an Operator, such as a Variable or an OID.
// Every type of Term is either a FreeTerm or a FixedTerm.
type Term interface {
	String() string
	cmp.Key
	aTerm()
}

// A FreeTerm is a Term that has an unknown value when the operator executes. It
// represents a result from the operator.
type FreeTerm interface {
	Term
	aFreeTerm()
}

// ImplementFreeTerm is a list of types that implement FreeTerm. This serves as
// documentation and as a compile-time check.
var ImplementFreeTerm = []FreeTerm{
	new(DontCare),
	new(Variable),
}

// A FixedTerm is a Term that has a known value before the operator executes.
type FixedTerm interface {
	Term
	aFixedTerm()
}

// ImplementFixedTerm is a list of types that implement FixedTerm. This serves
// as documentation and as a compile-time check.
var ImplementFixedTerm = []FixedTerm{
	new(Binding),
	new(Literal),
	new(OID),
}

// DontCare is a FreeTerm representing a placeholder for a result to be discarded.
type DontCare struct {
	// The very bottom of the Go spec
	// https://golang.org/ref/spec#Size_and_alignment_guarantees says pointers to
	// two empty slices may be the same. This byte of padding ensures that pointers
	// distinct DontCare values will by unequal (not ==).
	_ byte
}

func (*DontCare) aTerm()     {}
func (*DontCare) aFreeTerm() {}

// String returns "_".
func (d *DontCare) String() string {
	return "_"
}

// Key implements cmp.Key.
func (d *DontCare) Key(b *strings.Builder) {
	b.WriteByte('_')
}

// A Variable is a FreeTerm representing a placeholder for a named result.
type Variable struct {
	Name string
}

func (*Variable) aTerm()        {}
func (*Variable) aFreeTerm()    {}
func (*Variable) anExpression() {}

// String returns a string like "?foo".
func (v *Variable) String() string {
	return "?" + v.Name
}

// Key implements cmp.Key.
func (v *Variable) Key(b *strings.Builder) {
	b.WriteByte('?')
	b.WriteString(v.Name)
}

// A Binding is a variable that will be filled in by values from an outer nested
// loop join. Unlike a Variable, Binding is a FixedTerm.
type Binding struct {
	Var *Variable
}

func (*Binding) aTerm()      {}
func (*Binding) aFixedTerm() {}

// String returns a string like "$foo".
func (b *Binding) String() string {
	return "$" + b.Var.Name
}

// Key implements cmp.Key.
func (b *Binding) Key(buf *strings.Builder) {
	buf.WriteByte('$')
	buf.WriteString(b.Var.Name)
}

// An OID is a FixedTerm containing an opaque resource identifier.
type OID struct {
	Value uint64
	// If not empty, this is a unique identifier for the same entity that's more
	// human-friendly, like "<foo>" or "rdfs:label".
	Hint string
}

func (*OID) aTerm()      {}
func (*OID) aFixedTerm() {}

// String returns oid.Hint if that's set; otherwise, it returns a string like
// "#1052".
func (oid *OID) String() string {
	if oid.Hint == "" {
		return fmt.Sprintf("#%v", oid.Value)
	}
	return oid.Hint
}

// Key implements cmp.Key. The key is based on oid.Value alone; the Hint does
// not affect it.
func (oid *OID) Key(b *strings.Builder) {
	b.WriteByte('#')
	b.WriteString(strconv.FormatUint(oid.Value, 10))
}

// A Literal is a FixedTerm containing a literal value, like a particular
// float or string.
type Literal struct {
	// Most KGObject types are allowed, but KID is not: that is represented as an
	// OID instead.
	Value rpc.KGObject
}

func (*Literal) aTerm()      {}
func (*Literal) aFixedTerm() {}

func (literal *Literal) String() string {
	return literal.Value.String()
}

// Key implements cmp.Key.
func (literal *Literal) Key(b *strings.Builder) {
	literal.Value.Key(b)
}
