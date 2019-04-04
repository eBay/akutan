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
)

// InferPO is an Operator that starts at the given object and transitively
// follows predicate edges backwards to yield all reachable subjects.
type InferPO struct {
	ID        FreeTerm
	Subject   FreeTerm
	Predicate FixedTerm
	Object    FixedTerm
}

func (op *InferPO) anOperator() {}

func (op *InferPO) String() string {
	return fmt.Sprintf("InferPO(%v %v %v %v)",
		op.ID, op.Subject, op.Predicate, op.Object)
}

// Key implements cmp.Key.
func (op *InferPO) Key(b *strings.Builder) {
	lookupKey(b, "InferPO", op.ID, op.Subject, op.Predicate, op.Object)
}

// Terms is a convenience method to fetch the ID, Subject, Predicate, Object in
// an iterable slice.
func (op *InferPO) Terms() []Term {
	return []Term{
		op.ID,
		op.Subject,
		op.Predicate,
		op.Object,
	}
}

// InferSP is an Operator that finds all transitive paths from a subject using
// the given predicate edges.
type InferSP struct {
	ID        FreeTerm
	Subject   FixedTerm
	Predicate FixedTerm
	Object    FreeTerm
}

func (op *InferSP) anOperator() {}

func (op *InferSP) String() string {
	return fmt.Sprintf("InferSP(%v %v %v %v)",
		op.ID, op.Subject, op.Predicate, op.Object)
}

// Key implements cmp.Key.
func (op *InferSP) Key(b *strings.Builder) {
	lookupKey(b, "InferSP", op.ID, op.Subject, op.Predicate, op.Object)
}

// Terms is a convenience method to fetch the ID, Subject, Predicate, Object in
// an iterable slice.
func (op *InferSP) Terms() []Term {
	return []Term{
		op.ID,
		op.Subject,
		op.Predicate,
		op.Object,
	}
}

// InferSPO is an Operator that finds all transitive paths from subject to
// object using predicate edges.
type InferSPO struct {
	ID        FreeTerm
	Subject   FixedTerm
	Predicate FixedTerm
	// It's a bit of an open question, but we think this may only be an OID or a
	// binding to an OID, never a literal or binding to a literal value.
	// -Diego 2018-06-21
	Object FixedTerm
}

func (op *InferSPO) anOperator() {}

func (op *InferSPO) String() string {
	return fmt.Sprintf("InferSPO(%v %v %v %v)",
		op.ID, op.Subject, op.Predicate, op.Object)
}

// Key implements cmp.Key.
func (op *InferSPO) Key(b *strings.Builder) {
	lookupKey(b, "InferSPO", op.ID, op.Subject, op.Predicate, op.Object)
}

// Terms is a convenience method to fetch the ID, Subject, Predicate, Object in
// an iterable slice.
func (op *InferSPO) Terms() []Term {
	return []Term{
		op.ID,
		op.Subject,
		op.Predicate,
		op.Object,
	}
}

// LookupPO is an Operator that finds static facts with the given predicate and object.
type LookupPO struct {
	ID        FreeTerm
	Subject   FreeTerm
	Predicate FixedTerm
	Object    FixedTerm
}

func (op *LookupPO) anOperator() {}

func (op *LookupPO) String() string {
	return fmt.Sprintf("LookupPO(%v %v %v %v)",
		op.ID, op.Subject, op.Predicate, op.Object)
}

// Key implements cmp.Key.
func (op *LookupPO) Key(b *strings.Builder) {
	lookupKey(b, "LookupPO", op.ID, op.Subject, op.Predicate, op.Object)
}

// Terms is a convenience method to fetch the ID, Subject, Predicate, Object in
// an iterable slice.
func (op *LookupPO) Terms() []Term {
	return []Term{
		op.ID,
		op.Subject,
		op.Predicate,
		op.Object,
	}
}

// LookupPOCmp is an Operator that finds static facts with the given predicate
// and whose object is a literal value satisfying the given filter.
type LookupPOCmp struct {
	ID        FreeTerm
	Subject   FreeTerm
	Predicate FixedTerm
	Object    *Variable
	Cmp       SelectClause
}

func (op *LookupPOCmp) anOperator() {}

func (op *LookupPOCmp) String() string {
	return fmt.Sprintf("LookupPOCmp(%v %v %v %v %v)",
		op.ID, op.Subject, op.Predicate, op.Object, op.Cmp)
}

// Key implements cmp.Key.
func (op *LookupPOCmp) Key(b *strings.Builder) {
	lookupKey(b, "LookupPOCmp", op.ID, op.Subject, op.Predicate, op.Object)
	b.WriteByte(' ')
	op.Cmp.Key(b)
}

// Terms is a convenience method to fetch the ID, Subject, Predicate, Object in
// an iterable slice.
func (op *LookupPOCmp) Terms() []Term {
	return []Term{
		op.ID,
		op.Subject,
		op.Predicate,
		op.Object,
	}
}

// LookupS is an Operator that finds static facts with the given subject.
type LookupS struct {
	ID        FreeTerm
	Subject   FixedTerm
	Predicate FreeTerm
	Object    FreeTerm
}

func (op *LookupS) anOperator() {}

func (op *LookupS) String() string {
	return fmt.Sprintf("LookupS(%v %v %v %v)",
		op.ID, op.Subject, op.Predicate, op.Object)
}

// Key implements cmp.Key.
func (op *LookupS) Key(b *strings.Builder) {
	lookupKey(b, "LookupS", op.ID, op.Subject, op.Predicate, op.Object)
}

// Terms is a convenience method to fetch the ID, Subject, Predicate, Object in
// an iterable slice.
func (op *LookupS) Terms() []Term {
	return []Term{
		op.ID,
		op.Subject,
		op.Predicate,
		op.Object,
	}
}

// LookupSP is an Operator that finds static facts with the given subject and predicate.
type LookupSP struct {
	ID        FreeTerm
	Subject   FixedTerm
	Predicate FixedTerm
	Object    FreeTerm
}

func (op *LookupSP) anOperator() {}

func (op *LookupSP) String() string {
	return fmt.Sprintf("LookupSP(%v %v %v %v)",
		op.ID, op.Subject, op.Predicate, op.Object)
}

// Key implements cmp.Key.
func (op *LookupSP) Key(b *strings.Builder) {
	lookupKey(b, "LookupSP", op.ID, op.Subject, op.Predicate, op.Object)
}

// Terms is a convenience method to fetch the ID, Subject, Predicate, Object in
// an iterable slice.
func (op *LookupSP) Terms() []Term {
	return []Term{
		op.ID,
		op.Subject,
		op.Predicate,
		op.Object,
	}
}

// LookupSPO is an Operator that finds the(?) static fact with the given subject, predicate, and object.
type LookupSPO struct {
	ID        FreeTerm
	Subject   FixedTerm
	Predicate FixedTerm
	Object    FixedTerm
}

func (op *LookupSPO) anOperator() {}

func (op *LookupSPO) String() string {
	return fmt.Sprintf("LookupSPO(%v %v %v %v)",
		op.ID, op.Subject, op.Predicate, op.Object)
}

// Key implements cmp.Key.
func (op *LookupSPO) Key(b *strings.Builder) {
	lookupKey(b, "LookupSPO", op.ID, op.Subject, op.Predicate, op.Object)
}

// Terms is a convenience method to fetch the ID, Subject, Predicate, Object in
// an iterable slice.
func (op *LookupSPO) Terms() []Term {
	return []Term{
		op.ID,
		op.Subject,
		op.Predicate,
		op.Object,
	}
}

// lookupKey is a helper for the Lookup/Infer Key() methods.
func lookupKey(b *strings.Builder, op string, id, subject, predicate, object Term) {
	b.WriteString(op)
	b.WriteByte(' ')
	id.Key(b)
	b.WriteByte(' ')
	subject.Key(b)
	b.WriteByte(' ')
	predicate.Key(b)
	b.WriteByte(' ')
	object.Key(b)
}
