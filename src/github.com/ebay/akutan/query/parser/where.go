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
	"fmt"
	"strings"
	"time"

	"github.com/ebay/akutan/api"
	"github.com/ebay/akutan/rpc"
)

var (
	// NoUnit indicates no units were specified for the literal.
	NoUnit = Unit{}

	// NoLanguage indicates no language was specified for the string.
	NoLanguage = Language{}

	patterns = map[api.Precision]string{
		api.Year:       "2006",
		api.Month:      "2006-01",
		api.Day:        "2006-01-02",
		api.Hour:       "2006-01-02T15",
		api.Minute:     "2006-01-02T15:04",
		api.Second:     "2006-01-02T15:04:05",
		api.Nanosecond: "2006-01-02T15:04:05.999999999",
	}
)

// Quad query statement.
type Quad struct {
	// ID is set to &Nil{} when not present.
	// TODO: should it be nil instead?
	ID          Term
	Subject     Term
	Predicate   Term
	Object      Term
	Specificity MatchSpecificity
}

// WhereClause type.
type WhereClause []*Quad

// Entity type.
type Entity struct {
	ID    uint64
	Value string
}

// QName type.
// eg. rdf:type is a QName (qualified name)
type QName struct {
	ID    uint64
	Value string
}

// Variable name type.
type Variable struct {
	Name string
}

func (*Variable) isExpression()       {}
func (*Variable) isSelectClauseItem() {}
func (*Variable) isAggregateTarget()  {}

// LiteralBool type.
type LiteralBool struct {
	Unit  Unit
	Value bool
}

// LiteralFloat type.
type LiteralFloat struct {
	Unit  Unit
	Value float64
}

// LiteralID type.
type LiteralID struct {
	Value uint64
	// A human-readable string that might be more evocative than the opaque
	// Value. This string includes angle brackets (like "<foo>") when rewritten
	// from an Entity.
	Hint string
}

// LiteralInt type.
type LiteralInt struct {
	Unit  Unit
	Value int64
}

// LiteralSet represents a set of literal values.
type LiteralSet struct {
	Values []Term
}

// LiteralString object value type.
type LiteralString struct {
	Language Language
	Value    string
}

// LiteralTime type.
type LiteralTime struct {
	Unit      Unit
	Value     time.Time
	Precision api.Precision
}

// Unit type.
type Unit struct {
	ID    uint64
	Value string
}

// Language type.
type Language struct {
	ID    uint64
	Value string
}

// Nil type.
type Nil struct{}

// Operator type.
type Operator struct {
	Value rpc.Operator
}

// A Term is a reference to an entity, an operator, a variable, Nil, a primitive
// literal, or a set of these. Sets of sets are not allowed. Each field in a
// Quad is a Term.
type Term interface {
	// Marker method to prevent other types from implementing Term.
	aTerm()
	// equalType returns true if the Terms' types match.
	equalType(Term) bool
	// Return a parseable string representation of the Term. It is used for
	// debugging to build a parseable string of the query. It is also used to
	// distinguish Terms when building the literal set.
	String() string
}

// Ensures that each of these implements the Term interface.
var _ = []Term{
	new(Entity),
	new(LiteralBool),
	new(LiteralFloat),
	new(LiteralID),
	new(LiteralInt),
	new(LiteralSet),
	new(LiteralString),
	new(LiteralTime),
	new(Nil),
	new(Operator),
	new(QName),
	new(Variable),
}

func (*Entity) aTerm()        {}
func (*LiteralBool) aTerm()   {}
func (*LiteralFloat) aTerm()  {}
func (*LiteralID) aTerm()     {}
func (*LiteralInt) aTerm()    {}
func (*LiteralSet) aTerm()    {}
func (*LiteralString) aTerm() {}
func (*LiteralTime) aTerm()   {}
func (*Nil) aTerm()           {}
func (*Operator) aTerm()      {}
func (*QName) aTerm()         {}
func (*Variable) aTerm()      {}

func (*Entity) equalType(t Term) bool {
	_, ok := t.(*Entity)
	return ok
}
func (*LiteralBool) equalType(t Term) bool {
	_, ok := t.(*LiteralBool)
	return ok
}
func (*LiteralFloat) equalType(t Term) bool {
	_, ok := t.(*LiteralFloat)
	return ok
}
func (*LiteralID) equalType(t Term) bool {
	_, ok := t.(*LiteralID)
	return ok
}
func (*LiteralInt) equalType(t Term) bool {
	_, ok := t.(*LiteralInt)
	return ok
}
func (*LiteralSet) equalType(t Term) bool {
	_, ok := t.(*LiteralSet)
	return ok
}
func (*LiteralString) equalType(t Term) bool {
	_, ok := t.(*LiteralString)
	return ok
}
func (*LiteralTime) equalType(t Term) bool {
	_, ok := t.(*LiteralTime)
	return ok
}
func (*Nil) equalType(t Term) bool {
	_, ok := t.(*Nil)
	return ok
}
func (*Operator) equalType(t Term) bool {
	_, ok := t.(*Operator)
	return ok
}
func (*QName) equalType(t Term) bool {
	_, ok := t.(*QName)
	return ok
}
func (*Variable) equalType(t Term) bool {
	_, ok := t.(*Variable)
	return ok
}

func (e *Entity) String() string {
	return fmt.Sprintf("<%s>", e.Value)
}

func (l *Language) String() string {
	return l.Value
}

func (l *LiteralBool) String() string {
	if l.Unit == NoUnit {
		return fmt.Sprintf("%v", l.Value)
	}
	return fmt.Sprintf("%v^^%s", l.Value, &l.Unit)
}

func (l *LiteralFloat) String() string {
	if l.Unit == NoUnit {
		return fmt.Sprintf("%f", l.Value)
	}
	return fmt.Sprintf("%f^^%s", l.Value, &l.Unit)
}

func (l *LiteralID) String() string {
	if l.Hint == "" {
		return fmt.Sprintf("#%d", l.Value)
	}
	return l.Hint
}

func (l *LiteralInt) String() string {
	if l.Unit == NoUnit {
		return fmt.Sprintf("%d", l.Value)
	}
	return fmt.Sprintf("%d^^%s", l.Value, &l.Unit)
}

func (l *LiteralSet) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "{")
	for i, v := range l.Values {
		if i == 0 {
			fmt.Fprintf(&b, "%v", v.String())
		} else {
			fmt.Fprintf(&b, ", %v", v.String())
		}
	}
	fmt.Fprintf(&b, "}")
	return b.String()
}

func (l *LiteralString) String() string {
	if l.Language == NoLanguage {
		return fmt.Sprintf(`"%s"`, l.Value)
	}
	return fmt.Sprintf(`"%s"@%s`, l.Value, &l.Language)
}

func (l *LiteralTime) String() string {
	if l.Unit == NoUnit {
		return fmt.Sprintf("'%s' (%v)", l.Value.Format(patterns[l.Precision]), l.Precision)
	}
	return fmt.Sprintf("'%s' (%v)^^%v", l.Value.Format(patterns[l.Precision]), l.Precision, l.Unit)
}

func (n *Nil) String() string {
	return "_"
}

func (o *Operator) String() string {
	return fmt.Sprintf("%v", o.Value)
}

func (q *QName) String() string {
	return q.Value
}

func (q *Quad) String() string {
	return fmt.Sprintf("%v %v %v%s %v", q.ID, q.Subject, q.Predicate, q.Specificity.queryLineFormat(), q.Object)
}

func (q WhereClause) String() string {
	str := strings.Builder{}
	for i, quad := range q {
		if i > 0 {
			str.WriteByte('\n')
		}
		str.WriteString(quad.String())
	}
	return str.String()
}

func (u *Unit) String() string {
	return u.Value
}

func (v *Variable) String() string {
	return fmt.Sprintf("?%s", v.Name)
}

// Terms that require lookups of a string identifer into an internal KID
// implement requiresKIDs. The lookupExternalIDs rewriter will collect these up
// and set the results.
type requiresKIDs interface {
	toResolve() []resolveItem
}

// resolveItem is a single item that needs resolving. The rewriter will store the
// result in the uint64 pointed to by KID. The rewriter will return an error in the
// event that it can't find a KID for the supplied StringID.
type resolveItem struct {
	StringID string
	KID      *uint64
}

// Ensures each of these implement requiresKIDs.
var _ = []requiresKIDs{
	new(Language),
	new(Unit),
	new(QName),
	new(Entity),
	new(LiteralBool),
	new(LiteralFloat),
	new(LiteralInt),
	new(LiteralSet),
	new(LiteralString),
	new(LiteralTime),
	new(Quad),
}

func (l *Language) toResolve() []resolveItem {
	if l.Value != "" {
		return []resolveItem{{StringID: l.Value, KID: &l.ID}}
	}
	return nil
}

func (u *Unit) toResolve() []resolveItem {
	if u.Value != "" {
		return []resolveItem{{StringID: u.Value, KID: &u.ID}}
	}
	return nil
}

func (q *QName) toResolve() []resolveItem {
	return []resolveItem{{StringID: q.Value, KID: &q.ID}}
}

func (e *Entity) toResolve() []resolveItem {
	return []resolveItem{{StringID: e.Value, KID: &e.ID}}
}

func (l *LiteralBool) toResolve() []resolveItem {
	return l.Unit.toResolve()
}

func (l *LiteralFloat) toResolve() []resolveItem {
	return l.Unit.toResolve()
}

func (l *LiteralInt) toResolve() []resolveItem {
	return l.Unit.toResolve()
}

func (l *LiteralString) toResolve() []resolveItem {
	return l.Language.toResolve()
}

func (l *LiteralTime) toResolve() []resolveItem {
	return l.Unit.toResolve()
}

func (l *LiteralSet) toResolve() []resolveItem {
	var results []resolveItem
	for _, v := range l.Values {
		if resolve, ok := v.(requiresKIDs); ok {
			results = append(results, resolve.toResolve()...)
		}
	}
	return results
}

func (q *Quad) toResolve() []resolveItem {
	var results []resolveItem
	add := func(terms ...Term) {
		for _, term := range terms {
			if resolve, ok := term.(requiresKIDs); ok {
				results = append(results, resolve.toResolve()...)
			}
		}
	}
	add(q.ID, q.Subject, q.Predicate, q.Object)
	return results
}

// Literal represents comparable object values.
type Literal interface {
	Term
	// Literal returns the api.KGObject representation of a parsed literal value.
	Literal() *api.KGObject
}

// Literal returns a boolean api object.
func (l *LiteralBool) Literal() *api.KGObject {
	return &api.KGObject{UnitID: l.Unit.ID, Value: &api.KGObject_ABool{ABool: l.Value}}
}

// Literal returns a floating point api object.
func (l *LiteralFloat) Literal() *api.KGObject {
	return &api.KGObject{UnitID: l.Unit.ID, Value: &api.KGObject_AFloat64{AFloat64: l.Value}}
}

// Literal returns a kid api object.
func (l *LiteralID) Literal() *api.KGObject {
	return &api.KGObject{Value: &api.KGObject_AKID{AKID: l.Value}}
}

// Literal returns a string api object.
func (l *LiteralString) Literal() *api.KGObject {
	return &api.KGObject{LangID: l.Language.ID, Value: &api.KGObject_AString{AString: l.Value}}
}

// Literal returns an integer api object.
func (l *LiteralInt) Literal() *api.KGObject {
	return &api.KGObject{UnitID: l.Unit.ID, Value: &api.KGObject_AInt64{AInt64: l.Value}}
}

// Literal returns a time api object.
func (l *LiteralTime) Literal() *api.KGObject {
	return &api.KGObject{
		UnitID: l.Unit.ID,
		Value: &api.KGObject_ATimestamp{
			ATimestamp: &api.KGTimestamp{
				Precision: l.Precision,
				Value:     l.Value,
			},
		},
	}
}

// MatchSpecificity describes requirements for how a query line fits with the
// overall query
type MatchSpecificity int

const (
	// MatchRequired indicates that the queryline is required to have a matching
	// fact for there to be a result
	MatchRequired MatchSpecificity = 0
	// MatchOptional indicates that the queryline is optional, if it exists it's
	// returned, if it doesn't it does not prevent an otherwise complete result
	// being generated.
	MatchOptional MatchSpecificity = 1
)

func (m MatchSpecificity) String() string {
	switch m {
	case MatchRequired:
		return "Required"
	case MatchOptional:
		return "Optional"
	default:
		return fmt.Sprintf("MatchSpecificity(%d)", int(m))
	}
}

// queryLineFormat returns this MatchSpecificity as it would be indicated in a
// query line.
func (m MatchSpecificity) queryLineFormat() string {
	switch m {
	case MatchRequired:
		return ""
	case MatchOptional:
		return "?"
	default:
		return fmt.Sprintf("MatchSpecificity(%d)", int(m))
	}
}
