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

package exec

import (
	"fmt"
	"strings"

	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/rpc"
)

// FactSet represents the facts that make up a result row from an operator. It
// consists of 0 or more facts that are true.
type FactSet struct {
	Facts []rpc.Fact
}

func (fs FactSet) String() string {
	return fmt.Sprintf("{Facts:%v}", fs.Facts)
}

// joinFactSets returns a new FactSet that is the union of the supplied 2
// FactSets. Facts are simply appended together. The supplied input FactSets are
// not mutated in any way.
func joinFactSets(a, b FactSet) FactSet {
	r := FactSet{
		Facts: make([]rpc.Fact, len(a.Facts)+len(b.Facts)),
	}
	copy(r.Facts, a.Facts)
	copy(r.Facts[len(a.Facts):], b.Facts)
	return r
}

// Value stores the current value of a single item. Typically these are the
// value for a Variable.
type Value struct {
	KGObject  rpc.KGObject
	ExtID     string
	LangExtID string
	UnitExtID string
}

// SetExtID stores the supplied ExternalID in the Value, formatting it as
// necessary. It doesn't stores the supplied ExternalID if it's empty. This along
// with SetLangExtID and SetUnitExtID are used by the externalIds query op to
// update values with the fetched externalID.
func (v *Value) SetExtID(s string) {
	if len(s) > 0 {
		if strings.IndexByte(s, ':') != -1 {
			v.ExtID = s
		} else {
			v.ExtID = "<" + s + ">"
		}
	}
}

// SetLangExtID stores the supplied LangExtID in the Value.
func (v *Value) SetLangExtID(s string) {
	// Note that the language trailer format doesn't use wrapping <>, i.e
	// in "SomeString"@en, the LangExtID is en.
	v.LangExtID = s
}

// SetUnitExtID stores the supplied UnitExtID in the Value, formatting it as
// necessary. It doesn't stores the supplied UnitExtID if it's empty.
func (v *Value) SetUnitExtID(s string) {
	if len(s) > 0 {
		if strings.IndexByte(s, ':') != -1 {
			v.UnitExtID = s
		} else {
			v.UnitExtID = "<" + s + ">"
		}
	}
}

func (v Value) String() string {
	if v.ExtID != "" {
		return v.ExtID + " " + v.KGObject.String()
	} else if v.LangExtID != "" {
		return v.KGObject.String() + " " + v.LangExtID
	} else if v.UnitExtID != "" {
		return v.KGObject.String() + " " + v.UnitExtID
	}
	return v.KGObject.String()
}

// variableSet represents the variables bound to the fields id, subject, predicate, object.
// Fields that are not bound to a variable are nil.
type variableSet [4]*plandef.Variable

// variableSetFromTerms returns a variableSet of the terms that are just Variables.
// 't' is expected to be the 4 Terms in the order id, subject, predicate, object.
func variableSetFromTerms(t []plandef.Term) variableSet {
	var r variableSet
	for i, t := range t {
		switch vt := t.(type) {
		case *plandef.Variable:
			r[i] = vt
		case *plandef.Binding:
			r[i] = vt.Var
		}
	}
	return r
}

func (vs variableSet) columns() Columns {
	res := make(Columns, 0, vs.count())
	for _, v := range vs {
		if v != nil {
			res = append(res, v)
		}
	}
	return res
}

// count returns the number populated variables in this variableSet.
func (vs variableSet) count() int {
	c := 0
	if vs[0] != nil {
		c++
	}
	if vs[1] != nil {
		c++
	}
	if vs[2] != nil {
		c++
	}
	if vs[3] != nil {
		c++
	}
	return c
}

// resultsOf takes an input fact and returns the result of binding the variables
// from the variableSet to it.
func (vs variableSet) resultsOf(f *rpc.Fact) []Value {
	c := vs.count()
	if c == 0 {
		return nil
	}
	res := make([]Value, 0, c)
	if vs[0] != nil {
		res = append(res, Value{KGObject: rpc.AKID(f.Id)})
	}
	if vs[1] != nil {
		res = append(res, Value{KGObject: rpc.AKID(f.Subject)})
	}
	if vs[2] != nil {
		res = append(res, Value{KGObject: rpc.AKID(f.Predicate)})
	}
	if vs[3] != nil {
		res = append(res, Value{KGObject: f.Object})
	}
	return res
}
