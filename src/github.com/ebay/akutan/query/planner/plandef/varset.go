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
	"sort"
	"strings"
)

// A VarSet is a set of variables. It's represented as an ordered slice of
// uniquely-named variables.
type VarSet []*Variable

// NewVarSet creates a new VarSet from the given variables. The key to 'in'
// should be the variable's Name.
func NewVarSet(in map[string]*Variable) VarSet {
	set := make([]*Variable, 0, len(in))
	for _, v := range in {
		set = append(set, v)
	}
	sort.Slice(set, func(i, j int) bool {
		return set[i].Name < set[j].Name
	})
	return set
}

// Contains returns true if v is in the set, false otherwise.
func (set VarSet) Contains(v *Variable) bool {
	i := sort.Search(len(set),
		func(i int) bool {
			return set[i].Name >= v.Name
		})
	return i < len(set) && set[i].Name == v.Name
}

// ContainsSet return true if all variables in 'other' are in 'set', false
// otherwise.
func (set VarSet) ContainsSet(other VarSet) bool {
	for _, v := range other {
		if !set.Contains(v) {
			return false
		}
	}
	return true
}

// Intersect returns a new set with the variables present in both 'set' and
// 'other'.
func (set VarSet) Intersect(other VarSet) VarSet {
	var both VarSet
	left, right := set, other
	for len(left) > 0 && len(right) > 0 {
		switch {
		case left[0].Name == right[0].Name:
			both = append(both, left[0])
			left = left[1:]
			right = right[1:]
		case left[0].Name < right[0].Name:
			left = left[1:]
		case left[0].Name > right[0].Name:
			right = right[1:]
		}
	}
	return both
}

// Union returns a new set with the variables present in either 'set' or 'other'.
func (set VarSet) Union(other VarSet) VarSet {
	var either VarSet
	left, right := set, other
	for len(left) > 0 && len(right) > 0 {
		switch {
		case left[0].Name == right[0].Name:
			either = append(either, left[0])
			left = left[1:]
			right = right[1:]
		case left[0].Name < right[0].Name:
			either = append(either, left[0])
			left = left[1:]
		case left[0].Name > right[0].Name:
			either = append(either, right[0])
			right = right[1:]
		}
	}
	if len(left) > 0 {
		either = append(either, left...)
	} else if len(right) > 0 {
		either = append(either, right...)
	}
	return either
}

// Sub returns a new set with the variables present in 'set' but not 'other'.
func (set VarSet) Sub(other VarSet) VarSet {
	var diff VarSet
	left, right := set, other
	for len(left) > 0 && len(right) > 0 {
		switch {
		case left[0].Name == right[0].Name:
			left = left[1:]
			right = right[1:]
		case left[0].Name < right[0].Name:
			diff = append(diff, left[0])
			left = left[1:]
		case left[0].Name > right[0].Name:
			right = right[1:]
		}
	}
	if len(left) > 0 {
		diff = append(diff, left...)
	}
	return diff
}

// Equal returns true if the two sets are made up of the same variable names,
// false otherwise.
func (set VarSet) Equal(other VarSet) bool {
	if len(set) != len(other) {
		return false
	}
	for i := range set {
		if set[i].Name != other[i].Name {
			return false
		}
	}
	return true
}

// String returns a space-delimited ordered list of variable names.
func (set VarSet) String() string {
	res := strings.Builder{}
	for i, v := range set {
		if i > 0 {
			res.WriteByte(' ')
		}
		res.WriteString(v.String())
	}
	return res.String()
}

// Key implements cmp.Key.
func (set VarSet) Key(b *strings.Builder) {
	for i, v := range set {
		if i > 0 {
			b.WriteByte(' ')
		}
		v.Key(b)
	}
}
