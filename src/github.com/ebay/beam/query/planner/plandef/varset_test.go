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
	"testing"

	"github.com/ebay/beam/util/cmp"
	"github.com/stretchr/testify/assert"
)

var (
	person   = &Variable{Name: "person"}
	place    = &Variable{Name: "place"}
	product  = &Variable{Name: "product"}
	postcard = &Variable{Name: "postcard"}
)

func newVarSet(vars ...*Variable) VarSet {
	m := make(map[string]*Variable)
	for _, v := range vars {
		m[v.Name] = v
	}
	return NewVarSet(m)
}

func ExampleVarSet() {
	vars := NewVarSet(map[string]*Variable{
		"person": &Variable{Name: "person"},
		"place":  &Variable{Name: "place"},
		"europe": &Variable{Name: "europe"},
	})
	// As it's a slice, you can for-range over a VarSet in order:
	for _, v := range vars {
		fmt.Println(v)
	}
	// Output:
	// ?europe
	// ?person
	// ?place
}

func TestVarSet_basic(t *testing.T) {
	assert := assert.New(t)
	vars := newVarSet(place, place, person)
	assert.True(vars.Contains(place))
	assert.True(vars.Contains(person))
	assert.False(vars.Contains(product))
	assert.Equal("?person ?place", vars.String())

	var varList []string
	for _, v := range vars {
		varList = append(varList, v.Name)
	}
	assert.Equal("person place", strings.Join(varList, " "))
}

func TestVarSet_Intersect(t *testing.T) {
	assert := assert.New(t)
	set1 := newVarSet(place, postcard, person)
	set2 := newVarSet(place, product, postcard)
	assert.Equal("?place ?postcard", set1.Intersect(set2).String())
	assert.Equal("?place ?postcard", set2.Intersect(set1).String())
}

func TestVarSet_Union(t *testing.T) {
	assert := assert.New(t)
	set1 := newVarSet(place, person)
	set2 := newVarSet(postcard, place)
	assert.Equal("?person ?place ?postcard", set1.Union(set2).String())
	assert.Equal("?person ?place ?postcard", set2.Union(set1).String())
}

func TestVarSet_Sub(t *testing.T) {
	assert := assert.New(t)
	set1 := newVarSet(place, person)
	set2 := newVarSet(postcard, place)
	assert.Equal("?person", set1.Sub(set2).String())
	assert.Equal("?postcard", set2.Sub(set1).String())
}

func TestVarSet_Equal(t *testing.T) {
	assert := assert.New(t)
	set1 := newVarSet(place, person)
	set2 := newVarSet(person)
	set3 := newVarSet(person, place)
	set4 := newVarSet(product, person)
	assert.False(set1.Equal(set2))
	assert.False(set2.Equal(set1))
	assert.True(set1.Equal(set3))
	assert.True(set3.Equal(set1))
	assert.False(set4.Equal(set1))
	assert.False(set1.Equal(set4))
}

func TestVarSet_ContainsSet(t *testing.T) {
	assert := assert.New(t)
	set1 := newVarSet(place, person)
	set2 := newVarSet(person)
	set3 := newVarSet(person, place)
	assert.True(set1.ContainsSet(set2))
	assert.True(set1.ContainsSet(set3))
	assert.True(set3.ContainsSet(set1))
	assert.True(set2.ContainsSet(set2))
	assert.False(set2.ContainsSet(set1))
	assert.False(set2.ContainsSet(set3))
}

func TestVarSet_Key(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("", cmp.GetKey(newVarSet()))
	vars := newVarSet(place, place, person)
	assert.Equal("?person ?place", cmp.GetKey(vars))
}
