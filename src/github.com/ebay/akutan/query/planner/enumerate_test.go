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
	"testing"

	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/query/planner/search"
	"github.com/stretchr/testify/assert"
)

// sampleEnumerate returns a filled in Enumerate operator used in the tests
// below.
func sampleEnumerate() *plandef.Enumerate {
	return &plandef.Enumerate{
		Output: &plandef.Variable{Name: "var"},
		Values: []plandef.FixedTerm{
			&plandef.OID{Value: 30},
			&plandef.OID{Value: 40},
			&plandef.OID{Value: 50},
		},
	}
}

// Tests that an enumerate operator can make it through the search process. This
// includes checking that its logical properties and estimated costs are set.
func Test_Enumerate_alone(t *testing.T) {
	assert := assert.New(t)
	space := runSearch(t, search.NewExpr(sampleEnumerate()))
	assert.NoError(contains(space, `
Enumerate ?var {#30, #40, #50}
`))
}

// Tests basic planner rules manipulating an Enumerate operator below a Join.
func Test_Enumerate_withJoin(t *testing.T) {
	assert := assert.New(t)
	enumerate := sampleEnumerate()
	queryTree := search.NewExpr(
		&innerJoinOperator{
			variables: termsToVars(enumerate.Output),
		},
		search.NewExpr(&lookupOperator{
			id:        &plandef.DontCare{},
			subject:   enumerate.Output,
			predicate: &plandef.OID{Value: 3},
			object:    &plandef.OID{Value: 4},
		}),
		search.NewExpr(enumerate))
	space := runSearch(t, queryTree)
	assert.NoError(contains(space, `
InnerJoin ?var
	Lookup(_ ?var #3 #4)
	Enumerate ?var {#30, #40, #50}
`))
	assert.NoError(contains(space, `
HashJoin (inner) ?var
	Enumerate ?var {#30, #40, #50}
	LookupPO(_ ?var #3 #4)
`))
	assert.NoError(contains(space, `
HashJoin (inner) ?var
	LookupPO(_ ?var #3 #4)
	Enumerate ?var {#30, #40, #50}
`))
	assert.NoError(contains(space, `
LoopJoin (inner) ?var
	Enumerate ?var {#30, #40, #50}
	LookupSPO(_ $var #3 #4)
`))
	assert.NoError(contains(space, `
LoopJoin (inner) ?var
	LookupPO(_ ?var #3 #4)
	Enumerate $var {#30, #40, #50}
`))
}
