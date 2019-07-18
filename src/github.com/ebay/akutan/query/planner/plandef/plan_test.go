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
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_PlanStringer(t *testing.T) {
	p := Plan{
		Operator: &Projection{Select: []ExprBinding{{Expr: product, Out: product}}},
		Inputs: []*Plan{{
			Operator: &LoopJoin{Variables: VarSet{product}},
			Inputs: []*Plan{{
				Operator: &LookupSP{&DontCare{}, &OID{Value: 1}, &OID{Value: 2}, product},
			}, {
				Operator: &LookupSP{&DontCare{}, &OID{Value: 10}, &OID{Value: 3}, product},
			}},
		}},
	}
	exp := `
Project ?product
	LoopJoin (inner) ?product
		LookupSP(_ #1 #2 ?product)
		LookupSP(_ #10 #3 ?product)
`
	assert.Equal(t, exp, "\n"+p.String())
}
