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
	"math"
	"testing"

	"github.com/ebay/beam/query/planner/plandef"
	"github.com/stretchr/testify/assert"
)

func Test_LimitAndOffsetProperties(t *testing.T) {
	product := &plandef.Variable{Name: "product"}

	var tests = []struct {
		name     string
		op       *plandef.LimitAndOffsetOp
		inputs   []*logicalProperties
		expected *logicalProperties
	}{
		{
			name: "op_with_no_limit",
			op: &plandef.LimitAndOffsetOp{
				Paging: plandef.LimitOffset{},
			},
			inputs: []*logicalProperties{{
				variables:  plandef.VarSet{product},
				resultSize: 100,
			}},
			expected: &logicalProperties{
				variables:  plandef.VarSet{product},
				resultSize: 100,
			},
		},
		{
			name: "op_with_limit_smaller_than_input_result_size",
			op: &plandef.LimitAndOffsetOp{
				Paging: plandef.LimitOffset{
					Limit:  uint64p(10),
					Offset: uint64p(5),
				},
			},
			inputs: []*logicalProperties{{
				variables:  plandef.VarSet{product},
				resultSize: 100,
			}},
			expected: &logicalProperties{
				variables:  plandef.VarSet{product},
				resultSize: 10,
			},
		},
		{
			name: "op_with_limit_greater_than_input_result_size",
			op: &plandef.LimitAndOffsetOp{
				Paging: plandef.LimitOffset{
					Limit:  uint64p(10),
					Offset: uint64p(5),
				},
			},
			inputs: []*logicalProperties{{
				variables:  plandef.VarSet{product},
				resultSize: 5,
			}},
			expected: &logicalProperties{
				variables:  plandef.VarSet{product},
				resultSize: 5,
			},
		},
		{
			name: "op_with_limit_greater_than_maxint64",
			op: &plandef.LimitAndOffsetOp{
				Paging: plandef.LimitOffset{
					Limit:  uint64p(math.MaxInt64 + 1),
					Offset: uint64p(5),
				},
			},
			inputs: []*logicalProperties{{
				variables:  plandef.VarSet{product},
				resultSize: 5,
			}},
			expected: &logicalProperties{
				variables:  plandef.VarSet{product},
				resultSize: 5,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			act := limitAndOffsetLogicalProperties(tc.op, tc.inputs, nil)
			assert.Equal(t, tc.expected, act)
		})
	}
}

// uint64p returns pointer of the supplied uint64 val.
func uint64p(i uint64) *uint64 {
	return &i
}
