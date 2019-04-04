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
	"context"
	"testing"

	"github.com/ebay/beam/query/parser"
	"github.com/ebay/beam/query/planner/plandef"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_HashJoin(t *testing.T) {
	for _, test := range joinDataSets() {
		t.Run(test.name, func(t *testing.T) {
			hashJoinPlan := &plandef.HashJoin{
				Variables:   test.joinVars,
				Specificity: test.joinType,
			}
			leftOp := makeInputOpForHashJoin(test.left)
			rightOp := makeInputOpForHashJoin(test.right)

			hashJoinOp := newHashJoin(hashJoinPlan, []queryOperator{leftOp, rightOp})
			res, err := executeOp(t, hashJoinOp, test.inputBinder)
			assert.NoError(t, err)
			assertResultChunkEqual(t, test.expResults.sorted(t), res.sorted(t))
		})
	}
}

// makeInputOpForHashJoin returns a queryOperator that returns the next chunk
// from chunks for each execution.
func makeInputOpForHashJoin(chunks []ResultChunk) queryOperator {
	currentOffset := 0
	return &mockQueryOp{
		cols: chunks[0].Columns,
		exec: func(ctx context.Context, b valueBinder, resCh chan<- ResultChunk) error {
			c := chunks[currentOffset]
			// hash join is executed one at a time, so offsets should always be
			// zero.
			c.offsets = make([]uint32, len(c.offsets))
			resCh <- c
			currentOffset++
			close(resCh)
			return nil
		},
	}
}

func Test_HashJoinPanicsOnBogusSpecificity(t *testing.T) {
	hashJoinOp := plandef.HashJoin{
		Variables:   plandef.VarSet{varS},
		Specificity: parser.MatchSpecificity(42),
	}
	defer func() {
		// logrus.Panic panics with a logrus.Entry as the panic value
		// which includes an embedded timestamp. This makes it impossible to
		// test with assert.PanicsWithValue
		p := recover()
		assert.NotNil(t, p)
		exp := "Unexpected value for Join Specificity MatchSpecificity(42)"
		msg, err := p.(*logrus.Entry).String()
		assert.NoError(t, err)
		assert.Contains(t, msg, exp)
	}()
	newHashJoin(&hashJoinOp, []queryOperator{nil, nil})
}
