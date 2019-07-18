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
	"errors"
	"testing"
	"time"

	"github.com/ebay/akutan/facts/cache"
	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/clocks"
	"github.com/stretchr/testify/assert"
)

func Test_Execute(t *testing.T) {
	type test struct {
		plan     plandef.Plan
		expected ResultChunk
	}
	tests := []test{{
		plan: plandef.Plan{
			Operator: &plandef.Enumerate{
				Output: varS,
				Values: []plandef.FixedTerm{&plandef.OID{Value: 123}, &plandef.OID{Value: 124}},
			},
			Variables: plandef.VarSet{varS},
		},
		expected: ResultChunk{
			Columns: Columns{varS},
			Values:  []Value{kidVal(123), kidVal(124)},
			offsets: []uint32{0, 0},
			Facts:   []FactSet{{Facts: nil}, {Facts: nil}},
		},
	}, {
		plan: plandef.Plan{
			Operator: &plandef.Enumerate{
				Output: varS,
				Values: []plandef.FixedTerm{},
			},
			Variables: plandef.VarSet{varS},
		},
		expected: ResultChunk{
			Columns: Columns{varS},
		},
	}}
	for _, test := range tests {
		t.Run(test.plan.String(), func(t *testing.T) {
			resCh := make(chan ResultChunk, 4)
			err := Execute(context.Background(), nil, 1, cache.New(), nil, &test.plan, resCh)
			assert.NoError(t, err)
			if assert.Len(t, resCh, 1) {
				assert.Equal(t, test.expected, <-resCh)
				assert.True(t, isClosed(resCh))
			}
		})
	}
}

func Test_DecoratedOp(t *testing.T) {
	clock := clocks.NewMock()
	start := clock.Now()
	a := FactSet{Facts: []rpc.Fact{{Subject: 1}}}
	b := FactSet{Facts: []rpc.Fact{{Subject: 2}}}
	c := FactSet{Facts: []rpc.Fact{{Subject: 1}}}
	op := &mockOp{
		def:  &plandef.LookupS{},
		cols: Columns{varS},
		exec: func(_ context.Context, binder valueBinder, res results) error {
			assert.NotNil(t, res)
			res.add(ctx, 0, a, []Value{{KGObject: rpc.AKID(10)}})
			res.add(ctx, 1, b, []Value{{KGObject: rpc.AKID(11)}})
			res.add(ctx, 2, c, []Value{{KGObject: rpc.AKID(12)}})
			clock.Advance(time.Second)
			return nil
		},
	}
	events := captureEvents{clock: clock}
	queryOp := decoratedOp{events: &events, op: op}
	resCh := make(chan ResultChunk, 4)
	err := queryOp.run(context.Background(), new(defaultBinder), resCh)
	assert.NoError(t, err)
	if assert.Len(t, resCh, 1) {
		res := <-resCh
		if assert.Equal(t, 3, res.NumRows()) {
			assert.Equal(t, []FactSet{a, b, c}, res.Facts)
			assert.Equal(t, []uint32{0, 1, 2}, res.offsets)
			assert.Equal(t, Columns{varS}, res.Columns)
			assert.Equal(t, []Value{
				{KGObject: rpc.AKID(10)},
				{KGObject: rpc.AKID(11)},
				{KGObject: rpc.AKID(12)},
			}, res.Values)
		}
		assert.True(t, isClosed(resCh))
	}
	if assert.Len(t, events.events, 1) {
		assert.Equal(t, OpCompletedEvent{
			Operator:       op.def,
			InputBulkCount: 1,
			StartedAt:      start,
			EndedAt:        start.Add(time.Second),
			Output: StreamStats{
				NumChunks:   1,
				NumFactSets: 3,
			},
			Err: nil,
		}, events.events[0])
	}
}

func Test_DecoratedOpError(t *testing.T) {
	expErr := errors.New("Unable to lookupS")
	op := &mockOp{
		def: &plandef.LookupS{},
		exec: func(_ context.Context, binder valueBinder, res results) error {
			assert.NotNil(t, res)
			return expErr
		},
	}
	clock := clocks.NewMock()
	events := captureEvents{clock: clock}
	queryOp := decoratedOp{events: &events, op: op}
	assert.NotNil(t, queryOp)
	resCh := make(chan ResultChunk, 4)
	err := queryOp.run(context.Background(), new(defaultBinder), resCh)
	assert.Equal(t, expErr, err)
	assert.Len(t, resCh, 0)
	assert.True(t, isClosed(resCh))
	if assert.Len(t, events.events, 1) {
		assert.Equal(t, OpCompletedEvent{
			Operator:       op.def,
			InputBulkCount: 1,
			StartedAt:      clock.Now(),
			EndedAt:        clock.Now(),
			Output: StreamStats{
				NumChunks:   0,
				NumFactSets: 0,
			},
			Err: expErr,
		}, events.events[0])
	}
}
