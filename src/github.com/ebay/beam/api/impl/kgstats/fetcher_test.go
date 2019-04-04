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

package kgstats

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ebay/beam/query/planner"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/clocks"
	"github.com/stretchr/testify/assert"
)

// mockStats is an integer that implements planner.Stats.
// Most of its methods return this integer.
type mockStats int

var _ planner.Stats = mockStats(1)

func (m mockStats) NumSPOPartitions() int {
	return int(m)
}
func (m mockStats) NumPOSPartitions() int {
	return int(m)
}
func (m mockStats) BytesPerFact() int {
	return int(m)
}
func (m mockStats) NumFacts() int {
	return int(m)
}
func (m mockStats) NumFactsS(subject uint64) int {
	return int(m)
}
func (m mockStats) NumFactsSP(subject uint64, predicate uint64) int {
	return int(m)
}
func (m mockStats) NumFactsSO(subject uint64, object rpc.KGObject) int {
	return int(m)
}
func (m mockStats) NumFactsP(predicate uint64) int {
	return int(m)
}
func (m mockStats) NumFactsPO(predicate uint64, object rpc.KGObject) int {
	return int(m)
}
func (m mockStats) NumFactsO(object rpc.KGObject) int {
	return int(m)
}

func Test_getLatest(t *testing.T) {
	assert := assert.New(t)
	fetcher := NewFetcher(nil)
	assert.Nil(fetcher.getLatest())
	fetcher.latest = mockStats(1)
	close(fetcher.ready)
	assert.Equal(1, fetcher.getLatest().NumFacts())
}

// Tests Last when latest is nil and closed is not ready.
func Test_Last_notReady(t *testing.T) {
	assert := assert.New(t)
	fetcher := NewFetcher(nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	stats, err := fetcher.Last(ctx)
	assert.Nil(stats)
	assert.Error(err)
	assert.Equal(ctx.Err(), err)
}

// Tests Last when latest is set and closed is ready.
func Test_Wait_ready(t *testing.T) {
	assert := assert.New(t)
	fetcher := NewFetcher(nil)
	fetcher.latest = mockStats(1)
	close(fetcher.ready)
	stats, err := fetcher.Last(context.Background())
	assert.Equal(1, stats.NumFacts())
	assert.NoError(err)
}

func Test_Run_ok(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fetch := func(ctx context.Context) (planner.Stats, error) {
		cancel()
		return mockStats(1), nil
	}
	fetcher := NewFetcher(fetch)
	fetcher.Run(ctx)
	stats, err := fetcher.Last(context.Background())
	assert.Equal(1, stats.NumFacts())
	assert.NoError(err)
}

// Tests behavior of Run when fetchStats returns an error.
func Test_Run_error(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up mock clock that advances quickly, so that we don't have to wait a full
	// backoff for the retry.
	mockClock := clocks.NewMock()
	clock = mockClock
	defer func() {
		clock = clocks.Wall
	}()
	go func() {
		for ctx.Err() == nil {
			mockClock.Advance(fetchBackoffEmpty)
			clocks.Wall.SleepUntil(ctx, clocks.Wall.Now().Add(10*time.Microsecond))
		}
	}()

	fetchCount := 0
	fetch := func(ctx context.Context) (planner.Stats, error) {
		fetchCount++
		switch fetchCount {
		case 1:
			return nil, errors.New("ants in pants")
		case 2:
			cancel()
			return mockStats(1), nil
		}
		panic("called too much")
	}
	fetcher := NewFetcher(fetch)
	fetcher.Run(ctx)
	stats, err := fetcher.Last(context.Background())
	assert.Equal(1, stats.NumFacts())
	assert.NoError(err)
	assert.Equal(2, fetchCount)
}
