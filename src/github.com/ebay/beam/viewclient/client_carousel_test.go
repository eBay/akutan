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

package viewclient

import (
	"errors"
	"testing"

	"github.com/ebay/beam/blog"
	"github.com/ebay/beam/logentry/logencoder"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/space"
	"github.com/stretchr/testify/assert"
)

func logPos(index blog.Index, version logencoder.Version) rpc.LogPosition {
	return rpc.LogPosition{
		Index:   index,
		Version: version,
	}
}

func Test_LogStatus_ReplayPosition(t *testing.T) {
	v4 := newViews(4)
	v3 := newViews(3)
	r := LogStatusResult{
		viewResults: []*rpc.LogStatusResult{
			{ReplayPosition: logPos(100, 3), KeyEncoding: rpc.KeyEncodingSPO},
			{ReplayPosition: logPos(100, 3), KeyEncoding: rpc.KeyEncodingSPO},
			{ReplayPosition: logPos(100, 3), KeyEncoding: rpc.KeyEncodingSPO},
			{ReplayPosition: logPos(100, 3), KeyEncoding: rpc.KeyEncodingSPO},
			{ReplayPosition: logPos(99, 2), KeyEncoding: rpc.KeyEncodingSPO},
			{ReplayPosition: logPos(99, 2), KeyEncoding: rpc.KeyEncodingSPO},
			{ReplayPosition: logPos(99, 2), KeyEncoding: rpc.KeyEncodingSPO},
		},
		viewErrors: make([]error, 7),
	}
	r.processResults(flatten(v4, v3))
	exp := viewLogStates{
		{covers: v4[0].Serves(), replayPos: logPos(100, 3)},
		{covers: v4[1].Serves(), replayPos: logPos(100, 3)},
		{covers: v4[2].Serves(), replayPos: logPos(100, 3)},
		{covers: v4[3].Serves(), replayPos: logPos(100, 3)},
		{covers: v3[0].Serves(), replayPos: logPos(99, 2)},
		{covers: v3[1].Serves(), replayPos: logPos(99, 2)},
		{covers: v3[2].Serves(), replayPos: logPos(99, 2)},
	}
	assert.Equal(t, exp, r.byEncoding[rpc.KeyEncodingSPO])
	assertPosInSpace(t, logPos(99, 2), nil, &r, rpc.KeyEncodingSPO, fullRange)
	assertPosInSpace(t, rpc.LogPosition{},
		errors.New("no view found to cover partition range 0x0000000000000000-∞ in KeyEncodingPOS"),
		&r, rpc.KeyEncodingPOS, fullRange)

	// Check for detailed error message.
	r.viewErrors[3] = errors.New("ants in pants")
	assertPosInSpace(t, rpc.LogPosition{},
		errors.New("no view found to cover partition range 0x0000000000000000-∞ in KeyEncodingPOS. "+
			"Some results missing (sample error: ants in pants)"),
		&r, rpc.KeyEncodingPOS, fullRange)
	r.viewErrors[3] = nil

	// if one of the views is further behind, and thats part of the requested range, it should get applied
	r.viewResults[0].ReplayPosition = logPos(50, 1)
	r.processResults(flatten(v4, v3))
	assertPosInSpace(t, logPos(50, 1), nil, &r, rpc.KeyEncodingSPO, fullRange)
	assertPosInSpace(t, logPos(50, 1), nil, &r, rpc.KeyEncodingSPO, v3[0].Serves())
	assertPosInSpace(t, logPos(99, 2), nil, &r, rpc.KeyEncodingSPO, v3[2].Serves())

	// if there are more than 2 views for a given partition, it should discard the laggard
	v2 := newViews(2)
	r.viewResults = append(r.viewResults,
		&rpc.LogStatusResult{ReplayPosition: logPos(40, 1), KeyEncoding: rpc.KeyEncodingSPO},
		&rpc.LogStatusResult{ReplayPosition: logPos(110, 1), KeyEncoding: rpc.KeyEncodingSPO},
	)
	r.processResults(flatten(v4, v3, v2))
	assertPosInSpace(t, logPos(50, 1), nil, &r, rpc.KeyEncodingSPO, fullRange)
	assertPosInSpace(t, logPos(50, 1), nil, &r, rpc.KeyEncodingSPO, v3[0].Serves())
	assertPosInSpace(t, logPos(100, 3), nil, &r, rpc.KeyEncodingSPO, v3[2].Serves())
}

func assertPosInSpace(t *testing.T, exp rpc.LogPosition, expErr error, ls *LogStatusResult, enc rpc.FactKeyEncoding, rng space.Range) {
	t.Helper()
	actual, err := ls.ReplayPositionInSpace(enc, rng)
	assert.Equal(t, exp, actual)
	assert.Equal(t, expErr, err)
}

func assertNextPositionInSpace(t *testing.T, exp rpc.LogPosition, expErr error, ls *LogStatusResult, enc rpc.FactKeyEncoding, rng space.Range) {
	t.Helper()
	actual, err := ls.NextPositionInSpace(enc, rng)
	assert.Equal(t, exp, actual)
	assert.Equal(t, expErr, err)
}

func Test_LogStatus_MixedEncodings(t *testing.T) {
	v2 := newViews(2)
	v3 := newViews(3)
	r := LogStatusResult{
		viewResults: []*rpc.LogStatusResult{
			{ReplayPosition: logPos(90, 3), KeyEncoding: rpc.KeyEncodingSPO},
			{ReplayPosition: logPos(91, 3), KeyEncoding: rpc.KeyEncodingSPO},
			{ReplayPosition: logPos(92, 3), KeyEncoding: rpc.KeyEncodingPOS},
			{ReplayPosition: logPos(93, 3), KeyEncoding: rpc.KeyEncodingPOS},
			{ReplayPosition: logPos(94, 3), KeyEncoding: rpc.KeyEncodingPOS},
		},
	}
	r.processResults(flatten(v2, v3))
	assertPosInSpace(t, logPos(90, 3), nil, &r, rpc.KeyEncodingSPO, fullRange)
	assertPosInSpace(t, logPos(92, 3), nil, &r, rpc.KeyEncodingPOS, fullRange)
	assertPosInSpace(t, logPos(93, 3), nil, &r, rpc.KeyEncodingPOS, pctRange(45, 55))
	assertPosInSpace(t, logPos(93, 3), nil, &r, rpc.KeyEncodingPOS, pctRange(45, 70))
	assertPosInSpace(t, logPos(94, 3), nil, &r, rpc.KeyEncodingPOS, pctRange(70, 90))
	pos, err := r.ReplayPosition()
	assert.NoError(t, err)
	assert.Equal(t, logPos(90, 3), pos)
}

func Test_LogStatus_MissingResults(t *testing.T) {
	v4 := newViews(4)
	r := LogStatusResult{
		viewResults: []*rpc.LogStatusResult{
			{ReplayPosition: logPos(50, 2), KeyEncoding: rpc.KeyEncodingPOS},
			{ReplayPosition: logPos(51, 2), KeyEncoding: rpc.KeyEncodingPOS},
			nil,
			{ReplayPosition: logPos(53, 2), KeyEncoding: rpc.KeyEncodingPOS},
		},
	}
	r.processResults(flatten(v4))
	errMissingView := errors.New("no view found to cover partition range 0x8000000000000000-0xC000000000000000 in KeyEncodingPOS")
	assertPosInSpace(t, logPos(0, 0), errMissingView, &r, rpc.KeyEncodingPOS, fullRange)
	assertPosInSpace(t, logPos(50, 2), nil, &r, rpc.KeyEncodingPOS, pctRange(20, 40))
	assertPosInSpace(t, logPos(53, 2), nil, &r, rpc.KeyEncodingPOS, pctRange(80, 90))
	assertPosInSpace(t, logPos(0, 0), errMissingView, &r, rpc.KeyEncodingPOS, pctRange(45, 51))
	assertPosInSpace(t, logPos(0, 0), errMissingView, &r, rpc.KeyEncodingPOS, pctRange(74, 80))
}

func Test_LogStatus_LastApplied(t *testing.T) {
	v4 := newViews(4)
	v3 := newViews(3)
	r := LogStatusResult{
		viewResults: []*rpc.LogStatusResult{
			{NextPosition: logPos(100, 1), KeyEncoding: rpc.KeyEncodingSPO},
			{NextPosition: logPos(101, 1), KeyEncoding: rpc.KeyEncodingSPO},
			{NextPosition: logPos(102, 1), KeyEncoding: rpc.KeyEncodingSPO},
			{NextPosition: logPos(103, 1), KeyEncoding: rpc.KeyEncodingSPO},
			{NextPosition: logPos(97, 1), KeyEncoding: rpc.KeyEncodingSPO},
			{NextPosition: logPos(98, 1), KeyEncoding: rpc.KeyEncodingSPO},
			{NextPosition: logPos(99, 1), KeyEncoding: rpc.KeyEncodingSPO},
		},
	}
	r.processResults(flatten(v4, v3))
	assertNextPositionInSpace(t, logPos(100, 1), nil, &r, rpc.KeyEncodingSPO, fullRange)
	assertNextPositionInSpace(t, logPos(100, 1), nil, &r, rpc.KeyEncodingSPO, pctRange(10, 20))
	assertNextPositionInSpace(t, logPos(102, 1), nil, &r, rpc.KeyEncodingSPO, pctRange(60, 65))
	assertNextPositionInSpace(t, logPos(103, 1), nil, &r, rpc.KeyEncodingSPO, pctRange(90, 95))
	_, err := r.LastApplied()
	assert.EqualError(t, err, "no view found to cover partition range 0x0000000000000000-∞ in KeyEncodingPOS")

	v1 := newViews(1)
	r.viewResults = append(r.viewResults, &rpc.LogStatusResult{
		NextPosition: logPos(95, 1), KeyEncoding: rpc.KeyEncodingPOS,
	})
	r.processResults(flatten(v4, v3, v1))
	lastApplied, err := r.LastApplied()
	assert.Equal(t, uint64(94), lastApplied)
	assert.NoError(t, err)
}

func Test_LogStatus_LastApplied_MissingResults(t *testing.T) {
	v3 := newViews(3)
	r := LogStatusResult{
		viewResults: []*rpc.LogStatusResult{
			{NextPosition: logPos(123, 1), KeyEncoding: rpc.KeyEncodingPOS},
			nil,
			{NextPosition: logPos(130, 1), KeyEncoding: rpc.KeyEncodingPOS},
		},
	}
	r.processResults(flatten(v3))
	errMissing := errors.New("no view found to cover partition range 0x5555555555555556-0xAAAAAAAAAAAAAAAB in KeyEncodingPOS")
	assertNextPositionInSpace(t, logPos(0, 0), errMissing, &r, rpc.KeyEncodingPOS, fullRange)
	assertNextPositionInSpace(t, logPos(0, 0), errMissing, &r, rpc.KeyEncodingPOS, pctRange(50, 55))
	assertNextPositionInSpace(t, logPos(0, 0), errMissing, &r, rpc.KeyEncodingPOS, pctRange(20, 35))
	assertNextPositionInSpace(t, logPos(123, 1), nil, &r, rpc.KeyEncodingPOS, pctRange(10, 22))
	assertNextPositionInSpace(t, logPos(130, 1), nil, &r, rpc.KeyEncodingPOS, pctRange(70, 80))
}

func Test_EncodingWithResultsThatsNot(t *testing.T) {
	r := LogStatusResult{
		viewResults: []*rpc.LogStatusResult{
			{ReplayPosition: logPos(50, 2), KeyEncoding: rpc.KeyEncodingPOS},
			{ReplayPosition: logPos(50, 2), KeyEncoding: rpc.KeyEncodingSPO},
		},
	}
	views := newViews(2)
	r.processResults(flatten(views))
	assert.Equal(t, rpc.KeyEncodingPOS, r.encodingWithResultsThatsNot(rpc.KeyEncodingSPO))
	assert.Equal(t, rpc.KeyEncodingSPO, r.encodingWithResultsThatsNot(rpc.KeyEncodingPOS))

	rPOS := r
	rPOS.viewResults = rPOS.viewResults[:1]
	rPOS.processResults(flatten(views))
	assert.Equal(t, rpc.KeyEncodingPOS, rPOS.encodingWithResultsThatsNot(rpc.KeyEncodingSPO))
	assert.Equal(t, rpc.KeyEncodingUnknown, rPOS.encodingWithResultsThatsNot(rpc.KeyEncodingPOS))

	rSPO := r
	rSPO.viewResults = rSPO.viewResults[1:]
	rSPO.processResults(flatten(views))
	assert.Equal(t, rpc.KeyEncodingUnknown, rSPO.encodingWithResultsThatsNot(rpc.KeyEncodingSPO))
	assert.Equal(t, rpc.KeyEncodingSPO, rSPO.encodingWithResultsThatsNot(rpc.KeyEncodingPOS))

	rNone := LogStatusResult{}
	rNone.processResults(flatten(views))
	assert.Equal(t, rpc.KeyEncodingUnknown, rNone.encodingWithResultsThatsNot(rpc.KeyEncodingSPO))
	assert.Equal(t, rpc.KeyEncodingUnknown, rNone.encodingWithResultsThatsNot(rpc.KeyEncodingPOS))
}
