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

package debug

import (
	"testing"
	"time"

	"github.com/ebay/beam/query/exec"
	"github.com/ebay/beam/query/planner/plandef"
	"github.com/stretchr/testify/assert"
)

func Test_OpTotals_String(t *testing.T) {
	opJoin := &plandef.HashJoin{}
	opInfer := &plandef.InferPO{}

	empty := opTotals(opJoin, nil)
	assert.Equal(t, "[not executed]", empty)

	events := []exec.OpCompletedEvent{{
		Operator:       opJoin,
		InputBulkCount: 5,
		StartedAt:      time.Date(2018, 1, 1, 13, 0, 0, 0, time.UTC),
		EndedAt:        time.Date(2018, 1, 1, 13, 0, 1, 0, time.UTC),
		Output: exec.StreamStats{
			NumChunks:   2,
			NumFactSets: 10,
		},
	}, {
		Operator:       opJoin,
		InputBulkCount: 1,
		StartedAt:      time.Date(2018, 1, 1, 14, 0, 0, 0, time.UTC),
		EndedAt:        time.Date(2018, 1, 1, 14, 0, 2, 0, time.UTC),
		Output: exec.StreamStats{
			NumChunks:   20,
			NumFactSets: 1000,
		},
	}, {
		Operator:       opInfer,
		InputBulkCount: 5,
		StartedAt:      time.Date(2018, 1, 1, 14, 0, 2, 0, time.UTC),
		EndedAt:        time.Date(2018, 1, 1, 14, 0, 3, 0, time.UTC),
		Output: exec.StreamStats{
			NumChunks:   20,
			NumFactSets: 1000,
		},
	}}

	inferStats := opTotals(opInfer, events)
	assert.Equal(t, "execs:   1 | totals: | input rows:   5 | out chunks:  20 | out factsets:  1000 | took     1s", inferStats)

	joinStats := opTotals(opJoin, events)
	assert.Equal(t, "execs:   2 | totals: | input rows:   6 | out chunks:  22 | out factsets:  1010 | took     3s (avg exec 1.5s)", joinStats)
}
