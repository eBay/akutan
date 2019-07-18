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

package impl

import (
	"context"
	"fmt"
	"testing"

	"github.com/ebay/akutan/api"
	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/logentry"
	"github.com/stretchr/testify/assert"
)

func Test_ResolveIndexConstraint(t *testing.T) {
	type test struct {
		recent        blog.Index
		infoResult    blog.Index
		infoResultErr error
		input         api.LogIndex
		expRecent     blog.Index
		expIndex      blog.Index
		expError      string
		expInfoCalls  int
	}
	infoErr := fmt.Errorf("There was an error fetching Info")
	tests := []test{{
		input:    api.LogIndex{Index: 500, Constraint: api.LogIndexExact},
		expIndex: 500,
	}, {
		recent:       0,
		infoResult:   100,
		input:        api.LogIndex{Constraint: api.LogIndexAtLeast, Index: 50},
		expIndex:     100,
		expRecent:    100,
		expInfoCalls: 1,
	}, {
		recent:       55,
		infoResult:   100,
		input:        api.LogIndex{Constraint: api.LogIndexAtLeast, Index: 50},
		expIndex:     55,
		expRecent:    55,
		expInfoCalls: 0,
	}, {
		recent:       55,
		infoResult:   100,
		input:        api.LogIndex{Constraint: api.LogIndexAtLeast, Index: 60},
		expIndex:     60,
		expRecent:    55,
		expInfoCalls: 0,
	}, {
		recent:       55,
		infoResult:   100,
		input:        api.LogIndex{Constraint: api.LogIndexAtLeast, Index: 0},
		expIndex:     55,
		expRecent:    55,
		expInfoCalls: 0,
	}, {
		recent:       500,
		infoResult:   600,
		input:        api.LogIndex{Constraint: api.LogIndexRecent},
		expIndex:     500,
		expRecent:    500,
		expInfoCalls: 0,
	}, {
		recent:       0,
		infoResult:   600,
		input:        api.LogIndex{Constraint: api.LogIndexRecent},
		expIndex:     600,
		expRecent:    600,
		expInfoCalls: 1,
	}, {
		recent:       999,
		infoResult:   1000,
		input:        api.LogIndex{Constraint: api.LogIndexLatest},
		expIndex:     1000,
		expRecent:    1000,
		expInfoCalls: 1,
	}, {
		input:    api.LogIndex{Constraint: api.LogIndexLatest, Index: 10},
		expError: "a log index can't be specified when using the Latest constraint",
	}, {
		input:    api.LogIndex{Constraint: api.LogIndexRecent, Index: 10},
		expError: "a log index can't be specified when using the Recent constraint",
	}, {
		// a ExactIndex of 0 is valid if the log is actually empty.
		recent:       0,
		infoResult:   0,
		input:        api.LogIndex{Constraint: api.LogIndexExact, Index: 0},
		expIndex:     0,
		expInfoCalls: 1,
	}, {
		recent:       0,
		infoResult:   10,
		input:        api.LogIndex{Constraint: api.LogIndexExact, Index: 0},
		expError:     "a log index constraint of Exact requires a log index to be set",
		expInfoCalls: 1,
	}, {
		input:         api.LogIndex{Constraint: api.LogIndexExact, Index: 11},
		infoResultErr: infoErr,
		expIndex:      11,
	}, {
		recent:        50,
		input:         api.LogIndex{Constraint: api.LogIndexAtLeast, Index: 12},
		infoResultErr: infoErr,
		expIndex:      50,
	}, {
		recent:        5,
		input:         api.LogIndex{Constraint: api.LogIndexAtLeast, Index: 13},
		infoResultErr: infoErr,
		expIndex:      13,
	}, {
		recent:        0,
		input:         api.LogIndex{Constraint: api.LogIndexAtLeast, Index: 14},
		infoResultErr: infoErr,
		expError:      infoErr.Error(),
		expInfoCalls:  1,
	}, {
		recent:        5,
		input:         api.LogIndex{Constraint: api.LogIndexLatest},
		infoResultErr: infoErr,
		expError:      infoErr.Error(),
		expInfoCalls:  1,
	}, {
		input:    api.LogIndex{Constraint: api.LogIndexConstraint(42)},
		expError: "LogIndex constraint of 42 is not supported",
	}}
	for _, test := range tests {
		t.Run(test.input.String(), func(t *testing.T) {
			s := Server{}
			s.locked.recentIndex = test.recent
			log := &mockInfo{latestIndex: test.infoResult, err: test.infoResultErr}
			resIndex, resErr := s.resolveIndexConstraint(context.Background(), log, test.input)
			if test.expError != "" {
				assert.EqualError(t, resErr, test.expError)
			} else {
				assert.NoError(t, resErr)
				assert.Equal(t, test.expIndex, resIndex, "returned Index not correct")
			}
			if test.expRecent != 0 {
				assert.Equal(t, s.locked.recentIndex, test.expRecent, "Resulting value for Recent unexpected")
			}
			assert.Equal(t, test.expInfoCalls, log.infoCalls, "Unexpected number of calls to log.Info()")
		})
	}
}

func Test_FetchLatestLogIndex(t *testing.T) {
	type test struct {
		recent        blog.Index
		infoResult    blog.Index
		infoResultErr error
		expResult     blog.Index
		expErr        string
		expRecent     blog.Index
	}
	tests := []test{{
		recent:     10,
		infoResult: 20,
		expResult:  20,
		expRecent:  20,
	}, {
		recent:     11,
		infoResult: 10,
		expResult:  10,
		expRecent:  11,
	}, {
		recent:        10,
		infoResultErr: fmt.Errorf("Failed to reach log server"),
		expErr:        "Failed to reach log server",
		expRecent:     10,
	}}
	for _, test := range tests {
		s := Server{}
		s.locked.recentIndex = test.recent
		log := &mockInfo{latestIndex: test.infoResult, err: test.infoResultErr}
		res, err := s.fetchLatestLogIndex(context.Background(), log)
		if test.expErr != "" {
			assert.EqualError(t, err, test.expErr)
			continue
		}
		assert.Equal(t, test.expResult, res, "Unexpected result value returned")
		assert.Equal(t, test.expRecent, s.locked.recentIndex, "recent value has unexpected value")
	}
}

type mockInfo struct {
	latestIndex blog.Index
	err         error
	infoCalls   int
}

func (m *mockInfo) Info(context.Context) (*blog.Info, error) {
	m.infoCalls++
	if m.err != nil {
		return nil, m.err
	}
	return &blog.Info{
		LastIndex: m.latestIndex,
	}, nil
}

func Test_TimestampPrecisionSame(t *testing.T) {
	assert.Equal(t, logentry.TimestampPrecision_value, api.Precision_value, "API & Logentry defintions are expected to have the same names/values for Precision")
}
