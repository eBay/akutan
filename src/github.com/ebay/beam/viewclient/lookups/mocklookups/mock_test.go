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

package mocklookups

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ebay/beam/rpc"
	"github.com/stretchr/testify/assert"
)

func Test_Mock_lookup_unexpected(t *testing.T) {
	assertions := &mockAssert{}
	mock, assertDone := New(assertions)
	ch := make(chan *rpc.LookupChunk, 4)
	err := mock.LookupSP(context.Background(), &rpc.LookupSPRequest{}, ch)
	assert.Error(t, err)
	if assert.Len(t, assertions.log, 1) {
		assert.Contains(t, assertions.log[0], "Unexpected request")
	}
	assertDone()
}

func Test_Mock_lookup_nil_empty(t *testing.T) {
	assertions := &mockAssert{}
	mock, assertDone := New(assertions, OK(nil))
	ch := make(chan *rpc.LookupChunk, 4)
	err := mock.LookupSP(context.Background(), &rpc.LookupSPRequest{}, ch)
	assert.NoError(t, err)
	assert.Len(t, assertions.log, 0)
	assertDone()
}

func Test_Mock_lookup_mismatch(t *testing.T) {
	assertions := &mockAssert{}
	mock, assertDone := New(assertions,
		OK(&rpc.LookupSPRequest{
			Index: 3,
		}))
	ch := make(chan *rpc.LookupChunk, 4)
	err := mock.LookupSP(context.Background(), &rpc.LookupSPRequest{}, ch)
	assert.NoError(t, err)
	if assert.Len(t, assertions.log, 1) {
		assert.Contains(t, assertions.log[0], "did not match")
	}
	assertDone()
}

func Test_Mock_lookup_repliesAndError(t *testing.T) {
	assertions := &mockAssert{}
	mock, assertDone := New(assertions)
	mock.Expect(Expected{
		Request: nil,
		Replies: []rpc.LookupChunk{
			{Facts: []rpc.LookupChunk_Fact{{Lookup: 1}}},
			{Facts: []rpc.LookupChunk_Fact{{Lookup: 3}}},
		},
		ReplyErr: errors.New("ants in pants"),
	})
	ch := make(chan *rpc.LookupChunk, 4)
	err := mock.LookupSP(context.Background(), &rpc.LookupSPRequest{}, ch)
	assert.Error(t, err, "ants in pants")
	assert.Len(t, assertions.log, 0)
	assert.Len(t, ch, 2)
	assertDone()
	chunk1 := <-ch
	chunk2 := <-ch
	assert.Equal(t, uint32(1), chunk1.Facts[0].Lookup)
	assert.Equal(t, uint32(3), chunk2.Facts[0].Lookup)
}

// mockAssert implements assert.TestingT.
type mockAssert struct {
	log []string
}

func (t *mockAssert) Errorf(format string, args ...interface{}) {
	t.log = append(t.log, fmt.Sprintf(format, args...))
}
