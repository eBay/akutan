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

// Package mocklookups provides a mock implementation of the various Fact lookup
// RPCs. This is useful for unit testing.
package mocklookups

import (
	"context"
	"errors"
	"sync"

	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/viewclient/lookups"
	"github.com/stretchr/testify/assert"
)

// LookupRequest is an interface that is satisfied by *rpc.LookupPORequest,
// *rpc.LookupPOCmpRequest, etc.
//
// Note: Additional types that aren't lookup requests may happen to satisfy this
// interface, but using those with Mock will typically lead to assertion
// failures.
type LookupRequest interface {
	ProtoMessage()
	String() string
	GetIndex() uint64
}

// These types must implement LookupRequest.
var _ = []LookupRequest{
	(*rpc.LookupPORequest)(nil),
	(*rpc.LookupPOCmpRequest)(nil),
	(*rpc.LookupSRequest)(nil),
	(*rpc.LookupSPRequest)(nil),
	(*rpc.LookupSPORequest)(nil),
}

// Expected describes what Mock should do when it receives a single lookup
// request.
type Expected struct {
	// If non-nil, a pointer to the expected request. If nil, the actual request is
	// not checked (any request is acceptable).
	Request LookupRequest
	// Responses to send on the channel, in order.
	Replies []rpc.LookupChunk
	// If set, this will be returned by the lookup function after the replies
	// are sent.
	ReplyErr error
}

// OK is a convenience function for constructing Expected values. It returns a
// descriptor that expects the given request and sends the given replies to it.
// 'req' may be nil to indicate that any request is acceptable.
func OK(req LookupRequest, replies ...rpc.LookupChunk) Expected {
	return Expected{
		Request: req,
		Replies: replies,
	}
}

// Err is a convenience function for constructing Expected values. It returns a
// descriptor that expects the given request, sends it nothing, and returns the
// given error from the lookup function. 'req' may be nil to indicate that any
// request is acceptable.
func Err(req LookupRequest, replyErr error) Expected {
	return Expected{
		Request:  req,
		ReplyErr: replyErr,
	}
}

// Mock is a combined client and server that implements lookups.All. Mock is
// thread-safe.
type Mock struct {
	t        assert.TestingT
	lock     sync.Mutex
	expected []Expected
}

// Ensures that Mock implements lookups.All.
var _ lookups.All = (*Mock)(nil)

// New constructs a combined client and server. It also returns a function that
// should be called before tearing down the test, which asserts that the Mock
// received all of the requests it expected. The given 't' is typically a
// *testing.T. The given 'expected' values instruct the Mock on how to behave
// when it gets its first lookup requests (in FIFO order).
func New(t assert.TestingT, expected ...Expected) (*Mock, func()) {
	mock := &Mock{t: t, expected: expected}
	assertDone := func() {
		mock.lock.Lock()
		assert.Equal(t, 0, len(mock.expected), "More requests expected")
		mock.lock.Unlock()
	}
	return mock, assertDone
}

// Expect instructs the Mock on how to behave when it gets additional lookup
// requests (in FIFO order).
func (mock *Mock) Expect(exp ...Expected) {
	mock.lock.Lock()
	mock.expected = append(mock.expected, exp...)
	mock.lock.Unlock()
}

// LookupPO implements lookups.PO.
func (mock *Mock) LookupPO(ctx context.Context, req *rpc.LookupPORequest, resCh chan *rpc.LookupChunk) error {
	return mock.lookup(ctx, req, resCh)
}

// LookupPOCmp implements lookups.POCmp.
func (mock *Mock) LookupPOCmp(ctx context.Context, req *rpc.LookupPOCmpRequest, resCh chan *rpc.LookupChunk) error {
	return mock.lookup(ctx, req, resCh)
}

// LookupS implements lookups.S.
func (mock *Mock) LookupS(ctx context.Context, req *rpc.LookupSRequest, resCh chan *rpc.LookupChunk) error {
	return mock.lookup(ctx, req, resCh)
}

// LookupSP implements lookups.SP.
func (mock *Mock) LookupSP(ctx context.Context, req *rpc.LookupSPRequest, resCh chan *rpc.LookupChunk) error {
	return mock.lookup(ctx, req, resCh)
}

// LookupSPO implements lookups.SPO.
func (mock *Mock) LookupSPO(ctx context.Context, req *rpc.LookupSPORequest, resCh chan *rpc.LookupChunk) error {
	return mock.lookup(ctx, req, resCh)
}

// lookup is called to make a lookup request. The Mock asserts that the 'req'
// matches its next Expected request, and returns results accordingly.
func (mock *Mock) lookup(ctx context.Context, req LookupRequest, resCh chan *rpc.LookupChunk) error {
	defer close(resCh)
	var expected Expected
	mock.lock.Lock()
	ok := len(mock.expected) > 0
	if ok {
		expected, mock.expected = mock.expected[0], mock.expected[1:]
	}
	mock.lock.Unlock()

	if !ok {
		assert.Fail(mock.t, "Unexpected request", "req: %+v", req)
		return errors.New("unexpected request")
	}
	if expected.Request != nil {
		assert.Equal(mock.t, expected.Request.String(), req.String(),
			"Actual request did not match expected")
	}
	for i := range expected.Replies {
		// Note: This ranges over the index rather than the values to avoid
		// exposing a pointer to a loop iterator variable. (Go mutates loop
		// iterator variables in each iteration.)
		resCh <- &expected.Replies[i]
	}
	return expected.ReplyErr
}
