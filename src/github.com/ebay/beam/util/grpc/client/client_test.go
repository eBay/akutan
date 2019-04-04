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

package grpcclientutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

// This tests that invoking an RPC on InvalidConn will return an error message
// that identifies the connection as being invalid.
func Test_InvalidConn(t *testing.T) {
	assert := assert.New(t)
	conn := InvalidConn("foo")
	defer conn.Close()
	assert.Equal("TRANSIENT_FAILURE", conn.GetState().String())
	err := conn.Invoke(context.Background(), "someMethod", nil, nil)
	if assert.Error(err) {
		assert.EqualError(err, "rpc error: code = Unavailable "+
			"desc = all SubConns are in TransientFailure, "+
			"latest connection error: connection error: "+
			`desc = "transport: Error while dialing intentionally invalid connection (foo)"`)
	}
}

// This tests that invoking an RPC on InvalidConn with an expired context will
// return an error about the context, not about the connection being invalid.
func Test_InvalidConn_ctxExpired(t *testing.T) {
	assert := assert.New(t)
	conn := InvalidConn("foo")
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := conn.Invoke(ctx, "someMethod", nil, nil)
	assert.EqualError(err, "rpc error: code = Canceled desc = context canceled")
}

// This benchmark shows that creating an InvalidConn isn't cheap.
func Benchmark_InvalidConn(b *testing.B) {
	for i := 0; i < b.N; i++ {
		conn := InvalidConn("foo")
		conn.Close()
	}
}

// This test shows that it is possible for grpc.DialContext to return a nil
// connection with a non-nil error.
func Test_grpc_DialContext_mayReturnError(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	conn, err := grpc.DialContext(ctx, "localhost:8080",
		grpc.WithInsecure())
	assert.Equal(ctx.Err(), err)
	if !assert.Nil(conn) {
		conn.Close()
	}
}

// This tests that InsecureDialContext will deal with a context that's already
// expired by returning an InvalidConn, which will result in a good error
// message if later used with a non-expired context.
func Test_InsecureDialContext_ctxExpired(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	conn := InsecureDialContext(ctx, "localhost:8080")
	assert.NotNil(conn)
	defer conn.Close()
	err := conn.Invoke(context.Background(), "someMethod", nil, nil)
	if assert.Error(err) {
		assert.Contains(err.Error(), "(context canceled)")
	}
}
