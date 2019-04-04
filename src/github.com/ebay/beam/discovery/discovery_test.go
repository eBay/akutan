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

package discovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

var endpointTests = []struct {
	e             Endpoint
	stringer      string
	hostPort      string
	hostPortPanic string
}{
	{
		e:        Endpoint{Network: "tcp", Host: "example.com", Port: "80"},
		stringer: "tcp://example.com:80",
		hostPort: "example.com:80",
	},
	{
		e:             Endpoint{Host: "example.com", Port: ""},
		stringer:      "???://example.com:???",
		hostPortPanic: "HostPort called on discovery.Endpoint with no port (host example.com)",
	},
	{
		e:        Endpoint{Network: "udp", Host: "127.0.0.1", Port: "echo"},
		stringer: "udp://127.0.0.1:echo",
		hostPort: "127.0.0.1:echo",
	},
	{
		e:        Endpoint{Host: "2001:0db8:85a3:0000:0000:8a2e:0370:7334", Port: "8080"},
		stringer: "???://[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:8080",
		hostPort: "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:8080",
	},
}

func Test_Endpoint_HostPort(t *testing.T) {
	for i, test := range endpointTests {
		if test.hostPortPanic == "" {
			assert.Equal(t, test.hostPort, test.e.HostPort(),
				"test %v endpoint %#v", i, test.e)
		} else {
			assert.PanicsWithValue(t, test.hostPortPanic,
				func() { test.e.HostPort() },
				"test %v endpoint %#v", i, test.e)
		}
	}
}

func Test_Endpoint_String(t *testing.T) {
	for i, test := range endpointTests {
		assert.Equal(t, test.stringer, test.e.String(),
			"test %v endpoint %#v", i, test.e)
	}
}

func Test_GetNonempty(t *testing.T) {
	assert := assert.New(t)

	// A suitable result gets returned.
	l := NewStaticLocator([]*Endpoint{
		{Host: "example.com", Port: "80"},
	})
	r, err := GetNonempty(context.Background(), l)
	assert.NoError(err)
	assert.Len(r.Endpoints, 1)
	assert.Equal(uint64(1), r.Version)

	// Context error gets returned.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err = GetNonempty(ctx, NewStaticLocator(nil))
	assert.Error(err)
	assert.Equal(err, ctx.Err())

	// The for loop calls back into the locator repeatedly until getting a nonempty
	// result.
	r, err = GetNonempty(context.Background(), &slowLocator{})
	assert.NoError(err)
	assert.Len(r.Endpoints, 1)
	assert.Equal(uint64(20), r.Version)
}

// slowLocator is used in Test_GetNonempty.
type slowLocator struct {
	reqs uint64
}

func (l *slowLocator) Cached() Result {
	panic("not implemented")
}

// Immediately returns 0 endpoints for the first 19 calls, then subsequently
// returns 1 result.
func (l *slowLocator) WaitForUpdate(ctx context.Context, oldVersion uint64) (Result, error) {
	l.reqs++
	res := Result{
		Version: l.reqs,
	}
	if l.reqs >= 20 {
		res.Endpoints = []*Endpoint{{Host: "example.com", Port: "80"}}
	}
	return res, nil
}

func (l *slowLocator) String() string {
	return "slowLocator"
}
