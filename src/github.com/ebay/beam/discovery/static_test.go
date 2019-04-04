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
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_NewStaticLocator(t *testing.T) {
	assert := assert.New(t)
	l := NewStaticLocator(nil)
	assert.Equal(uint64(0), l.Cached().Version)
	assert.Len(l.Cached().Endpoints, 0)

	l = NewStaticLocator([]*Endpoint{
		{Network: "tcp", Host: "example.com", Port: "8080"},
		{Network: "udp", Host: "127.0.0.1", Port: "7"},
	})
	assert.Equal(uint64(1), l.Cached().Version)
	assert.Len(l.Cached().Endpoints, 2)
}

func Test_StaticLocator_Cached(t *testing.T) {
	assert := assert.New(t)
	l := NewStaticLocator([]*Endpoint{
		{Network: "tcp", Host: "example.com", Port: "8080"},
		{Network: "udp", Host: "127.0.0.1", Port: "7"},
	})
	r := l.Cached()
	assert.Len(r.Endpoints, 2)
	assert.Equal(uint64(1), r.Version)

	l.Set([]*Endpoint{
		{Network: "udp", Host: "127.0.0.1", Port: "7"},
	})
	r = l.Cached()
	assert.Len(r.Endpoints, 1)
	assert.Equal(uint64(2), r.Version)
}

func Test_StaticLocator_WaitForUpdate(t *testing.T) {
	assert := assert.New(t)
	l := NewStaticLocator([]*Endpoint{
		{Network: "tcp", Host: "example.com", Port: "8080"},
	})

	// With oldVersion == 0, WaitForUpdate returns immediately.
	r, err := l.WaitForUpdate(context.Background(), 0)
	assert.NoError(err)
	assert.Len(r.Endpoints, 1)
	assert.Equal(uint64(1), r.Version)

	// With oldVersion > 0 and no calls to Set, WaitForUpdate returns only on
	// ctx.Done.
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	r, err = l.WaitForUpdate(ctx, 1)
	assert.True(time.Since(start) >= time.Millisecond,
		"WaitForUpdate didn't block long enough")
	assert.Error(err)
	assert.Equal(err, ctx.Err())
	assert.Equal(uint64(0), r.Version)

	// After a call to Set, WaitForUpdate returns new data, even using an expired
	// context.
	l.Set([]*Endpoint{
		{Network: "udp", Host: "127.0.0.1", Port: "7"},
	})
	r, err = l.WaitForUpdate(ctx, 1)
	assert.NoError(err)
	assert.Equal(uint64(2), r.Version)
	if assert.Len(r.Endpoints, 1) {
		assert.Equal("127.0.0.1:7", r.Endpoints[0].HostPort())
	}
}

func Test_StaticLocator_WaitForUpdate_wait(t *testing.T) {
	assert := assert.New(t)
	l := NewStaticLocator(nil)
	go func() {
		// Some delay just causes WaitForUpdate to wait.
		time.Sleep(100 * time.Microsecond)
		l.Set([]*Endpoint{
			{Network: "udp", Host: "127.0.0.1", Port: "7"},
		})
	}()
	r, err := l.WaitForUpdate(context.Background(), 0)
	assert.NoError(err)
	if assert.Len(r.Endpoints, 1) {
		assert.Equal("127.0.0.1:7", r.Endpoints[0].HostPort())
	}
}

func Test_StaticLocator_String(t *testing.T) {
	assert := assert.New(t)
	l := NewStaticLocator(nil)
	assert.Equal("empty StaticLocator", l.String())
	l = NewStaticLocator([]*Endpoint{
		{Network: "tcp", Host: "example.com", Port: "8080"},
	})
	assert.Equal("StaticLocator(tcp://example.com:8080)", l.String())
	l = NewStaticLocator([]*Endpoint{
		{Network: "tcp", Host: "example.com", Port: "8080"},
		{Network: "udp", Host: "127.0.0.1", Port: "7"},
	})
	assert.Equal("StaticLocator(len=2, first=tcp://example.com:8080)", l.String())
}
