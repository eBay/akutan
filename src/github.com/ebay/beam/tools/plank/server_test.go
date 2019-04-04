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

package main

import (
	"context"
	"testing"

	"github.com/ebay/beam/logspec"
	"github.com/stretchr/testify/assert"
)

func testServer() *Server {
	server := &Server{
		ctx:     context.Background(),
		options: &Options{},
	}
	server.locked.startIndex = 1
	server.locked.changed = make(chan struct{})
	return server
}

func Test_append(t *testing.T) {
	assert := assert.New(t)
	server := testServer()

	indexes := server.append(nil)
	assert.Equal([]uint64{}, indexes)
	assert.Equal(uint64(1), server.locked.startIndex)
	assert.Equal(uint64(0), server.lastIndexLocked())

	indexes = server.append([][]byte{[]byte("hello"), []byte("world")})
	assert.Equal([]uint64{1, 2}, indexes)
	assert.Equal(uint64(1), server.locked.startIndex)
	assert.Equal(uint64(2), server.lastIndexLocked())
}

func Test_append_autodiscard(t *testing.T) {
	assert := assert.New(t)
	server := testServer()
	server.options.Limit = 3
	server.hardLimit = 5

	indexes := server.append([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")})
	assert.Equal([]uint64{1, 2, 3, 4}, indexes)
	assert.Equal(uint64(1), server.locked.startIndex)
	assert.Equal(uint64(4), server.lastIndexLocked())
	assert.Equal("a", string(server.locked.entries[0].Data))

	indexes = server.append([][]byte{[]byte("e")})
	assert.Equal([]uint64{5}, indexes)
	assert.Equal(uint64(3), server.locked.startIndex)
	assert.Equal(uint64(5), server.lastIndexLocked())
	assert.Equal("c", string(server.locked.entries[0].Data))

	indexes = server.append([][]byte{[]byte("f"), []byte("g")})
	assert.Equal([]uint64{6, 7}, indexes)
	assert.Equal(uint64(5), server.locked.startIndex)
	assert.Equal(uint64(7), server.lastIndexLocked())
	assert.Equal("e", string(server.locked.entries[0].Data))
}

func Test_discard(t *testing.T) {
	assert := assert.New(t)
	server := testServer()
	server.discard(11)
	server.discard(8)
	assert.Equal(uint64(11), server.locked.startIndex)
	assert.Equal(uint64(10), server.lastIndexLocked())

	_ = server.append([][]byte{[]byte("hello"), []byte("world")})
	assert.Equal(uint64(11), server.locked.startIndex)
	assert.Equal(uint64(12), server.lastIndexLocked())
	assert.Equal("hello", string(server.locked.entries[0].Data))

	server.discard(12)
	assert.Equal(uint64(12), server.locked.startIndex)
	assert.Equal(uint64(12), server.lastIndexLocked())
	assert.Equal("world", string(server.locked.entries[0].Data))

	server.discard(13)
	assert.Equal(uint64(13), server.locked.startIndex)
	assert.Equal(uint64(12), server.lastIndexLocked())

	server.discard(500)
	assert.Equal(uint64(500), server.locked.startIndex)
	assert.Equal(uint64(499), server.lastIndexLocked())
}

func Test_info(t *testing.T) {
	assert := assert.New(t)
	server := testServer()
	server.discard(20)
	_ = server.append([][]byte{[]byte("hello"), []byte("world")})
	res, _ := server.info()
	ok := res.Reply.(*logspec.InfoReply_Ok).Ok
	assert.Equal(uint64(20), ok.FirstIndex)
	assert.Equal(uint64(21), ok.LastIndex)
}
