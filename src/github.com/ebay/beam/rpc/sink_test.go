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

package rpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const lookupChunkSize = 200

func Test_LookupSink_emptyFlush(t *testing.T) {
	assert := assert.New(t)
	stream := new(MockChunkStream)
	sink := NewFactSink(stream.Send, lookupChunkSize)
	assert.NoError(sink.Flush())
	assert.Len(stream.chunks, 0)
}

func Test_LookupSink_sameOffset(t *testing.T) {
	assert := assert.New(t)
	stream := new(MockChunkStream)
	sink := NewFactSink(stream.Send, lookupChunkSize)
	offset := 0
	for i := 0; i < lookupChunkSize*2-1; i++ {
		err := sink.Write(offset, Fact{Id: uint64(i)})
		assert.NoError(err)
	}
	assert.Len(stream.chunks, 1)
	err := sink.Write(0, Fact{Id: lookupChunkSize*2 - 1})
	assert.NoError(err)
	assert.Len(stream.chunks, 2)
	err = sink.Flush()
	assert.NoError(err)
	assert.Len(stream.chunks, 2)
	err = sink.Write(0, Fact{Id: lookupChunkSize * 2})
	assert.NoError(err)
	err = sink.Flush()
	assert.NoError(err)
	if assert.Len(stream.chunks, 3) {
		chunk1 := stream.chunks[0]
		chunk2 := stream.chunks[1]
		chunk3 := stream.chunks[2]
		assert.Len(chunk1.Facts, lookupChunkSize)
		assert.Equal(uint32(offset), chunk1.Facts[0].Lookup)
		assert.Len(chunk2.Facts, lookupChunkSize)
		assert.Equal(uint32(offset), chunk2.Facts[0].Lookup)
		assert.Len(chunk3.Facts, 1)
		assert.Equal(uint32(offset), chunk3.Facts[0].Lookup)
		for i, fact := range stream.Flatten().Facts {
			assert.Equal(uint64(i), fact.Fact.Id)
		}
	}
}

func Test_LookupSink_differentOffsets(t *testing.T) {
	assert := assert.New(t)
	stream := new(MockChunkStream)
	sink := NewFactSink(stream.Send, lookupChunkSize)
	assert.NoError(sink.Write(3, Fact{Id: 11}))
	assert.NoError(sink.Write(3, Fact{Id: 12}))
	assert.NoError(sink.Write(4, Fact{Id: 13}))
	assert.NoError(sink.Write(9, Fact{Id: 14}))
	assert.NoError(sink.Flush())
	assert.NoError(sink.Write(9, Fact{Id: 15}))
	assert.NoError(sink.Flush())
	if assert.Len(stream.chunks, 2) {
		chunk1 := stream.chunks[0]
		if assert.Len(chunk1.Facts, 4) {
			assert.EqualValues(3, chunk1.Facts[0].Lookup)
			assert.EqualValues(11, chunk1.Facts[0].Fact.Id)
			assert.EqualValues(3, chunk1.Facts[1].Lookup)
			assert.EqualValues(12, chunk1.Facts[1].Fact.Id)
			assert.EqualValues(4, chunk1.Facts[2].Lookup)
			assert.EqualValues(13, chunk1.Facts[2].Fact.Id)
			assert.EqualValues(9, chunk1.Facts[3].Lookup)
			assert.EqualValues(14, chunk1.Facts[3].Fact.Id)
		}
		chunk2 := stream.chunks[1]
		if assert.Len(chunk2.Facts, 1) {
			assert.EqualValues(9, chunk2.Facts[0].Lookup)
			assert.EqualValues(15, chunk2.Facts[0].Fact.Id)
		}
	}
}
