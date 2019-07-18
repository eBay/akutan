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
	"github.com/ebay/akutan/util/cmp"
)

// ChunkReadyCallback will be called when the LookupSink has ready chunks to pass along
// This signature matches various grpc streaming endpoints
type ChunkReadyCallback func(*LookupChunk) error

// FactSink accumulates facts and sends them to the destination in large chunks.
type FactSink struct {
	// Chunks are sent here.
	dest ChunkReadyCallback
	// The next chunk to send.
	res *LookupChunk
	// When the chunk is considered full and should be flushed
	flushAtSize int
}

// NewFactSink constructs a new LookupSink, once 'flustAtSize' items have been
// accumulated the readyCallback function will be called with the new chunk
func NewFactSink(readyCallback ChunkReadyCallback, flushAtSize int) *FactSink {
	return &FactSink{
		dest:        readyCallback,
		res:         new(LookupChunk),
		flushAtSize: cmp.MaxInt(1, flushAtSize),
	}
}

// Write accumulates the fact to send to the destination. It may also flush a chunk
// of facts. offset is the index into the lookups slice in the request for which
// this fact matches. Write returns nil on success, or an error if flushing
// failed.
func (b *FactSink) Write(offset int, fact Fact) error {
	b.res.Facts = append(b.res.Facts, LookupChunk_Fact{
		Lookup: uint32(offset),
		Fact:   fact,
	})
	if len(b.res.Facts) == b.flushAtSize {
		return b.Flush()
	}
	return nil
}

// Flush sends a chunk of facts to the destination, if needed. It returns nil on
// success, or an error if sending the chunk failed.
func (b *FactSink) Flush() error {
	if len(b.res.Facts) == 0 {
		return nil
	}
	err := b.dest(b.res)
	b.res = new(LookupChunk)
	return err
}
