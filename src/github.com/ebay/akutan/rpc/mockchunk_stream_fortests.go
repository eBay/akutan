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

// MockChunkStream is designed to be used as a LookupChunkReadyCallback to collect all chunks
// flushed out of the LookupSink into a set. This is for writing tests.
type MockChunkStream struct {
	// All chunks sent to the stream.
	chunks []*LookupChunk
	// Returned on next call to Send, then cleared.
	nextErr error
}

// SetNextError will cause the next callback (and only the next callback) to Send to return an error
func (s *MockChunkStream) SetNextError(err error) {
	s.nextErr = err
}

// Reset will discard all collected chunks and reset nextErr
func (s *MockChunkStream) Reset() {
	s.nextErr = nil
	s.chunks = s.chunks[:0]
}

// Send implements LookupChunkReadyCallback so that this can be a destination of a LookupSink
func (s *MockChunkStream) Send(chunk *LookupChunk) error {
	s.chunks = append(s.chunks, chunk)
	err := s.nextErr
	if err != nil {
		s.nextErr = nil
	}
	return err
}

// Chunks returns the list of chunks accumulated, each LookupChunk was from a callback to Send
func (s *MockChunkStream) Chunks() []*LookupChunk {
	return s.chunks
}

// Flatten returns all the facts previously sent into one big chunk.
func (s *MockChunkStream) Flatten() *LookupChunk {
	all := new(LookupChunk)
	for _, chunk := range s.chunks {
		all.Facts = append(all.Facts, chunk.Facts...)
	}
	return all
}

// ByOffset will return a slice of lists of facts, each index into the slice
// equals the offset that was set in the Fact, at each index, there is a list
// facts in the order they were recieved for that offset.
func (s *MockChunkStream) ByOffset() [][]Fact {
	res := make([][]Fact, 0, 4)
	for _, chunk := range s.chunks {
		for _, f := range chunk.Facts {
			for len(res) <= int(f.Lookup) {
				res = append(res, nil)
			}
			res[f.Lookup] = append(res[f.Lookup], f.Fact)
		}
	}
	return res
}
