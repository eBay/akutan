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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MockChunkStream(t *testing.T) {
	s := new(MockChunkStream)
	c1 := &LookupChunk{
		Facts: []LookupChunk_Fact{{
			Lookup: 1,
			Fact:   Fact{Id: 1, Index: 2, Subject: 3, Predicate: 4, Object: AString("bob", 0)},
		}, {
			Lookup: 2,
			Fact:   Fact{Id: 11, Index: 12, Subject: 13, Predicate: 14, Object: AString("alice", 0)},
		}},
	}
	c2 := &LookupChunk{
		Facts: []LookupChunk_Fact{{
			Lookup: 1,
			Fact:   Fact{Id: 21, Index: 22, Subject: 23, Predicate: 24, Object: AString("eve", 0)},
		}},
	}
	assert.Empty(t, s.Flatten().Facts, "An empty collector should have no facts")
	s.Send(c1)
	assert.Equal(t, []*LookupChunk{c1}, s.Chunks())
	assert.Equal(t, c1, s.Flatten())
	s.Send(c2)
	assert.Equal(t, []*LookupChunk{c1, c2}, s.Chunks())
	expFlat := &LookupChunk{
		Facts: []LookupChunk_Fact{c1.Facts[0], c1.Facts[1], c2.Facts[0]},
	}
	assert.Equal(t, expFlat, s.Flatten())
	expByOffset := make([][]Fact, 3)
	expByOffset[1] = []Fact{c1.Facts[0].Fact, c2.Facts[0].Fact}
	expByOffset[2] = []Fact{c1.Facts[1].Fact}
	assert.Equal(t, expByOffset, s.ByOffset())

	sendErr := errors.New("failed to send")
	s.SetNextError(sendErr)
	assert.Equal(t, sendErr, s.Send(c1))
	assert.NoError(t, s.Send(c1))

	s.SetNextError(sendErr)
	s.Reset()
	assert.Empty(t, s.Chunks())
	assert.Empty(t, s.Flatten().Facts)
	assert.NoError(t, s.Send(c1))
	assert.Equal(t, []*LookupChunk{c1}, s.Chunks())
}
