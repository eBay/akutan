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

package mockstore

import (
	"context"
	"testing"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_NewDB(t *testing.T) {
	db := NewDB()
	assert.Equal(t, blog.Index(1), db.SkipIndex())
}

func Test_Lookups(t *testing.T) {
	db := NewDB()
	lookups := db.Lookups()
	db.AddSPO(823, 2, rpc.AKID(3))
	db.AddFact(rpc.Fact{
		Index:     1000,
		Id:        1000001,
		Subject:   4,
		Predicate: 5,
		Object:    rpc.AKID(6),
	})
	resCh := make(chan *rpc.LookupChunk, 1)
	err := lookups.LookupS(context.Background(), &rpc.LookupSRequest{
		Index:    10,
		Subjects: []uint64{823},
	}, resCh)
	require.NoError(t, err)
	chunk := <-resCh
	require.Len(t, chunk.Facts, 1)
	assert.Equal(t, "{idx:1 id:1001 s:823 p:2 o:#3}", chunk.Facts[0].Fact.String())
}

func Test_Current(t *testing.T) {
	db := NewDB()
	db.AddSPO(823, 2, rpc.AKID(3))
	snap := db.Current()
	db.AddSPO(824, 2, rpc.AKID(3))
	assert.Len(t, snap.StoredFacts(), 1)
}

func Test_AddFact(t *testing.T) {
	db := NewDB()
	fact := rpc.Fact{
		Index:     1000,
		Id:        1000001,
		Subject:   4,
		Predicate: 5,
		Object:    rpc.AKID(6),
	}
	db.AddFact(fact)
	assert.Equal(t, []rpc.Fact{fact}, db.Current().StoredFacts())
	assert.Equal(t, blog.Index(1001), db.SkipIndex())
}

func Test_AddSPO(t *testing.T) {
	db := NewDB()
	db.SkipIndex()
	fact1 := db.AddSPO(823, 2, rpc.AString("hello", 0))
	fact2 := db.AddSPO(824, 2, rpc.AString("world", 0))
	assert.Equal(t, `{idx:2 id:2001 s:823 p:2 o:"hello"}`, fact1.String())
	assert.Equal(t, `{idx:3 id:3001 s:824 p:2 o:"world"}`, fact2.String())
}
