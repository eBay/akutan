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

package infer

import (
	"context"
	"testing"

	"github.com/ebay/beam/facts/cache"
	"github.com/ebay/beam/rpc"
	"github.com/stretchr/testify/assert"
)

func Test_BreadthFirstSearch(t *testing.T) {
	cache := cache.New()
	r1 := factCollector{}
	r2 := factCollector{}
	s1 := newInferenceSPOSearchRoot(atIndex+1, cache, SPORequestItem{kidPostcard1, kpType, rpc.AKID(kidProduct)}, r1.add)
	s2 := newInferenceSPOSearchRoot(atIndex+1, cache, SPORequestItem{kidPostcard1, kpLocation, rpc.AKID(kidEurope)}, r2.add)

	testFacts.resetLookups()
	breadthFirstForwardSearch(context.Background(), testFacts, atIndex+1, []searchSegment{s1, s2})
	expPostcardProduct := rpc.Fact{Index: atIndex + 1, Subject: kidPostcard1, Predicate: kpType, Object: rpc.AKID(kidProduct)}
	if assert.Equal(t, 1, len(r1.facts), "expecting to have gotten a fact for lookup[0]") {
		assert.Equal(t, expPostcardProduct, r1.facts[0])
	}
	expLocationFact := rpc.Fact{Index: atIndex + 1, Subject: kidPostcard1, Predicate: kpLocation, Object: rpc.AKID(kidEurope)}
	if assert.Equal(t, 1, len(r2.facts), "expecting to have gotten a fact for lookup[1]") {
		assert.Equal(t, expLocationFact, r2.facts[0])
	}
	assert.Equal(t, 3, len(testFacts.lookupsps), "search should have 3 iterations of LookupSP")

	// if we execute the exact same search using the same cache it should be able to
	// complete the search with only one lookup. (because the exact match cache check is done in infer_spo,
	// not the search itself)
	testFacts.resetLookups()
	r1.facts = nil
	r2.facts = nil
	breadthFirstForwardSearch(context.Background(), testFacts, atIndex+1, []searchSegment{s1, s2})
	assert.Equal(t, expPostcardProduct, r1.facts[0])
	assert.Equal(t, expLocationFact, r2.facts[0])
	assert.Equal(t, 1, len(testFacts.lookupsps), "search should have only 1 iteration of LookupSP")

	// a search that connects to a point in a previous path can also take advantage of the cache
	testFacts.resetLookups()
	r3 := factCollector{}
	s3 := newInferenceSPOSearchRoot(atIndex+1, cache, SPORequestItem{kidPostcard2, kpType, rpc.AKID(kidProduct)}, r3.add)
	breadthFirstForwardSearch(context.Background(), testFacts, atIndex+1, []searchSegment{s3})
	expFact := rpc.Fact{Index: atIndex + 1, Subject: kidPostcard2, Predicate: kpType, Object: rpc.AKID(kidProduct)}
	if assert.Equal(t, 1, len(r3.facts), "expected to have gotten a fact for s3") {
		assert.Equal(t, expFact, r3.facts[0])
	}
	assert.Equal(t, 1, len(testFacts.lookupsps), "search should have only 1 iteration of LookupSP")
}

func Test_breadthFirstForwardSearchNotFound(t *testing.T) {
	r := factCollector{}
	s := newInferenceSPOSearchRoot(atIndex, cache.New(), SPORequestItem{kidPostcard1, kpLocation, rpc.AKID(kidUSA)}, r.add)
	breadthFirstForwardSearch(context.Background(), testFacts, atIndex, []searchSegment{s})
	assert.Empty(t, r.facts)

	// for bulk search, some might find a path, some might not
	r2 := factCollector{}
	s2 := newInferenceSPOSearchRoot(atIndex, cache.New(), SPORequestItem{kidPostcard1, kpType, rpc.AKID(kidStationary)}, r2.add)
	breadthFirstForwardSearch(context.Background(), testFacts, atIndex, []searchSegment{s, s2})
	expPostcardFact := rpc.Fact{Index: atIndex, Subject: kidPostcard1, Predicate: kpType, Object: rpc.AKID(kidStationary)}
	assert.Empty(t, r.facts)
	if assert.Equal(t, 1, len(r2.facts), "Expecting a result for s2") {
		assert.Equal(t, expPostcardFact, r2.facts[0])
	}
}
