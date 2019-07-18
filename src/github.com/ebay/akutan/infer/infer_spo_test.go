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

	"github.com/ebay/akutan/facts/cache"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/parallel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_InferSPONotInferredFact(t *testing.T) {
	// a search may be looking for an inferred fact but find a concrete fact, check that's the Fact Id is returned correctly
	r := factCollector{}
	s := newInferenceSPOSearchRoot(atIndex, cache.New(), SPORequestItem{kidPostcard1, kpType, rpc.AKID(kidPostcard)}, r.add)
	breadthFirstForwardSearch(context.Background(), testFacts, atIndex, []searchSegment{s})
	exp := rpc.Fact{
		Index:     atIndex,
		Subject:   kidPostcard1,
		Predicate: kpType,
		Object:    rpc.AKID(kidPostcard),
	}
	if assert.Equal(t, 1, len(r.facts), "expecting 1 fact as a result: %#v", r.facts) {
		assertFactEqualIgnoringFactID(t, exp, r.facts[0])
		assert.True(t, r.facts[0].Id >= kidFactIDs)
	}
}

func Test_InferSPOCachesInference(t *testing.T) {
	// check that the relevant inferred facts are cached
	c := cache.New()
	resCh := make(chan *rpc.LookupChunk, 12)
	req := SPORequest{
		Index: atIndex,
		Lookups: []SPORequestItem{{
			Subject:   kidPostcard1,
			Predicate: kpType,
			Object:    rpc.AKID(kidEntity),
		}},
	}
	err := SPO(context.Background(), testFacts, c, &req, resCh)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resCh))
	act := <-resCh
	expFact := rpc.Fact{Index: atIndex, Subject: kidPostcard1, Predicate: kpType, Object: rpc.AKID(kidEntity)}
	assert.Equal(t, expFact, act.Facts[0].Fact)
	assert.Equal(t, 1, len(act.Facts))
	// we expect a cached fact from each of these subjects to kEntity via kpType
	// as they all can be inferred from the Postcard1 -> Entity result
	expSubjects := []uint64{kidPostcard1, kidPostcard, kidStationary}
	expPath := buildResolvedFactPath(atIndex, kidPostcard1, kpType, kidPostcard, kidStationary, kidProduct, kidEntity)
	for i, subject := range expSubjects {
		f, p := c.Fact(atIndex, subject, kpType, rpc.AKID(kidEntity))
		assert.Equal(t, &rpc.Fact{Index: atIndex, Subject: subject, Predicate: kpType, Object: rpc.AKID(kidEntity)}, f)
		assertFactsEqualWithAnyValidFactID(t, expPath[i:], p)
	}

	// executing the same request with the cache that was just populated should be able to answer the request from the cache
	testFacts.resetLookups()
	resCh = make(chan *rpc.LookupChunk, 4)
	err = SPO(context.Background(), testFacts, c, &req, resCh)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resCh))
	act = <-resCh
	assert.Equal(t, expFact, act.Facts[0].Fact)
	assert.Equal(t, 1, len(act.Facts))
	assert.Equal(t, 0, len(testFacts.lookuppos))
	assert.Equal(t, 0, len(testFacts.lookupsps))
}

func Test_FulfillFromCache(t *testing.T) {
	c := cache.New()
	f := rpc.Fact{Index: atIndex, Id: 1, Subject: kidLetter, Predicate: kpType, Object: rpc.AKID(kidStationary)}
	inferredFact := rpc.Fact{Index: atIndex, Subject: kidLetter, Predicate: kpType, Object: rpc.AKID(kidEntity)}
	inferredPath := buildResolvedFactPath(atIndex, kidLetter, kpType, kidStationary, kidProduct, kidEntity)
	c.AddFact(&f)
	c.AddInferedFact(&inferredFact, inferredPath)
	c.AddFiction(atIndex, kidPostcard1, kpLocation, rpc.AKID(kidUSA))

	req := SPORequest{
		Index: atIndex,
		Lookups: []SPORequestItem{
			{Subject: kidLetter, Predicate: kpType, Object: rpc.AKID(kidStationary)},    // should match a cached fact
			{Subject: kidLetter, Predicate: kpType, Object: rpc.AKID(kidEntity)},        // should match an inferred fact
			{Subject: kidPostcard1, Predicate: kpLocation, Object: rpc.AKID(kidUSA)},    // should match a cached fiction
			{Subject: kidPostcard3, Predicate: kpType, Object: rpc.AKID(kidStationary)}, // shouldn't be fulfillable via the cache
		},
	}
	r := factCollector{}
	assert.True(t, fulfillLookupFromCache(atIndex, c, &req.Lookups[0], r.add))
	assert.Equal(t, []rpc.Fact{f}, r.facts)

	r.facts = nil
	assert.True(t, fulfillLookupFromCache(atIndex, c, &req.Lookups[1], r.add))
	assert.Equal(t, []rpc.Fact{inferredFact}, r.facts)

	r.facts = nil
	assert.True(t, fulfillLookupFromCache(atIndex, c, &req.Lookups[2], r.add))
	assert.Empty(t, r.facts, "we should of gotten no facts for lookup[2]")

	r.facts = nil
	assert.False(t, fulfillLookupFromCache(atIndex, c, &req.Lookups[3], r.add))
	assert.Empty(t, r.facts, "we should of gotten no facts for lookup[3]")
}

func Test_InferSPO_TargetLiteral(t *testing.T) {
	store := *testFacts
	kidStdSize := kidFactIDs + 1
	stdSizeVal := rpc.AInt64(5, 0)
	store.add(rpc.Fact{Index: atIndex, Subject: kidPostcard, Predicate: kidStdSize, Object: stdSizeVal})
	store.addKid(atIndex, kidPostcard1, kidStdSize, kidPostcard)

	resCh := make(chan *rpc.LookupChunk, 12)
	req := SPORequest{
		Index: atIndex,
		Lookups: []SPORequestItem{{
			Subject:   kidPostcard1,
			Predicate: kidStdSize,
			Object:    stdSizeVal,
		}},
	}
	err := SPO(context.Background(), &store, cache.New(), &req, resCh)
	assert.NoError(t, err)
	require.True(t, len(resCh) > 0)
	act := <-resCh
	exp := rpc.LookupChunk{
		Facts: []rpc.LookupChunk_Fact{{
			Lookup: 0,
			Fact:   rpc.Fact{Index: atIndex, Subject: kidPostcard1, Predicate: kidStdSize, Object: stdSizeVal},
		}},
	}
	assert.Equal(t, exp, *act)
	assert.Zero(t, len(resCh), "More than the expected number of results")

	// now repeat the SPO with a literal object that's no there
	resCh = make(chan *rpc.LookupChunk, 12)
	req.Lookups[0].Object = rpc.AInt64(42, 0)
	err = SPO(context.Background(), &store, cache.New(), &req, resCh)
	assert.NoError(t, err)
	assert.Zero(t, len(resCh))
}

func Test_InferSPandSPOagree(t *testing.T) {
	// all results from an InferSP call should also be generated by InferSPO
	req := SPRequest{
		Index: atIndex,
		Lookups: []SPRequestItem{{
			Subject:   kidPostcard2,
			Predicate: kpType,
		}},
	}
	resCh := make(chan *rpc.LookupChunk, 4)
	resCount := 0
	waiter := parallel.Go(func() {
		for c := range resCh {
			for _, f := range c.Facts {
				t.Logf("Testing InferSP result %#v", f.Fact)
				spoReq := SPORequest{
					Index: atIndex,
					Lookups: []SPORequestItem{{
						Subject:   f.Fact.Subject,
						Predicate: f.Fact.Predicate,
						Object:    f.Fact.Object,
					}},
				}
				spoResCh := make(chan *rpc.LookupChunk, 2)
				err := SPO(context.Background(), testFacts, cache.New(), &spoReq, spoResCh)
				assert.NoError(t, err)
				assert.Equal(t, 1, len(spoResCh))
				act := <-spoResCh
				assert.Equal(t, 1, len(act.Facts))
				assert.Equal(t, f.Fact, act.Facts[0].Fact)
				resCount++
			}
		}
	})
	err := SP(context.Background(), testFacts, &req, resCh)
	assert.NoError(t, err)
	waiter()
	assert.True(t, resCount > 0)
}
