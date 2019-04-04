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
	"errors"
	"sort"
	"testing"

	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/parallel"
	"github.com/ebay/beam/viewclient/lookups"
	"github.com/stretchr/testify/assert"
)

func Test_InferPO(t *testing.T) {
	req := PORequest{
		Index: atIndex,
		Lookups: []PORequestItem{{
			Predicate: kpType,
			Object:    rpc.AKID(kidProduct),
		}, {
			Predicate: kpLocation,
			Object:    rpc.AKID(kidUSA),
		}},
	}
	res, err := collectInferPOResults(t, &req, testFacts)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(res), "Expected results for both lookups in the request")

	expSubjects0 := []uint64{kidStationary, kidPostcard, kidLetter, kidPostcard1, kidPostcard2, kidPostcard3, kidPostcard4, kidPostcard5}
	assert.ElementsMatch(t, expSubjects0, subjectsOf(res[0]))
	assertTheseSubjectsHaveIds(t, res[0], kidStationary)

	expSubjects1 := []uint64{kidCalifornia, kidPostcard3}
	assert.ElementsMatch(t, expSubjects1, subjectsOf(res[1]))
	assertTheseSubjectsHaveIds(t, res[1], kidCalifornia)
}

func Test_InferPONoLoops(t *testing.T) {
	const (
		a uint64 = iota + 1
		b
		c
		p
	)
	loopedFacts := new(mockStore)
	loopedFacts.add(rpc.Fact{Index: atIndex, Subject: b, Predicate: p, Object: rpc.AKID(c)})
	loopedFacts.add(rpc.Fact{Index: atIndex, Subject: a, Predicate: p, Object: rpc.AKID(b)})
	loopedFacts.add(rpc.Fact{Index: atIndex, Subject: c, Predicate: p, Object: rpc.AKID(a)})

	req := PORequest{
		Index: atIndex,
		Lookups: []PORequestItem{{
			Predicate: p,
			Object:    rpc.AKID(a),
		}},
	}
	res, err := collectInferPOResults(t, &req, loopedFacts)
	assert.NoError(t, err)
	expSubjects := []uint64{b, c}
	assert.ElementsMatch(t, expSubjects, subjectsOf(res[0]))
}

func Test_InferPOError(t *testing.T) {
	lookupErr := errors.New("There was an error")
	testFacts.nextError = lookupErr
	req := PORequest{
		Index: 2,
		Lookups: []PORequestItem{{
			Predicate: 3,
			Object:    rpc.AKID(4),
		}},
	}
	res, err := collectInferPOResults(t, &req, testFacts)
	assert.Equal(t, lookupErr, err)
	assert.Empty(t, res[0])
}

func Test_InferPOLookupChunking(t *testing.T) {
	// under large fan-out or large number of lookups, Infer should chunk the LookupPO requests it makes
	store := new(mockStore)
	topLevelEntities := make([]uint64, 5)
	subjectsForTopLevel := make([][]uint64, 5)
	var next, lvl1, lvl2 uint64
	for i := range topLevelEntities {
		root := uint64(1000 * (i + 1))
		topLevelEntities[i] = root
		next = root + 1
		for j := 0; j < 11; j++ {
			lvl1, next = next, next+1
			subjectsForTopLevel[i] = append(subjectsForTopLevel[i], lvl1)
			store.add(rpc.Fact{Index: 2, Subject: lvl1, Predicate: 42, Object: rpc.AKID(root)})
			for k := 0; k < 12; k++ {
				lvl2, next = next, next+1
				subjectsForTopLevel[i] = append(subjectsForTopLevel[i], lvl2)
				store.add(rpc.Fact{Index: 2, Subject: lvl2, Predicate: 42, Object: rpc.AKID(lvl1)})
			}
		}
	}
	req := PORequest{
		Index:   2,
		Lookups: make([]PORequestItem, len(topLevelEntities)),
	}
	for i, kid := range topLevelEntities {
		req.Lookups[i] = PORequestItem{Predicate: 42, Object: rpc.AKID(kid)}
	}
	res, err := collectInferPOResults(t, &req, store)
	assert.NoError(t, err)
	for i := range topLevelEntities {
		assert.ElementsMatch(t, subjectsForTopLevel[i], subjectsOf(res[i]), "Unexpected results for lookup[%d]", i)
	}
	for _, lreq := range store.lookuppos {
		assert.True(t, len(lreq.Lookups) <= maxObjectsPerLookupPO, "a generated LookupPO request had too many lookups in it: %d", len(lreq.Lookups))
	}
}

// collectInferPOResults will accumulate all the results from the stream into a single list of results
// for each lookup executed.
func collectInferPOResults(t *testing.T, req *PORequest, facts lookups.PO) ([][]rpc.Fact, error) {
	res := make([][]rpc.Fact, len(req.Lookups))

	resCh := make(chan *rpc.LookupChunk, 4)
	wait := parallel.GoCaptureError(func() error {
		return PO(context.Background(), facts, req, resCh)
	})
	for chunk := range resCh {
		for _, f := range chunk.Facts {
			res[f.Lookup] = append(res[f.Lookup], f.Fact)
		}
	}
	if err := wait(); err != nil {
		return res, err
	}
	for lookup := range res {
		subjects := subjectsOf(res[lookup])
		sort.Slice(subjects, func(i, j int) bool {
			return subjects[i] < subjects[j]
		})
		prev := uint64(0)
		for _, s := range subjects {
			assert.NotEqual(t, prev, s, "InferPO for Lookup[%d] returned duplicate results for subject %d", lookup, s)
			prev = s
		}
	}
	return res, nil
}

func assertTheseSubjectsHaveIds(t *testing.T, facts []rpc.Fact, subjects ...uint64) {
	t.Helper()
	sm := make(map[uint64]bool, len(subjects))
	for _, s := range subjects {
		sm[s] = true
	}
	for _, f := range facts {
		assert.True(t, f.Index > 0, "Index of returned Fact was 0 %#v", f)
		if sm[f.Subject] {
			assert.True(t, f.Id > 0, "Id of fact was expected to not be zero")
			delete(sm, f.Subject)
		} else {
			assert.Equal(t, uint64(0), f.Id)
		}
	}
	assert.Emptyf(t, sm, "Expected subjects were not found in the results")
}

// subjectsOf will collect up all the subjects from the results and return them
func subjectsOf(facts []rpc.Fact) []uint64 {
	res := make([]uint64, len(facts))
	for i := range facts {
		res[i] = facts[i].Subject
	}
	return res
}
