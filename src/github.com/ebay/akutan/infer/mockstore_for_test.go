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

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/cmp"
	"github.com/stretchr/testify/assert"
)

const (
	kpType uint64 = iota + 100
	kpLocation
	kidEntity
	kidProduct
	kidLetter
	kidStationary
	kidPostcard
	kidPostcard1
	kidPostcard2
	kidPostcard3
	kidPostcard4
	kidPostcard5
	kidLondon
	kidEngland
	kidEurope
	kidCalifornia
	kidUSA
	kidFactIDs // leave this one last in the list
)

const (
	atIndex uint64 = 5
)

var (
	testFacts                     = new(mockStore)
	idPostcard3TypeLetter         uint64
	idPostcard3LocationCalifornia uint64
)

func init() {
	testFacts.addKid(atIndex, kidPostcard1, kpType, kidPostcard)
	testFacts.addKid(atIndex, kidPostcard2, kpType, kidPostcard)
	idPostcard3TypeLetter = testFacts.addKid(atIndex, kidPostcard3, kpType, kidLetter)
	testFacts.addKid(atIndex, kidPostcard3, kpType, kidPostcard)
	testFacts.addKid(atIndex, kidPostcard4, kpType, kidPostcard)
	testFacts.addKid(atIndex, kidPostcard5, kpType, kidPostcard)
	testFacts.addKid(atIndex, kidPostcard, kpType, kidStationary)
	testFacts.addKid(atIndex, kidLetter, kpType, kidStationary)
	testFacts.addKid(atIndex, kidStationary, kpType, kidProduct)
	testFacts.addKid(atIndex, kidProduct, kpType, kidEntity)
	testFacts.addKid(atIndex, kidPostcard1, kpLocation, kidLondon)
	testFacts.addKid(atIndex, kidLondon, kpLocation, kidEngland)
	testFacts.addKid(atIndex, kidEngland, kpLocation, kidEurope)
	testFacts.addKid(atIndex, kidCalifornia, kpLocation, kidUSA)
	idPostcard3LocationCalifornia = testFacts.addKid(atIndex, kidPostcard3, kpLocation, kidCalifornia)
}

func Test_MockStoreLookupSp(t *testing.T) {
	req := rpc.LookupSPRequest{
		Index: atIndex + 1,
		Lookups: []rpc.LookupSPRequest_Item{
			{Subject: kidPostcard, Predicate: kpType},
		},
	}
	resCh := make(chan *rpc.LookupChunk, 4)
	err := testFacts.LookupSP(context.Background(), &req, resCh)
	assert.NoError(t, err)
	if assert.Equal(t, 1, len(resCh), "Didn't find expected result in channel") {
		res := <-resCh
		if assert.Equal(t, 1, len(res.Facts), "Didn't find result in chunk") {
			assert.Equal(t, kidStationary, res.Facts[0].Fact.Object.ValKID())
			assert.True(t, res.Facts[0].Fact.Id > 0)
			assert.Equal(t, uint32(0), res.Facts[0].Lookup)
		}
	}
}

func Test_MockStoreLookupPO(t *testing.T) {
	req := rpc.LookupPORequest{
		Index: atIndex + 1,
		Lookups: []rpc.LookupPORequest_Item{
			{Predicate: kpType, Object: rpc.AKID(kidLetter)},
			{Predicate: kpLocation, Object: rpc.AKID(kidCalifornia)},
		},
	}
	resCh := make(chan *rpc.LookupChunk, 5)
	err := testFacts.LookupPO(context.Background(), &req, resCh)
	assert.NoError(t, err)
	assert.Equal(t, &rpc.LookupChunk{
		Facts: []rpc.LookupChunk_Fact{{
			Lookup: 0,
			Fact:   rpc.Fact{Index: atIndex, Id: idPostcard3TypeLetter, Subject: kidPostcard3, Predicate: kpType, Object: rpc.AKID(kidLetter)},
		}},
	}, <-resCh)
	assert.Equal(t, &rpc.LookupChunk{
		Facts: []rpc.LookupChunk_Fact{{
			Lookup: 1,
			Fact:   rpc.Fact{Index: atIndex, Id: idPostcard3LocationCalifornia, Subject: kidPostcard3, Predicate: kpLocation, Object: rpc.AKID(kidCalifornia)},
		}},
	}, <-resCh)
	assert.Equal(t, 0, len(resCh))
}

type factCollector struct {
	facts []rpc.Fact
}

func (fc *factCollector) add(fact rpc.Fact) {
	fc.facts = append(fc.facts, fact)
}

type mockStore struct {
	facts      []rpc.Fact
	nextFactID uint64
	lookupsps  []rpc.LookupSPRequest // the received lookupSP requests
	lookuppos  []rpc.LookupPORequest // the received lookupPO requests
	nextError  error
}

func (s *mockStore) resetLookups() {
	s.lookupsps = s.lookupsps[:0]
	s.lookuppos = s.lookuppos[:0]
}

func (s *mockStore) add(f rpc.Fact) uint64 {
	if s.nextFactID == 0 {
		s.nextFactID = kidFactIDs
	}
	f.Id = s.nextFactID
	s.nextFactID++
	s.facts = append(s.facts, f)
	return f.Id
}

func (s *mockStore) addKid(index uint64, subject, predicate, object uint64) uint64 {
	return s.add(rpc.Fact{
		Index:     index,
		Subject:   subject,
		Predicate: predicate,
		Object:    rpc.AKID(object),
	})
}

func (s *mockStore) LookupSP(ctx context.Context, req *rpc.LookupSPRequest, resCh chan *rpc.LookupChunk) error {
	defer close(resCh)
	if s.nextError != nil {
		err := s.nextError
		s.nextError = nil
		return err
	}
	s.lookupsps = append(s.lookupsps, *req)
	for offset, lookup := range req.Lookups {
		var res rpc.LookupChunk
		for _, f := range s.facts {
			if lookup.Subject == f.Subject && lookup.Predicate == f.Predicate && f.Index <= req.Index {
				res.Facts = append(res.Facts, rpc.LookupChunk_Fact{
					Lookup: uint32(offset),
					Fact:   f,
				})
			}
		}
		if len(res.Facts) > 0 {
			resCh <- &res
		}
	}
	return nil
}

func (s *mockStore) LookupPO(ctx context.Context, req *rpc.LookupPORequest, resCh chan *rpc.LookupChunk) error {
	defer close(resCh)
	if s.nextError != nil {
		err := s.nextError
		s.nextError = nil
		return err
	}
	s.lookuppos = append(s.lookuppos, *req)
	for offset, lookup := range req.Lookups {
		var res rpc.LookupChunk
		for _, f := range s.facts {
			if lookup.Predicate == f.Predicate && f.Index <= req.Index && lookup.Object.Equal(f.Object) {
				res.Facts = append(res.Facts, rpc.LookupChunk_Fact{
					Lookup: uint32(offset),
					Fact:   f,
				})
			}
		}
		if len(res.Facts) > 0 {
			resCh <- &res
		}
	}
	return nil
}

// buildResolvedFactPath build a list of resolvedFacts describing the path from start down pathItems
// via the predicate, e.g. given predicate=type, start=postcard1, pathItems=postcard, product
// you'd get a path of resolvedFacts
//		subject=postcard1 predicate=type object=postcard
//		subject=postcard predicate=type object=product
func buildResolvedFactPath(index uint64, start, predicate uint64, pathItems ...uint64) []rpc.Fact {
	r := make([]rpc.Fact, 0, len(pathItems))
	f := rpc.Fact{Index: index, Predicate: predicate}
	for _, item := range pathItems {
		f.Subject = start
		f.Object = rpc.AKID(item)
		r = append(r, f)
		start = item
	}
	return r
}

// buildInferedFacts will create an ResolvedFact consisting of <index> <start> <predicate> <target> for each
// of the supplied targets. Id will not be set
func buildInferedFacts(index blog.Index, subject, predicate uint64, targets ...uint64) []rpc.Fact {
	r := make([]rpc.Fact, 0, len(targets))
	f := rpc.Fact{Index: index, Predicate: predicate, Subject: subject}
	for _, t := range targets {
		f.Object = rpc.AKID(t)
		r = append(r, f)
	}
	return r
}

// assertFactsEqualWithAnyValidFactID asserts that the 2 lists of facts are the same, except for FactID all
// it checks is that the Fact Id > 0
func assertFactsEqualWithAnyValidFactID(t *testing.T, exp []rpc.Fact, act []rpc.Fact) {
	assert.Equal(t, len(exp), len(act), "Arrays different length")
	for i := 0; i < cmp.MinInt(len(exp), len(act)); i++ {
		assertFactEqualIgnoringFactID(t, exp[i], act[i])
		assert.True(t, act[i].Id >= kidFactIDs, "Fact should have a non-zero ID")
	}
}

// assertFactEqualIgnoringFactID assets that 2 ResolvedFacts are equal ignoring the FactID
func assertFactEqualIgnoringFactID(t *testing.T, exp rpc.Fact, act rpc.Fact) {
	assert.Equal(t, exp.Subject, act.Subject, "Subject mismatch")
	assert.Equal(t, exp.Predicate, act.Predicate, "Predicate mismatch")
	assert.Equal(t, exp.Object, act.Object, "Object mismatch")
	assert.Equal(t, exp.Index, act.Index, "Index mistmatch")
}

// assertFactsEqualSetIgnoreingFactID asserts that the actual set of facts is equal to the expected
// set of facts ignoring the factID, these are treated as sets, i.e. the order doesn't need to match
func assertFactsEqualSetIgnoreingFactID(t *testing.T, exp []rpc.Fact, act []rpc.Fact) {
	clearFactIDs := func(facts []rpc.Fact) {
		for i := range facts {
			facts[i].Id = 0
		}
	}
	clearFactIDs(exp)
	clearFactIDs(act)
	assert.ElementsMatchf(t, exp, act, "Fact set mismatch, atual facts : %v", act)
}
