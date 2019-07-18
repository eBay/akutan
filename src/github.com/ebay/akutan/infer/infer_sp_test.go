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

	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/parallel"
	"github.com/ebay/akutan/viewclient/lookups"
	"github.com/stretchr/testify/assert"
)

func Test_InferSP(t *testing.T) {
	req := SPRequest{
		Index: atIndex,
		Lookups: []SPRequestItem{
			{Subject: kidPostcard3, Predicate: kpType},
			{Subject: kidPostcard1, Predicate: kpLocation},
		},
	}
	res, err := collectInferSPResults(t, &req, testFacts)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(res))
	expResultsPostcard3 := buildInferedFacts(atIndex, kidPostcard3, kpType, kidPostcard, kidLetter, kidStationary, kidProduct, kidEntity)
	expResultsPostcard1 := buildInferedFacts(atIndex, kidPostcard1, kpLocation, kidLondon, kidEngland, kidEurope)
	for _, f := range res[0] {
		// we only expect postcard3 -> kPostcard & postcard3 -> kLetter to have a set fact ID
		kid := f.Object.ValKID()
		assert.Equal(t, kid == kidPostcard || kid == kidLetter, f.Id > 0)
	}
	for _, f := range res[1] {
		// we only expect postcard1 -> kLondon to have a set fact ID
		assert.Equal(t, f.Object.ValKID() == kidLondon, f.Id > 0)
	}
	assertFactsEqualSetIgnoreingFactID(t, expResultsPostcard3, res[0])
	assertFactsEqualSetIgnoreingFactID(t, expResultsPostcard1, res[1])
}

// collectInferSPResults will perform the supplied InferSP request, aggregating the results into a single
// LookupChunk, the function returns once inferSP indicates it completed, or had an error
func collectInferSPResults(t *testing.T, req *SPRequest, lookup lookups.SP) ([][]rpc.Fact, error) {
	res := new(rpc.MockChunkStream)
	resCh := make(chan *rpc.LookupChunk, 4)
	wait := parallel.Go(func() {
		for chunk := range resCh {
			res.Send(chunk)
		}
	})
	err := SP(context.Background(), lookup, req, resCh)
	wait()
	return res.ByOffset(), err
}
