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

	"github.com/ebay/beam/blog"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/parallel"
	"github.com/ebay/beam/viewclient/lookups"
	opentracing "github.com/opentracing/opentracing-go"
)

// breadthFirstForwardSearch performs a bulk iterative breadth first search for all the searches supplied.
// the searches get callbacks accumulating the paths discovered, its upto them to convert these callbacks
// into whatever result it needs, and decide if it needs to keep going.
// the entire search is performed as of the log index, which must be supplied.
func breadthFirstForwardSearch(ctx context.Context, lookuper lookups.SP, logIndex blog.Index, searches []searchSegment) error {
	if logIndex == 0 {
		panic("Programmer error infer.breadthFirstForwardSearch called with a 0 log index parameter")
	}
	if len(searches) == 0 {
		return nil
	}
	span, searchCtx := opentracing.StartSpanFromContext(ctx, "breadthFirstForwardSearch")
	span.SetTag("search count", len(searches))
	defer span.Finish()

	// we've got a list of searches, we build a lookup_sp request from each of them
	// run it and process the results, this might generate a new list of searches
	// if so we go around the loop again, if it doesn't then the search is complete
	lookupReq := rpc.LookupSPRequest{
		Index:   logIndex,
		Lookups: make([]rpc.LookupSPRequest_Item, 0, len(searches)),
	}
	type lookupSearchState struct {
		// the offset of the original inbound search
		searchOffset int
		// the set of searches for the current iteration
		searches []searchSegment
		// next accumulates the next iteration of searches, may be discarded if the search is terminated early
		next []searchSegment
		// this is get set if a searchSegment indicates it wants to terminate this search
		terminated bool
	}
	//lookupSPtoSearch tracks which search & lookupSearchState is related to a generated LookupSP request
	type lookupSPtoSearch struct {
		search searchSegment
		state  *lookupSearchState
	}
	state := make([]lookupSearchState, len(searches))
	for i := range searches {
		state[i].searchOffset = i
		state[i].searches = []searchSegment{searches[i]}
	}
	lookupSearchRef := make([]lookupSPtoSearch, 0, len(searches))
	iterationCount := 0
	finished := false
	for !finished {
		// build the next lookup_sp request for each of the current searchSegements
		span, iterCtx := opentracing.StartSpanFromContext(searchCtx, "search iteration")
		span.SetTag("iteration", iterationCount)
		lookupReq.Lookups = lookupReq.Lookups[:0]
		lookupSearchRef = lookupSearchRef[:0]
		for i := range state {
			for _, search := range state[i].searches {
				lookupReq.Lookups = append(lookupReq.Lookups, search.nextLookup())
				lookupSearchRef = append(lookupSearchRef, lookupSPtoSearch{
					search: search,
					state:  &state[i],
				})
			}
		}
		lookupResCh := make(chan *rpc.LookupChunk, 4)
		wait := parallel.Go(func() {
			// process the lookup results, pass each returned fact to the relevant
			// searchSegment, and accumulate any new searches. If a searchSegment
			// wants to terminate its search, we flag that in state and clear out
			// any previously constructed next searches.
			for chunk := range lookupResCh {
				for _, f := range chunk.Facts {
					state := lookupSearchRef[f.Lookup].state
					if !state.terminated {
						nextSearch, done := lookupSearchRef[f.Lookup].search.applyResult(f.Fact)
						if done {
							state.terminated = true
							state.next = nil
						} else if nextSearch != nil {
							state.next = append(state.next, nextSearch)
						}
					}
				}
			}
		})
		err := lookuper.LookupSP(iterCtx, &lookupReq, lookupResCh)
		wait()
		if err != nil {
			span.Finish()
			return err
		}
		// update the state to reflect the end of the iteration
		// and see if we need to go again
		finished = true
		for i := range state {
			s := &state[i]
			s.searches, s.next = s.next, nil
			if len(s.searches) > 0 {
				finished = false
			}
		}
		iterationCount++
		span.Finish()
	}
	return nil
}

// searchSegment is used in conjunction with breadthFirstForwardSearch,
// it represents a single potential edge in a path being explored.
type searchSegment interface {

	// nextLookup should return a lookup item describing the path (s,p,?o) to query.
	nextLookup() rpc.LookupSPRequest_Item

	// applyResult will be called repeatedly with the results of executing the lookup
	// previously returned from the nextLookup() call.
	// it can return a new pathSegment that will continue the search,
	// it can terminate the search, including other new searches generated
	// for this iteration by returning true.
	// the search continue until it is either explictly terminated, or
	// when there are no new searchSegments to execute.
	applyResult(rpc.Fact) (searchSegment, bool)
}
