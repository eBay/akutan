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

	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/parallel"
	"github.com/ebay/akutan/viewclient/lookups"
	opentracing "github.com/opentracing/opentracing-go"
)

const maxObjectsPerLookupPO = 2048

// PORequestItem describes a single infer PO starting point
type PORequestItem struct {
	Predicate uint64
	Object    rpc.KGObject
}

// PORequest describes 1 or more inference lookups on Predicate,Object to perform
// all are performed as of the indicated log index, which must be set
type PORequest struct {
	Index   uint64
	Lookups []PORequestItem
}

// PO will iteratively walk backwards from the starting object following the predicate and collect
// up all the relevant facts. Results with chunks of facts are generated into the supplied results channel.
// This function will block until all results have been generate, or an error occurs.
// resCh will always be closed before this function returns.
// All the Lookups in the request aggregate their RPC lookup requests and execute them in bulk for efficiency
func PO(ctx context.Context, lookup lookups.PO, inferReq *PORequest, resCh chan<- *rpc.LookupChunk) error {
	outerSpan, ctx := opentracing.StartSpanFromContext(ctx, "InferPO")
	defer outerSpan.Finish()
	defer close(resCh)

	// we start with a single lookup_po request that has one lookup for each PO pair in the infer request.
	// the offsets array captures the offset into the infer requests that particular
	// lookup_po lookup is asocicated with (so that the lookup PO results can be grouped under
	// the right offset in the overall results)
	starter := lookupPOWithOffsets{
		req: rpc.LookupPORequest{
			Index:   inferReq.Index,
			Lookups: make([]rpc.LookupPORequest_Item, len(inferReq.Lookups)),
		},
		inferOffsets: make([]int, len(inferReq.Lookups)),
	}
	for i, lk := range inferReq.Lookups {
		starter.req.Lookups[i] = rpc.LookupPORequest_Item{Predicate: lk.Predicate, Object: lk.Object}
		starter.inferOffsets[i] = i
	}
	// we need to track which KIDs have been visited, so that we can prevent loops
	// separate tracking for each infer offset as they are all independent inference requests
	visited := make([]map[uint64]struct{}, len(inferReq.Lookups))
	for i, lk := range inferReq.Lookups {
		visited[i] = map[uint64]struct{}{
			lk.Object.ValKID(): struct{}{},
		}
	}
	lookups := make(lookupQueue, 0, 4)
	lookups.push(starter)
	// we run the next lookupPO request, when we get the lookup results we add the subjects to the
	// results for the infer offset associated with the request lookup, and add a new lookup_po to check
	// for the next hop. we keep going until we've run out of needed lookup requests.
	iteration := 0
	for len(lookups) > 0 {
		span, iterCtx := opentracing.StartSpanFromContext(ctx, "InterPO Lookup Loop")
		span.SetTag("iteration", iteration)

		req := lookups.pop()
		span.SetTag("bulk count", len(req.req.Lookups))

		lookupResCh := make(chan *rpc.LookupChunk, 4)
		lookupWait := parallel.GoCaptureError(func() error {
			return lookup.LookupPO(iterCtx, &req.req, lookupResCh)
		})
		resultChunk := rpc.LookupChunk{
			Facts: make([]rpc.LookupChunk_Fact, 0, len(inferReq.Lookups)),
		}
		// we'll loop over the results of the lookup call and build
		// the result chunk and also the next request(s), remembering
		// to keep track of where we've visited to ensure we don't get
		// stuck in a loop
		nextReq := lookupPOWithOffsets{
			req: rpc.LookupPORequest{
				Index: starter.req.Index,
			},
		}
		nextReq.initLookups()
		// lookupResCh will get closed when the LookupPO has sent all its results.
		for chunk := range lookupResCh {
			for _, fact := range chunk.Facts {
				// offset into the infer input/output for this lookup_po result
				// and where we need to write any new subject/id results to for this lookupPOResult
				inferOffset := req.inferOffsets[fact.Lookup]

				if _, exists := visited[inferOffset][fact.Fact.Subject]; !exists {
					visited[inferOffset][fact.Fact.Subject] = struct{}{}
					// for the first iteration there are concreate facts for <s> <p> <o> so they can be
					// returned as is, but for subsequent iterations the results for that iteration are
					// infered and so don't have an Id, and have their Index set to the request Index.
					// [If we tracked the path of facts that led to this fact we could return the earliest
					// possible index that this infered fact could exist, but its doesn't seem like thats
					// useful]
					var resultFact rpc.Fact
					if iteration == 0 {
						resultFact = fact.Fact
					} else {
						// after iteration 0 all resulting facts are infered
						resultFact = rpc.Fact{
							Index:     inferReq.Index,
							Id:        0, // not a persisted fact
							Subject:   fact.Fact.Subject,
							Predicate: inferReq.Lookups[inferOffset].Predicate,
							Object:    inferReq.Lookups[inferOffset].Object,
						}
					}
					resultChunk.Facts = append(resultChunk.Facts, rpc.LookupChunk_Fact{Lookup: uint32(inferOffset), Fact: resultFact})

					// build a new LookupPO lookup that starts from this subject
					nextReq.addLookup(inferOffset, inferReq.Lookups[inferOffset].Predicate, fact.Fact.Subject)
					if len(nextReq.req.Lookups) >= maxObjectsPerLookupPO {
						lookups.push(nextReq)
						nextReq.initLookups()
					}
				}
			}
		}
		if len(resultChunk.Facts) > 0 {
			resCh <- &resultChunk
		}
		if len(nextReq.req.Lookups) > 0 {
			lookups.push(nextReq)
		}
		// the LookupPO call has already finished because lookupResCh was closed,
		//  but we need to check if it returned an error
		span.Finish()
		if err := lookupWait(); err != nil {
			return err
		}
		iteration++
	}
	return nil
}

type lookupQueue []lookupPOWithOffsets

func (q *lookupQueue) pop() lookupPOWithOffsets {
	r := (*q)[0]
	*q = (*q)[1:]
	return r
}

func (q *lookupQueue) push(r lookupPOWithOffsets) {
	*q = append(*q, r)
}

type lookupPOWithOffsets struct {
	req          rpc.LookupPORequest
	inferOffsets []int // for each lookup, what's the offset in the infer request/result
}

func (l *lookupPOWithOffsets) initLookups() {
	l.req.Lookups = make([]rpc.LookupPORequest_Item, 0, maxObjectsPerLookupPO)
	l.inferOffsets = make([]int, 0, maxObjectsPerLookupPO)
}

func (l *lookupPOWithOffsets) addLookup(inferOffset int, predicate, object uint64) {
	l.req.Lookups = append(l.req.Lookups, rpc.LookupPORequest_Item{Predicate: predicate, Object: rpc.AKID(object)})
	l.inferOffsets = append(l.inferOffsets, inferOffset)
}
