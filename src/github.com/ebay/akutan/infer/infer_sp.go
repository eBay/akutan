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
	"github.com/ebay/akutan/viewclient/lookups"
	opentracing "github.com/opentracing/opentracing-go"
)

// SPRequest describes a request to infer 1 or more (S,P) lookups
// all inference is performed as of the supplied index, which must be set
type SPRequest struct {
	Index   uint64
	Lookups []SPRequestItem
}

// SPRequestItem describes a single Subject, Predicate lookup to perform
// as part of a larger SP request.
type SPRequestItem struct {
	Subject   uint64
	Predicate uint64
}

// SP will perform a breadth first graph traversal to infer facts for each of the supplied subject & predicate pairs
// chunks of results will be written to the supplied result channel, when the traversal is successfully completed
// this function returns nil. In the event of an error, the error will be returned, in either case 'resCh' will
// be closed.
//
// The returned facts are all the static & infered facts from the subject following the predicate
// e.g. given facts
// 		<iPhone> <type> <smartPhone>
//		<smartPhone> <type> <consumerElectronics>
//		<consumerElectronics> <type> <product>
// running infer.SP(<iPhone><type>) would return results
//		<iPhone> <type> <smartPhone>
//		<iPhone> <type> <consumerElectronics>
//		<iPhone> <type> <product>
//
// We currently assume all predicates are transitive, however we expect that in the future the predicate schema
// can indicate that predicates are/are not transitive.
// the request can specify a list of infers to run, and these are execute efficiently in bulk.
// If there's more than one lookup in the request, the generated LookupChunks indicate which lookup they
// are for via the Lookup field which specifies the index into req.Lookups that that result is for
func SP(ctx context.Context, lookup lookups.SP, req *SPRequest, resCh chan<- *rpc.LookupChunk) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "InferSP")
	span.SetTag("lookup_count", len(req.Lookups))
	defer span.Finish()
	defer close(resCh)

	// sender will accumulate results and flush them to resCh as needed
	sender := rpc.NewFactSink(func(c *rpc.LookupChunk) error {
		resCh <- c
		return nil
	}, 32)

	// build a search root for each lookup in the request,
	// they send their results to the shared sender.
	searches := make([]searchSegment, len(req.Lookups))
	for i, l := range req.Lookups {
		search := inferenceSPSearchRoot{
			subject:        l.Subject,
			predicate:      l.Predicate,
			visited:        make(map[uint64]struct{}),
			resultCallback: resultFowarder(sender, i, req.Index),
		}
		searches[i] = &search
	}
	if err := breadthFirstForwardSearch(ctx, lookup, req.Index, searches); err != nil {
		return err
	}
	sender.Flush()
	return nil
}

// inferenceSPSearchRoot is a searchSegment that is the root of an inferSP search
// it will pass the results it finds off to the resultCallback
type inferenceSPSearchRoot struct {
	subject        uint64
	predicate      uint64
	visited        map[uint64]struct{} // visited keeps track of KIDs processed to detect & stop loops
	resultCallback func(rpc.Fact)
}

func (root *inferenceSPSearchRoot) nextLookup() rpc.LookupSPRequest_Item {
	return rpc.LookupSPRequest_Item{
		Subject:   root.subject,
		Predicate: root.predicate,
	}
}

func (root *inferenceSPSearchRoot) applyResult(f rpc.Fact) (searchSegment, bool) {
	return root.applyLookupResult(f, false)
}

// applyLookupResult will build a ResolvedFact from the supplied result and send them to the resultCallback.
// For any result that is itself a KID it'll build & return a new searchSegment for that to continue the search
func (root *inferenceSPSearchRoot) applyLookupResult(rf rpc.Fact, isInferred bool) (searchSegment, bool) {
	if rf.Object.ValKID() != 0 {
		if _, exists := root.visited[rf.Object.ValKID()]; exists {
			return nil, false
		}
		root.visited[rf.Object.ValKID()] = struct{}{}
	}
	factResult := rf
	if isInferred {
		factResult = rpc.Fact{
			Subject:   root.subject,
			Predicate: root.predicate,
			Object:    rf.Object,
		}
	}
	root.resultCallback(factResult)

	if rf.Object.ValKID() != 0 {
		ss := inferenceSPSearchEdge{
			root:    root,
			subject: rf.Object.ValKID(),
		}
		return &ss, false
	}
	return nil, false
}

type inferenceSPSearchEdge struct {
	root    *inferenceSPSearchRoot
	subject uint64
}

func (e *inferenceSPSearchEdge) nextLookup() rpc.LookupSPRequest_Item {
	return rpc.LookupSPRequest_Item{
		Subject:   e.subject,
		Predicate: e.root.predicate,
	}
}

func (e *inferenceSPSearchEdge) applyResult(f rpc.Fact) (searchSegment, bool) {
	return e.root.applyLookupResult(f, true)
}
