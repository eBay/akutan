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

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/facts/cache"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/viewclient/lookups"
)

// SPORequest defines 1..N lookups to be executed at the supplied index
type SPORequest struct {
	Index   uint64
	Lookups []SPORequestItem
}

// SPORequestItem descibes a single SPO infer lookup to perform
type SPORequestItem struct {
	Subject   uint64
	Predicate uint64
	Object    rpc.KGObject
}

// SPO runs a graph traversal to determine if a particular fact
// can be inferred to exist, by finding a path between the subject and
// target KIDs using the predicate.
//
// If it finds a path the inferred fact is sent as part of a result chunk
// on the supplied resCh, if it can't find that item, no result is generated
// with that lookup offset.
//
// Currently we assume all predicates are transitive, for example if <a> <type> <b>
// is true and <b> <type> <c> is true, then we infer that <a> <type> <c> is also true.
// Its expected that a future version will allow for non-transitive predicates.
func SPO(ctx context.Context, lookup lookups.SP, cache cache.FactCache, req *SPORequest, resCh chan<- *rpc.LookupChunk) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()
	defer close(resCh)

	sender := rpc.NewFactSink(func(c *rpc.LookupChunk) error {
		resCh <- c
		return nil
	}, 32)
	defer sender.Flush()

	searchRoots := make([]searchSegment, 0, len(req.Lookups))

	// build the search roots. If we've already got a cached fact or fiction
	// that answers the lookup use that.
	for resultIndex, lookup := range req.Lookups {
		forwarder := resultFowarder(sender, resultIndex, req.Index)
		if fulfillLookupFromCache(req.Index, cache, &lookup, forwarder) {
			continue
		}
		search := newInferenceSPOSearchRoot(req.Index, cache, lookup, forwarder)
		searchRoots = append(searchRoots, search)
	}

	return breadthFirstForwardSearch(ctx, lookup, req.Index, searchRoots)
}

// fulfillLookupFromCache tries to use the fact cache to fulfill this lookup request.
// If it finds a matching cached Fact, it'll Write it to the provided sink and return true
// If it finds the cache knows this fact doesn't exist, it' return true (but not write anything to the sink)
// If the cache can't tell us anything about this fact, false is returned
func fulfillLookupFromCache(index uint64, cache cache.FactCache,
	lookup *SPORequestItem, resultCallback func(rpc.Fact)) bool {

	cachedFact, _ := cache.Fact(index, lookup.Subject, lookup.Predicate, lookup.Object)
	if cachedFact != nil {
		resultCallback(*cachedFact)
		return true
	}
	if cache.IsFiction(index, lookup.Subject, lookup.Predicate, lookup.Object) {
		return true
	}
	return false
}

// forwarder will add the result to the fact sink, supplying the correct lookup offset
// and filling out the index for inferred facts to be the request index.
func resultFowarder(sink *rpc.FactSink, lookupIndex int, logIndex blog.Index) func(rpc.Fact) {
	return func(f rpc.Fact) {
		// populate index for inferred facts
		if f.Index == 0 {
			f.Index = logIndex
		}
		sink.Write(lookupIndex, f)
	}
}

// newInferenceSPOSearchRoot returns a new searchPathSegment representing a search for a path from <subject>
// to <targetObject> via a path of <predicate>
// it can be used with breadthFirstForwardSearch to execute the search, once the search is completed
// the parentResult will be updated with the results. The results will only include the discovered
// path if includePathInResult is true.
func newInferenceSPOSearchRoot(index uint64, cache cache.FactCache,
	target SPORequestItem, resultCallback func(rpc.Fact)) searchSegment {

	return &inferenceSPOSearchRoot{
		index:          index,
		target:         target,
		visited:        make(map[uint64]struct{}),
		cache:          cache,
		resultCallback: resultCallback,
	}
}

// inferenceSPOSearchRoot is a searchPathSegment that descibes the inference traversal being performed.
type inferenceSPOSearchRoot struct {
	index          uint64
	target         SPORequestItem
	visited        map[uint64]struct{} // visited keeps track of KIDs processed to detect & stop loops
	cache          cache.FactCache
	resultCallback func(rpc.Fact)
}

func (root *inferenceSPOSearchRoot) nextLookup() rpc.LookupSPRequest_Item {
	return rpc.LookupSPRequest_Item{
		Subject:   root.target.Subject,
		Predicate: root.target.Predicate,
	}
}

func (root *inferenceSPOSearchRoot) applyResult(f rpc.Fact) (searchSegment, bool) {
	return root.applyLookupResult(root.target.Subject, nil, f)
}

// pathEdge is a searchPathSegment that represents an edge that has been found during an inference search
// they are constructed by calls to applyLookupResult() as needed.
// e.g. if we started a search at subject:<iPhone> predicate:<type> and a target of <product>
// and the first lookup returned KIDs <smartPhone> & <WifiDevice> we'd end up with
// [inferenceSearchRoot (iPhone,Type)] <- [pathEdge (smartPhone)]
//								 	   <- [pathEdge (WifiDevice)]
// and the lookup / results would continue round again with lookups for (smartPhone,type) & (WifiDevice,type)
type pathEdge struct {
	root    *inferenceSPOSearchRoot
	parent  *pathEdge
	factID  uint64
	subject uint64
}

func (edge *pathEdge) nextLookup() rpc.LookupSPRequest_Item {
	return rpc.LookupSPRequest_Item{
		Subject:   edge.subject,
		Predicate: edge.root.target.Predicate,
	}
}

func (edge *pathEdge) applyResult(f rpc.Fact) (searchSegment, bool) {
	return edge.root.applyLookupResult(edge.subject, edge, f)
}

// applyLookupResult will take a result from the LookupSP call and process it
// working out if the search found a result or building more lookups
// 		 * the results might complete the search
//		 * the results in conjuction with the cache might complete the search
//		 * if we didn't complete the search, return the next pathEdge to traverse
// if we find a result then we'll tell the calling breadthFirstSearch to terminate the search
func (root *inferenceSPOSearchRoot) applyLookupResult(fromSubject uint64, currentLeaf *pathEdge, rf rpc.Fact) (searchSegment, bool) {
	root.visited[fromSubject] = struct{}{}
	if rf.Object.Equal(root.target.Object) {
		// we found the path we were looking for, we're done
		root.createInferenceResult(currentLeaf, uint64(rf.Index), rf.Id, rf.Object, nil)
		return nil, true
	}
	if kidv, iskid := rf.Object.ValKID(), rf.Object.ValueType() == rpc.KtKID; iskid {
		// can the cache complete our search from rf ?
		cf, cpath := root.cache.Fact(root.index, kidv, root.target.Predicate, root.target.Object)
		if cf != nil {
			root.createInferenceResult(currentLeaf, uint64(rf.Index), rf.Id, rf.Object, cpath)
			return nil, true
		}
		// this didn't complete the result, return a new searchSegment to continue, unless its a KID we've previously visited
		if _, alreadyVisited := root.visited[kidv]; !alreadyVisited {
			root.visited[kidv] = struct{}{}
			if !root.cache.IsFiction(root.index, kidv, root.target.Predicate, root.target.Object) {
				nextSearch := pathEdge{root: root, parent: currentLeaf, factID: rf.Id, subject: kidv}
				return &nextSearch, false
			}
		}
	}
	//TODO: ?
	// if len(nextSegments) == 0 {
	// 	// we didn't find any results, and we didn't find any more paths to search
	// 	// search is over and we didn't find a path, cache the negative outcome
	// 	root.cache.AddFiction(root.index, root.subject, root.predicate, root.targetKGObject)
	// }
	return nil, false
}

// createInferenceResult will build the inferred fact and send it to the results sink.
// it'll also register the new inferred fact with the cache, along with some additional facts it can infer
// from the path, e.g. given a path of a -> b -> c -> d the inferred fact would be a -> d, but in addition
// we can also infer b -> d (we don't cache c -> d because there's a static fact for that)
func (root *inferenceSPOSearchRoot) createInferenceResult(leaf *pathEdge, factIndex blog.Index, factID uint64, obj rpc.KGObject, additionalPath []rpc.Fact) {
	resultingFact := rpc.Fact{
		Id:        factID,
		Index:     factIndex,
		Subject:   root.target.Subject,
		Predicate: root.target.Predicate,
		Object:    root.target.Object,
	}

	// if there's leaf, then this is an inferred fact, otherwise it was a persisted fact
	if leaf != nil || len(additionalPath) > 0 {
		resultingFact.Index = root.index
		resultingFact.Id = 0
	}
	root.resultCallback(resultingFact)
	// the path can consist of the root -> edge -> edge -> leaf facts along with an additional set of path items that were
	// the result of using the cache to complete the query, in which case its root -> edge -> edge -> leaf -> additionalPath...
	var path []rpc.Fact
	// build the path from leaf to root, then flip it
	for current := leaf; current != nil; current = current.parent {
		path = append(path, rpc.Fact{
			Index:     root.index,
			Id:        factID,
			Subject:   current.subject,
			Predicate: root.target.Predicate,
			Object:    obj,
		})
		obj = rpc.AKID(current.subject)
		factID = current.factID
	}
	// add the last step in the path for the root
	path = append(path, rpc.Fact{
		Index:     root.index,
		Id:        factID,
		Subject:   root.target.Subject,
		Predicate: root.target.Predicate,
		Object:    obj,
	})
	// the path was built leaf -> root, but we want to return it as root -> leaf, so reverse it
	for left, right := 0, len(path)-1; left < right; left, right = left+1, right-1 {
		path[left], path[right] = path[right], path[left]
	}
	// if there was any more path as a result of using the cache, add that in.
	path = append(path, additionalPath...)
	// cache our overall fact
	if resultingFact.Id == 0 {
		root.cache.AddInferedFact(&resultingFact, path)
	}
	// also we can infer facts for each item in the path to the target node as well
	// there are other facts we could infer from the path (e.g. from the root to each
	// item in the path) but we don't cache those at the moment
	for i, pathItem := range path {
		// skip the first item in the path, as that's the result we just added
		// also skip the last item, as the inferred fact will be the same as the actual fact
		if i > 0 && i < len(path)-1 {
			// pathItem is a copy, so its safe to mutate
			pathItem.Id = 0
			pathItem.Object = root.target.Object
			root.cache.AddInferedFact(&pathItem, path[i:])
		}
	}
}
