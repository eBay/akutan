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

package viewclient

import (
	"context"
	"io"

	"github.com/ebay/akutan/partitioning"
	rpcdef "github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/space"
	"github.com/ebay/akutan/util/parallel"
	"github.com/ebay/akutan/viewclient/fanout"
	opentracing "github.com/opentracing/opentracing-go"
)

// StreamingConvention documents the calling convention for streaming RPC
// results. It's not a type used in the code: this exists only for documentation.
//
// The function will send incremental results on resCh until either: it completes
// successfully and returns nil, or it encounters an error and returns the error.
// Note that the function may complete successfully without sending any results
// on resCh. Whether it completes successfully or returns an error, the function
// closes resCh before returning.
type StreamingConvention func(ctx context.Context, req interface{}, resCh chan interface{}) error

// LookupS calls out to views and collects all facts that match the given
// subjects. LookupS produces facts in an undefined order, but it guarantees the
// facts are distinct (it filters duplicate facts caused by RPC hedging and
// errors). The returned facts have their fields fully populated. See
// StreamingConvention for how to call LookupS.
func (c *Client) LookupS(ctx context.Context, req *rpcdef.LookupSRequest, resCh chan *rpcdef.LookupChunk) error {
	return lookupS(ctx, c.ReadFactsSPViews(), req, resCh)
}

// lookupS does the heavy lifting for Client.LookupS. It's split out for unit
// testing.
func lookupS(ctx context.Context, views fanout.Views, req *rpcdef.LookupSRequest, resCh chan *rpcdef.LookupChunk) error {
	// Currently views are partitioned on hash(s,p), so we need to fan out the
	// request to cover the entire hash space, then glue the results together.
	rpc := func(ctx context.Context, view fanout.View,
		offsets []int, results func(fanout.Result)) error {
		stream, err := view.(*ReadFactsSPClient).Stub.LookupS(ctx, req)
		if err != nil {
			return err
		}
		return drainStream(func() (fanout.Result, error) { return stream.Recv() }, results)
	}
	resultsCh := make(chan fanout.Chunk, 4)
	ranges := fanout.Partition(fullRange, views)

	wait := parallel.Go(func() {
		// This function gets results from the RPC calls, filters out duplicate results,
		// and sends the distinct results to 'resCh'. To filter duplicates, this assumes
		// that each view returns results in req.Subject order, and for each subject,
		// that returned facts are returned in (predicate, object) order.

		// cursors[i] tracks which facts have already been returned for ranges.Get(i).
		type Cursor struct {
			Lookup    uint32
			Predicate uint64
			Object    rpcdef.KGObject
		}
		cursors := make([]Cursor, len(ranges.StartPoints))
		less := func(a Cursor, b rpcdef.LookupChunk_Fact) bool {
			if a == (Cursor{}) {
				return true
			}
			if a.Lookup < b.Lookup {
				return true
			}
			if b.Lookup < a.Lookup {
				return false
			}
			if a.Predicate < b.Fact.Predicate {
				return true
			}
			if b.Fact.Predicate < a.Predicate {
				return false
			}
			return kgObjectLess(a.Object, b.Fact.Object)
		}

		for chunk := range resultsCh {
			thisRes := chunk.Result.(*rpcdef.LookupChunk)
			all := thisRes.Facts
			thisRes.Facts = thisRes.Facts[:0]
			for _, fact := range all {
				lookup := int(fact.Lookup)
				fact.Fact.Subject = req.Subjects[lookup]
				hash := partitioning.HashSP(fact.Fact.Subject, fact.Fact.Predicate)
				cursor := &cursors[ranges.Find(hash)]
				if less(*cursor, fact) {
					*cursor = Cursor{
						Lookup:    fact.Lookup,
						Predicate: fact.Fact.Predicate,
						Object:    fact.Fact.Object,
					}
					thisRes.Facts = append(thisRes.Facts, fact)
				}
			}
			resCh <- thisRes
		}
		close(resCh)
	})
	err := fanout.Call(ctx, ranges.StartPoints, views, rpc, resultsCh)
	wait()
	return err
}

func kgObjectLess(a, b rpcdef.KGObject) bool {
	return a.Less(b)
}

// LookupSP calls out to views and collects all facts that match the given
// subject-predicates. LookupSP produces facts in an undefined order, but it
// guarantees the facts are distinct (it filters duplicate facts caused byRPC
// hedging and errors). The returned facts have their fields fully populated.
// See StreamingConvention for how to call LookupSP.
func (c *Client) LookupSP(ctx context.Context, req *rpcdef.LookupSPRequest, resCh chan *rpcdef.LookupChunk) error {
	return lookupSP(ctx, c.ReadFactsSPViews(), req, resCh)
}

// lookupSP does the heavy lifting for Client.LookupSP. It's split out for unit
// testing.
func lookupSP(ctx context.Context, views fanout.Views, overallReq *rpcdef.LookupSPRequest, resCh chan *rpcdef.LookupChunk) error {
	// The overall request contains N lookups (SPs). The offsets used throughout
	// this function are indexes into the slice of lookups.
	span, ctx := opentracing.StartSpanFromContext(ctx, "client.lookup_sp")
	defer span.Finish()
	span.SetTag("objects", len(overallReq.Lookups))

	rpc := func(ctx context.Context, view fanout.View, offsets []int, results func(fanout.Result)) error {
		thisReq := &rpcdef.LookupSPRequest{
			Index:   overallReq.Index,
			Lookups: make([]rpcdef.LookupSPRequest_Item, len(offsets)),
		}
		for i, offset := range offsets {
			thisReq.Lookups[i] = overallReq.Lookups[offset]
		}
		stream, err := view.(*ReadFactsSPClient).Stub.LookupSP(ctx, thisReq)
		if err != nil {
			return err
		}
		return drainStream(func() (fanout.Result, error) { return stream.Recv() }, results)
	}
	hashes := make([]space.Point, len(overallReq.Lookups))
	for i, lookup := range overallReq.Lookups {
		hashes[i] = partitioning.HashSP(lookup.Subject, lookup.Predicate)
	}
	resultsCh := make(chan fanout.Chunk, 4)
	wait := parallel.Go(func() {
		// This function gets results from the RPC calls, filters out duplicate results,
		// and sends the distinct results to 'resCh'. To filter duplicates, this assumes
		// that each view returns results in req.Lookups order, and for each lookup,
		// that returned facts are returned in ascending object order.

		// cursors[i] is the last object returned for overallReq.Lookups[i].
		cursors := make([]rpcdef.KGObject, len(overallReq.Lookups))
		for chunk := range resultsCh {
			thisRes := chunk.Result.(*rpcdef.LookupChunk)
			all := thisRes.Facts
			thisRes.Facts = thisRes.Facts[:0]
			for _, fact := range all {
				fact.Lookup = uint32(chunk.Offsets[fact.Lookup])
				lookup := overallReq.Lookups[fact.Lookup]
				fact.Fact.Subject = lookup.Subject
				fact.Fact.Predicate = lookup.Predicate
				cursor := &cursors[fact.Lookup]
				if cursor.IsType(rpcdef.KtNil) || kgObjectLess(*cursor, fact.Fact.Object) {
					*cursor = fact.Fact.Object
					thisRes.Facts = append(thisRes.Facts, fact)
				}
			}
			resCh <- thisRes
		}
		close(resCh)
	})
	err := fanout.Call(ctx, hashes, views, rpc, resultsCh)
	wait()
	return err
}

// LookupSPO calls out to views and collects all facts that match the given
// subject-predicate-objects. LookupSPO produces facts in an undefined order,
// but it guarantees the facts are distinct (it filters duplicate facts caused
// by RPC hedging and errors). The returned facts have their fields fully
// populated. See StreamingConvention for how to call LookupSP.
func (c *Client) LookupSPO(ctx context.Context, req *rpcdef.LookupSPORequest, resCh chan *rpcdef.LookupChunk) error {
	return lookupSPO(ctx, c.ReadFactsSPViews(), req, resCh)
}

// lookupSPO does the heavy lifting for Client.LookupSPO. It's split out for
// unit testing.
func lookupSPO(ctx context.Context, views fanout.Views, overallReq *rpcdef.LookupSPORequest, resCh chan *rpcdef.LookupChunk) error {
	// The overall request contains N lookups (SPOs). The offsets used throughout
	// this function are indexes into the slice of lookups. We use the hash(S, P)
	// views here, although the hash(PO) views would be equally good.
	span, ctx := opentracing.StartSpanFromContext(ctx, "client.lookup_spo")
	defer span.Finish()
	span.SetTag("objects", len(overallReq.Lookups))
	rpc := func(ctx context.Context, view fanout.View, offsets []int, results func(fanout.Result)) error {
		thisReq := &rpcdef.LookupSPORequest{
			Index:   overallReq.Index,
			Lookups: make([]rpcdef.LookupSPORequest_Item, len(offsets)),
		}
		for i, offset := range offsets {
			thisReq.Lookups[i] = overallReq.Lookups[offset]
		}
		stream, err := view.(*ReadFactsSPClient).Stub.LookupSPO(ctx, thisReq)
		if err != nil {
			return err
		}
		return drainStream(func() (fanout.Result, error) { return stream.Recv() }, results)
	}
	hashes := make([]space.Point, len(overallReq.Lookups))
	for i, lookup := range overallReq.Lookups {
		hashes[i] = partitioning.HashSP(lookup.Subject, lookup.Predicate)
	}
	resultsCh := make(chan fanout.Chunk, 4)
	wait := parallel.Go(func() {
		// This function gets results from the RPC calls, filters out duplicate results,
		// and sends the distinct results to 'resCh'. To filter duplicates, this assumes
		// that each view returns results in req.Lookups order, and for each lookup,
		// at most one fact is returned.

		// cursors[i] is true if a fact has already been returned for
		// overallReq.Lookups[i].
		cursors := make([]bool, len(overallReq.Lookups))
		for chunk := range resultsCh {
			thisRes := chunk.Result.(*rpcdef.LookupChunk)
			all := thisRes.Facts
			thisRes.Facts = thisRes.Facts[:0]
			for _, fact := range all {
				fact.Lookup = uint32(chunk.Offsets[fact.Lookup])
				lookup := overallReq.Lookups[fact.Lookup]
				fact.Fact.Subject = lookup.Subject
				fact.Fact.Predicate = lookup.Predicate
				fact.Fact.Object = lookup.Object
				if !cursors[fact.Lookup] {
					cursors[fact.Lookup] = true
					thisRes.Facts = append(thisRes.Facts, fact)
				}
			}
			resCh <- thisRes
		}
		close(resCh)
	})
	err := fanout.Call(ctx, hashes, views, rpc, resultsCh)
	wait()
	return err
}

// LookupPO calls out to views and collects all facts that match the given
// predicate-objects. LookupPO produces facts in an undefined order, but it
// guarantees the facts are distinct (it filters duplicate facts caused by RPC
// hedging and errors). The returned facts have their fields fully populated.
// See StreamingConvention for how to call LookupPO.
func (c *Client) LookupPO(ctx context.Context, req *rpcdef.LookupPORequest, resCh chan *rpcdef.LookupChunk) error {
	return lookupPO(ctx, c.ReadFactsPOViews(), req, resCh)
}

// lookupPO does the heavy lifting for Client.LookupPO. It's split out for unit
// testing.
func lookupPO(ctx context.Context, views fanout.Views, overallReq *rpcdef.LookupPORequest, resCh chan *rpcdef.LookupChunk) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "client.lookup_po")
	defer span.Finish()
	span.SetTag("count", len(overallReq.Lookups))
	// The overall request contains 1 predicate and N objects. The offsets used
	// throughout this function are indexes into the slice of objects.
	rpc := func(ctx context.Context, view fanout.View, offsets []int, results func(fanout.Result)) error {
		thisReq := &rpcdef.LookupPORequest{
			Index:   overallReq.Index,
			Lookups: make([]rpcdef.LookupPORequest_Item, len(offsets)),
		}
		for i, offset := range offsets {
			thisReq.Lookups[i] = overallReq.Lookups[offset]
		}
		stream, err := view.(*ReadFactsPOClient).Stub.LookupPO(ctx, thisReq)
		if err != nil {
			return err
		}
		return drainStream(func() (fanout.Result, error) { return stream.Recv() }, results)
	}
	hashes := make([]space.Point, len(overallReq.Lookups))
	for i, item := range overallReq.Lookups {
		hashes[i] = partitioning.HashPO(item.Predicate, item.Object)
	}
	resultsCh := make(chan fanout.Chunk, 4)
	wait := parallel.Go(func() {
		// This function gets results from the RPC calls, filters out duplicate results,
		// and sends the distinct results to 'resCh'. To filter duplicates, this assumes
		// that each view returns results in req.Lookups order, and for each lookup,
		// that returned facts are returned in ascending subject OID order.

		// cursors[i] is the last subject returned for overallReq.Lookups[i].
		cursors := make([]uint64, len(overallReq.Lookups))
		for chunk := range resultsCh {
			thisRes := chunk.Result.(*rpcdef.LookupChunk)
			all := thisRes.Facts
			thisRes.Facts = thisRes.Facts[:0]
			for _, fact := range all {
				fact.Lookup = uint32(chunk.Offsets[fact.Lookup])
				if cursors[fact.Lookup] < fact.Fact.Subject {
					cursors[fact.Lookup] = fact.Fact.Subject
					po := overallReq.Lookups[fact.Lookup]
					fact.Fact.Predicate = po.Predicate
					fact.Fact.Object = po.Object
					thisRes.Facts = append(thisRes.Facts, fact)
				}
			}
			resCh <- thisRes
		}
		close(resCh)
	})
	err := fanout.Call(ctx, hashes, views, rpc, resultsCh)
	wait()
	return err
}

// LookupPOCmp calls out to views and collects all facts that match the given
// predicate and the given object criteria. LookupPOCmp produces facts in an
// undefined order, but it guarantees the facts are distinct (it filters
// duplicate facts caused by RPC hedging and errors). The returned facts have
// their fields fully populated. See StreamingConvention for how to call
// LookupPOcmp.
func (c *Client) LookupPOCmp(ctx context.Context, req *rpcdef.LookupPOCmpRequest, resCh chan *rpcdef.LookupChunk) error {
	return lookupPOCmp(ctx, c.ReadFactsPOViews(), req, resCh)
}

// lookupPO does the heavy lifting for Client.LookupPO. It's split out for unit
// testing.
func lookupPOCmp(ctx context.Context, views fanout.Views, req *rpcdef.LookupPOCmpRequest, resCh chan *rpcdef.LookupChunk) error {
	// Currently views are partitioned on hash(p,o), so we need to fan out the
	// request to cover the entire hash space, then glue the results together.
	rpc := func(ctx context.Context, view fanout.View,
		offsets []int, results func(fanout.Result)) error {
		stream, err := view.(*ReadFactsPOClient).Stub.LookupPOCmp(ctx, req)
		if err != nil {
			return err
		}
		return drainStream(func() (fanout.Result, error) { return stream.Recv() }, results)
	}
	resultsCh := make(chan fanout.Chunk, 4)
	ranges := fanout.Partition(fullRange, views)
	wait := parallel.Go(func() {
		// This function gets results from the RPC calls, filters out duplicate results,
		// and sends the distinct results to 'resCh'. To filter duplicates, this assumes
		// that each view returns results in req.Lookups order, and for each lookup,
		// that returned facts are returned in (object, subject) order.

		// cursors[i] tracks which facts have already been returned for ranges.Get(i).
		type Cursor struct {
			lookup  uint32
			object  rpcdef.KGObject
			subject uint64
		}
		cursors := make([]Cursor, len(ranges.StartPoints))
		less := func(a Cursor, b rpcdef.LookupChunk_Fact) bool {
			if a == (Cursor{}) {
				return true
			}
			if a.lookup < b.Lookup {
				return true
			}
			if a.lookup > b.Lookup {
				return false
			}
			if kgObjectLess(a.object, b.Fact.Object) {
				return true
			}
			if kgObjectLess(b.Fact.Object, a.object) {
				return false
			}
			return a.subject < b.Fact.Subject
		}

		for chunk := range resultsCh {
			thisRes := chunk.Result.(*rpcdef.LookupChunk)
			all := thisRes.Facts
			thisRes.Facts = thisRes.Facts[:0]
			for _, fact := range all {
				fact.Fact.Predicate = req.Lookups[fact.Lookup].Predicate
				hash := partitioning.HashPO(fact.Fact.Predicate, fact.Fact.Object)
				cursor := &cursors[ranges.Find(hash)]
				if less(*cursor, fact) {
					*cursor = Cursor{
						lookup:  fact.Lookup,
						object:  fact.Fact.Object,
						subject: fact.Fact.Subject,
					}
					thisRes.Facts = append(thisRes.Facts, fact)
				}
			}
			resCh <- thisRes
		}
		close(resCh)
	})
	err := fanout.Call(ctx, ranges.StartPoints, views, rpc, resultsCh)
	wait()
	return err
}

// fullRange is the range for an entire 64-bit hash space.
var fullRange = space.Range{
	Start: space.Hash64(0),
	End:   space.Infinity,
}

// drainStream reads from recv until EOF, handing results to the given callback.
// If recv returns an error that isn't io.EOF, this is returned. Otherwise,
// returns nil once recv has completed.
func drainStream(recv func() (fanout.Result, error), results func(fanout.Result)) error {
	for {
		res, err := recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		results(res)
	}
}
