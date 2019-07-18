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

package diskview

import (
	"context"
	"fmt"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/diskview/database"
	"github.com/ebay/akutan/diskview/keys"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/tracing"
	"github.com/google/btree"
	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
)

// lookupChunkSize defines the maximum number of facts to send to the client in
// one LookupChunk response.
const lookupChunkSize = 256

// An enumerateSpec describes a key iteration.
type enumerateSpec struct {
	// No key < startKey will be emitted.
	startKey []byte
	// No key >= endKey will be emitted.
	endKey []byte
	// If not nil, a predicate that returns true for keys that should be emitted and
	// false for keys that should be skipped over.
	filter func(key []byte, fact rpc.Fact) bool
	// Where to send the keys.
	emit factCallback
}

// lookup orchestrates running a list of enumerations. It waits until the
// database reflects all necessary log entries and pending transactions, then
// executes the enumerations.
func (view *DiskView) lookup(ctx context.Context, index blog.Index, enumerations []enumerateSpec) error {
	// Wait for the log index to be applied.
	err := view.waitForIndex(ctx, index)
	if err != nil {
		return err
	}

	// Gather up transactions that affect the results and aren't yet decided.
	conflicting := make(map[blog.Index]*transaction)
	view.lock.RLock()
	for _, spec := range enumerations {
		view.pending.AscendRange(pendingItem{key: spec.startKey}, pendingItem{key: spec.endKey},
			func(btreeItem btree.Item) bool {
				item := btreeItem.(pendingItem)
				if item.tx.position.Index <= index {
					conflicting[item.tx.position.Index] = item.tx
				}
				return true
			})
	}
	view.lock.RUnlock()

	// Wait for those conflicting transactions to be decided.
	for _, tx := range conflicting {
		select {
		case <-tx.decided:
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Enumerate against a current snapshot.
	snap := view.db.Snapshot()
	defer snap.Close()
	for _, spec := range enumerations {
		err := enumerateDB(snap, spec, index)
		if err != nil {
			return err
		}
	}
	return nil
}

// A keyEnumerator is the minimal requirements on database.Snapshot for enumerateDB. It's used for unit tests.
type keyEnumerator interface {
	EnumerateKeys(startKey, endKey []byte, emit func(key []byte) error) error
}

// enumerateDB looks for facts matching spec's criteria and sends the latest
// ones up through maxIndex to 'spec.emit'.
func enumerateDB(snap keyEnumerator, spec enumerateSpec, maxIndex blog.Index) error {
	var out factCallback
	if spec.filter == nil {
		out = spec.emit
	} else {
		out = func(key []byte, fact rpc.Fact) error {
			if spec.filter(key, fact) {
				return spec.emit(key, fact)
			}
			return nil
		}
	}
	filterCb := newLatestIndexFilter(maxIndex, parseKeys(out))
	defer filterCb.Close()
	return snap.EnumerateKeys(spec.startKey, spec.endKey, filterCb.Apply)
}

// LookupPO implements the gRPC method ReadFactsPO.LookupPO. The real work
// happens in lookupPO, which is easier to unit test.
func (view *DiskView) LookupPO(req *rpc.LookupPORequest, resServer rpc.ReadFactsPO_LookupPOServer) error {
	return view.lookupPO(resServer.Context(), req, resServer.Send)
}

// lookupPO does the heavy lifting for LookupPO.
func (view *DiskView) lookupPO(ctx context.Context, req *rpc.LookupPORequest, stream rpc.ChunkReadyCallback) error {
	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		span.SetTag("lookups", len(req.Lookups))
	}
	sink := rpc.NewFactSink(stream, lookupChunkSize)
	enumerations := make([]enumerateSpec, len(req.Lookups))
	for offset := range req.Lookups {
		offset := offset // gotcha: closure needs its own offset variable
		lookup := req.Lookups[offset]
		keyPrefix := keys.KeyPrefixPredicateObjectNoLang(lookup.Predicate, lookup.Object)
		enumerations[offset] = enumerateSpec{
			startKey: keyPrefix,
			endKey:   incremented(keyPrefix),
			filter: func(key []byte, fact rpc.Fact) bool {
				// Note that the encoding for object doesn't include the length and so a keyPrefix for
				// p=1 o.a_string='Bob'
				// will have the same prefix as p=1 o.a_string='Bobs House'
				// so we need to check that the returned object is one we care about.
				return fact.Object.Equal(lookup.Object)
			},
			emit: func(key []byte, fact rpc.Fact) error {
				// No need to send Predicate and Object back.
				fact.Predicate = 0
				fact.Object = rpc.KGObject{}
				return sink.Write(offset, fact)
			},
		}
	}
	err := view.lookup(ctx, req.Index, enumerations)
	if err != nil {
		return err
	}
	return sink.Flush()
}

// LookupS implements the gRPC method ReadFactsSP.LookupS.
func (view *DiskView) LookupS(req *rpc.LookupSRequest, resServer rpc.ReadFactsSP_LookupSServer) error {
	return view.lookupS(resServer.Context(), req, resServer.Send)
}

// lookupS does the heavy lifting for LookupS.
func (view *DiskView) lookupS(ctx context.Context, req *rpc.LookupSRequest, stream rpc.ChunkReadyCallback) error {
	sink := rpc.NewFactSink(stream, lookupChunkSize)
	enumerations := make([]enumerateSpec, len(req.Subjects))
	for offset := range req.Subjects {
		offset := offset // gotcha: closure needs its own offset variable
		keyPrefix := keys.KeyPrefixSubject(req.Subjects[offset])
		enumerations[offset] = enumerateSpec{
			startKey: keyPrefix,
			endKey:   incremented(keyPrefix),
			emit: func(key []byte, fact rpc.Fact) error {
				// No need to send Subject back.
				fact.Subject = 0
				return sink.Write(offset, fact)
			},
		}
	}
	err := view.lookup(ctx, req.Index, enumerations)
	if err != nil {
		return err
	}
	return sink.Flush()
}

// LookupSP implements the gRPC method ReadFactsSP.LookupSP.
func (view *DiskView) LookupSP(req *rpc.LookupSPRequest, resServer rpc.ReadFactsSP_LookupSPServer) error {
	return view.lookupSP(resServer.Context(), req, resServer.Send)
}

// lookupSP does the heavy lifting for LookupSP. It's split out for unit testing.
func (view *DiskView) lookupSP(ctx context.Context, req *rpc.LookupSPRequest, stream rpc.ChunkReadyCallback) error {
	sink := rpc.NewFactSink(stream, lookupChunkSize)
	enumerations := make([]enumerateSpec, len(req.Lookups))
	for offset := range req.Lookups {
		offset := offset // gotcha: closure needs its own offset variable
		keyPrefix := keys.KeyPrefixSubjectPredicate(
			req.Lookups[offset].Subject,
			req.Lookups[offset].Predicate,
		)
		enumerations[offset] = enumerateSpec{
			startKey: keyPrefix,
			endKey:   incremented(keyPrefix),
			emit: func(key []byte, fact rpc.Fact) error {
				// No need to send subject or predicate back.
				fact.Subject = 0
				fact.Predicate = 0
				return sink.Write(offset, fact)
			},
		}
	}
	err := view.lookup(ctx, req.Index, enumerations)
	if err != nil {
		return err
	}
	return sink.Flush()
}

// LookupSPO implements the gRPC method ReadFactsSP.LookupSPO.
func (view *DiskView) LookupSPO(req *rpc.LookupSPORequest, resServer rpc.ReadFactsSP_LookupSPOServer) error {
	return view.lookupSPO(resServer.Context(), req, resServer.Send)
}

// lookupSPO does the heavy lifting for LookupSPO. It's split out for unit testing.
func (view *DiskView) lookupSPO(ctx context.Context, req *rpc.LookupSPORequest, stream rpc.ChunkReadyCallback) error {
	sink := rpc.NewFactSink(stream, lookupChunkSize)
	enumerations := make([]enumerateSpec, len(req.Lookups))
	for offset := range req.Lookups {
		offset := offset // gotcha: closure needs its own offset variable
		lookup := req.Lookups[offset]
		keyPrefix := keys.KeyPrefixSubjectPredicateObjectNoLang(
			lookup.Subject,
			lookup.Predicate,
			lookup.Object,
		)
		enumerations[offset] = enumerateSpec{
			startKey: keyPrefix,
			endKey:   incremented(keyPrefix),
			filter: func(key []byte, fact rpc.Fact) bool {
				// Need this check due to string encoding; see LookupPO for the explanation.
				return fact.Object.Equal(lookup.Object)
			},
			emit: func(key []byte, fact rpc.Fact) error {
				// No need to send back subjet, predicate, or object.
				fact.Subject = 0
				fact.Predicate = 0
				fact.Object = rpc.KGObject{}
				err := sink.Write(offset, fact)
				if err != nil {
					return err
				}
				return database.ErrHalt
			},
		}
	}
	err := view.lookup(ctx, req.Index, enumerations)
	if err != nil {
		return err
	}
	return sink.Flush()
}

// LookupPOCmp implements the gRPC method ReadFactsPO.LookupPO.
func (view *DiskView) LookupPOCmp(req *rpc.LookupPOCmpRequest, resServer rpc.ReadFactsPO_LookupPOCmpServer) error {
	return view.lookupPOCmp(resServer.Context(), req, resServer.Send)
}

// lookupPOCmp does the heavy lifting for LookupPOCmp. It's split out for unit testing.
func (view *DiskView) lookupPOCmp(ctx context.Context, req *rpc.LookupPOCmpRequest, stream rpc.ChunkReadyCallback) error {
	sink := rpc.NewFactSink(stream, lookupChunkSize)
	enumerations := make([]enumerateSpec, len(req.Lookups))
	for offset := range req.Lookups {
		offset := offset // gotcha: closure needs its own offset variable
		spec := poCmpSpec(req.Lookups[offset], req.Index)
		spec.emit = func(key []byte, fact rpc.Fact) error {
			// No need to send predicate back.
			fact.Predicate = 0
			return sink.Write(offset, fact)
		}
		enumerations[offset] = spec
	}
	err := view.lookup(ctx, req.Index, enumerations)
	if err != nil {
		return err
	}
	return sink.Flush()
}

// increment adds one to the last byte, carrying if needed. Panics if the slice contains all 0xFF bytes.
// This is useful for iterating up to and including x.
func increment(x []byte) {
	for i := len(x) - 1; i >= 0; i-- {
		if x[i] < 255 {
			x[i]++
			return
		}
		x[i] = 0
	}
	panic("overflow: can't increment all 1s")
}

// incremented is like increment but returns an incremented copy of the given slice.
func incremented(x []byte) []byte {
	x = append([]byte(nil), x...)
	increment(x)
	return x
}

// poCmpSpec translates the given criteria into an enumeration spec.
func poCmpSpec(req rpc.LookupPOCmpRequest_Item, maxIndex blog.Index) enumerateSpec {
	keyP := keys.KeyPrefixPredicateObjectType(req.Predicate, req.Object)
	keyPO := keys.KeyPrefixPredicateObjectNoLang(req.Predicate, req.Object)
	var endKeyPO []byte
	if (req.EndObject != rpc.KGObject{}) {
		endKeyPO = keys.KeyPrefixPredicateObjectNoLang(req.Predicate, req.EndObject)
	}

	switch req.Operator {
	case rpc.OpEqual:
		return enumerateSpec{
			startKey: keyPO,
			endKey:   incremented(keyPO),
			// Need this check due to string encoding; see LookupPO for the explanation.
			filter: func(key []byte, fact rpc.Fact) bool {
				return fact.Object.Equal(req.Object)
			},
		}
	case rpc.OpLess:
		return enumerateSpec{startKey: keyP, endKey: keyPO}
	case rpc.OpLessOrEqual:
		increment(keyPO)
		return enumerateSpec{startKey: keyP, endKey: keyPO}
	case rpc.OpGreater:
		increment(keyPO)
		increment(keyP)
		return enumerateSpec{startKey: keyPO, endKey: keyP}
	case rpc.OpGreaterOrEqual:
		increment(keyP)
		return enumerateSpec{startKey: keyPO, endKey: keyP}
	case rpc.OpRangeIncInc:
		increment(endKeyPO)
		return enumerateSpec{startKey: keyPO, endKey: endKeyPO}
	case rpc.OpRangeIncExc:
		return enumerateSpec{startKey: keyPO, endKey: endKeyPO}
	case rpc.OpRangeExcInc:
		increment(keyPO)
		increment(endKeyPO)
		return enumerateSpec{startKey: keyPO, endKey: endKeyPO}
	case rpc.OpRangeExcExc:
		increment(keyPO)
		return enumerateSpec{startKey: keyPO, endKey: endKeyPO}
	case rpc.OpPrefix:
		return enumerateSpec{startKey: keyPO, endKey: incremented(keyPO)}
	default:
		panic(fmt.Sprintf("Unknown operator %v", req.Operator))
	}
}

// latestIndexFilter takes a stream of keys and, for each fact, returns only
// the latest key up through the given log index.
// TODO: That doesn't really make sense, however, since facts are immutable.
type latestIndexFilter struct {
	maxIndex blog.Index
	// Keys that pass the filter are sent here.
	next keyCallback
	// The last key seen that was <= maxIndex, or nil if it's already been flushed.
	prevKey []byte
	// The last result from calling 'next'.
	nextState error
}

func newLatestIndexFilter(maxIndex blog.Index, next keyCallback) latestIndexFilter {
	return latestIndexFilter{
		maxIndex:  maxIndex,
		next:      next,
		nextState: nil,
	}
}

func (f *latestIndexFilter) Apply(k []byte) error {
	overIndex := keys.ParseIndex(k) > f.maxIndex
	newKey := !keys.FactKeysEqualIgnoreIndex(k, f.prevKey)
	var res error
	if overIndex || newKey {
		res = f.flushPrevKey()
	}
	if !overIndex {
		f.prevKey = k
	}
	return res
}

func (f *latestIndexFilter) Close() {
	f.flushPrevKey()
}

func (f *latestIndexFilter) flushPrevKey() error {
	if len(f.prevKey) == 0 || f.nextState == database.ErrHalt {
		return f.nextState
	}
	f.nextState = f.next(f.prevKey)
	f.prevKey = nil
	return f.nextState
}

// A keyCallback receives a stream of binary keys. Returns nil to continue, or
// database.ErrHalt or another error to stop.
type keyCallback func(k []byte) error

// A factCallback receives a stream of binary keys and the facts they encode.
// Returns nil to continue, or database.ErrHalt or another error to stop.
type factCallback func(k []byte, f rpc.Fact) error

// parseKeys parses a binary key for use with a factCallback. Given a
// factCallback, factCallbackAdapter returns a function that takes a binary key,
// parses it, and runs the fact callback with the binary key and the fact it
// encodes.
func parseKeys(cb factCallback) keyCallback {
	return func(k []byte) error {
		factKey, err := keys.ParseKey(k)
		if err != nil {
			log.Warnf("Unable to parse key %v: %v", k, err)
			return nil
		}
		return cb(k, *factKey.(keys.FactKey).Fact)
	}
}

// waitForIndex blocks until the given log index has been applied. Returns nil
// after the given log index was applied, or an error if the context expires.
func (view *DiskView) waitForIndex(ctx context.Context, targetIndex blog.Index) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "waitForIndex")
	tracing.UpdateMetric(span, metrics.lookupStallForLogEntriesSeconds)
	defer span.Finish()
	if targetIndex == 0 && view.LastApplied() > 0 {
		return fmt.Errorf("invalid target index: %d", targetIndex)
	}
	appliedIndex := view.LastApplied()
	span.SetTag("startingIndex", appliedIndex)
	span.SetTag("targetIndex", targetIndex)
	for targetIndex > appliedIndex {
		// TODO: This is a bit heavy-weight: rethink use of condition variable.
		ch := make(chan blog.Index, 1)
		go func() {
			view.lock.RLock()
			// Skip waiting if the atIndex has already advanced.
			if view.nextPosition.Index-1 == appliedIndex {
				view.updateCond.Wait()
			}
			ch <- view.nextPosition.Index - 1
			view.lock.RUnlock()
		}()
		select {
		case appliedIndex = <-ch:
		case <-ctx.Done():
			log.Debugf("Timed out waiting for log processing to reach the requested index %d, currently at %d",
				targetIndex, appliedIndex)
			return ctx.Err()
		}
	}
	return nil
}
