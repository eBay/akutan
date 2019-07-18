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

// Package update handles requests to modify the graph.
//
// Note: this package currently only handles Insert, but lots of things are
// called Update so that they can be generalized to handle Delete and combined
// Delete+Insert requests later.
package update

import (
	"context"
	"fmt"

	"github.com/ebay/akutan/api"
	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/logentry"
	"github.com/ebay/akutan/logentry/logencoder"
	"github.com/ebay/akutan/logentry/logread"
	wellknown "github.com/ebay/akutan/msg/facts"
	"github.com/ebay/akutan/query/parser"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/update/conv"
	"github.com/ebay/akutan/util/parallel"
	"github.com/ebay/akutan/viewclient/lookups"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
)

// An updateRequest corresponds to a single api.InsertRequest and its derived
// data.
type updateRequest struct {
	// Each inserted parser.Quad becomes one insertFact, in the same order.
	facts []insertFact
	// A set of all the external IDs either referenced or assigned by any of the
	// facts.
	allXIDs map[string]struct{}
}

// A user-supplied fact to be inserted.
type insertFact struct {
	// The result of the parser.
	quad *parser.Quad

	// Every external ID (parser.Entity or parser.QName string) referenced
	// in the quad. This is in no particular order and may include duplicates.
	referencesXIDs []string

	// If the fact manually assigns an external ID to its subject,
	// this is that external ID. Otherwise, this is the empty string.
	assignsXID string
}

// An updateReadSet contains all the lookup results for a particular update
// request at a particular log index.
type updateReadSet struct {
	// When the results were read.
	index blog.Index
	// Maps from external IDs to KIDs or 0. This includes all the external IDs
	// either referenced in the facts or manually assigned in the facts. A
	// non-zero KID in this map indicates there existed a fact:
	//     ?value <HasExternalID> "key"
	// A zero value in this map indicates there was no fact matching the
	// pattern.
	xidToKID map[string]uint64
	// Maps from the offset of updateRequest.facts to KIDs or 0. A non-zero
	// value at index i indicates there existed a fact:
	//     ?value update.facts[i].quad.Subject .quad.Predicate .quad.Object
	// A zero value at index i indicates there was no fact matching the pattern.
	existingFactIDs []uint64
}

// Store is the subset of lookups.All that updates need. This makes it easier to
// write unit tests.
type Store interface {
	lookups.PO  // to look up external IDs
	lookups.SPO // to look up existing facts
}

// ALog is the subset of blog.AkutanLog that updates need. This makes it easier
// to write unit tests.
type ALog interface {
	AppendSingle(ctx context.Context, data []byte) (blog.Index, error)
}

// Insert handles an api.InsertRequest. recentIdx is a recent log index used to
// read the supporting data in the initial attempt; if recentIdx is too stale,
// Insert will waste some effort.
func Insert(ctx context.Context, req *api.InsertRequest,
	recentIdx blog.Index, store Store, aLog ALog,
) (*api.InsertResult, error) {
	update, res := newInsertRequest(req)
	if res != nil {
		return res, nil
	}
	reads, err := update.read(ctx, store, recentIdx)
	if err != nil {
		return nil, err
	}
	attempt := 1
	for {
		res, newReads, err := update.attempt(ctx, attempt, reads, store, aLog)
		if err != nil {
			return nil, err
		}
		if res != nil {
			return res, nil
		}
		reads = newReads
		attempt++
	}
}

// attempt tries to update the graph using one transaction. If it does not
// succeed, the caller is expected to retry with increasing 'attempt' values.
// The given 'reads' are supporting lookups that were executed against a recent
// log index.
//
// If attempt returns a non-nil error or a non-nil api.InsertResult, that's the
// final outcome. The caller should not call attempt again.
//
// If attempt returns a nil error and a nil api.InsertResult, the transaction
// did not succeed but should be retried. In this case, attempt also returns a
// non-nil readSet at least as recent as the one it was given. The caller should
// attempt the update again using this readSet.
func (update *updateRequest) attempt(ctx context.Context, attempt int,
	reads *updateReadSet, store Store, aLog ALog,
) (*api.InsertResult, *updateReadSet, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	span, ctx := opentracing.StartSpanFromContext(ctx, "update_attempt")
	span.SetTag("attempt", attempt)
	defer span.Finish()
	logger := logrus.WithFields(logrus.Fields{
		"attempt":    attempt,
		"readsIndex": reads.index,
	})

	// Append a start transaction entry.
	txCmd, res := update.buildStartTxCommand(reads)
	if res != nil {
		return res, nil, nil
	}
	txIdx, err := appendInsertTxBegin(ctx, aLog, txCmd)
	if err != nil {
		return nil, nil, err
	}
	logger = logger.WithField("transactionIndex", txIdx)

	// Check if the read set changed.
	var newReads *updateReadSet
	switch {
	case reads.index >= txIdx:
		panic(fmt.Sprintf("Impossible: issued read at index %v, then appended to log at index %v",
			reads.index, txIdx))
	case reads.index == txIdx-1:
		newReads = reads
	default: // reads.Index < txIdx - 1
		newReads, err = update.read(ctx, store, txIdx-1)
		if err != nil {
			return nil, nil, err
		}
		logger = logger.WithField("newReadsIndex", newReads.index)
		if !reads.equalValues(newReads) {
			logger.Debug("Aborting attempt due to change in read set")
			_, err = appendTxDecide(ctx, aLog, txIdx, false)
			if err != nil {
				return nil, nil, err
			}
			return nil, newReads, nil
		}
	}

	// Try to commit the transaction.
	var aborted bool
	err = parallel.Invoke(ctx,
		func(ctx context.Context) error {
			_, err := appendTxDecide(ctx, aLog, txIdx, true)
			return err
		},
		func(ctx context.Context) error {
			// This will block until the transaction is no longer pending.
			a, err := wasExternallyAborted(ctx, store, txIdx, txCmd)
			aborted = a
			return err
		},
	)
	if err != nil {
		return nil, nil, err
	}
	if aborted {
		logger.Debug("Attempt was externally aborted")
		return nil, newReads, nil
	}
	return &api.InsertResult{
		Status: api.InsertStatus_OK,
		Index:  txIdx,
	}, nil, nil
}

// newInsertRequest extracts information about the request into a convenient
// format. Upon success, it returns a non-nil updateRequest and a nil
// InsertResult. Otherwise, it returns a nil updateRequest an a non-nil
// InsertResult, which should be returned to the client.
func newInsertRequest(req *api.InsertRequest) (*updateRequest, *api.InsertResult) {
	parsed, err := parser.ParseInsert(req.Format, req.Facts)
	if err != nil {
		return nil, &api.InsertResult{
			Status: api.InsertStatus_ParseError,
			Error:  err.Error(),
		}
	}
	update := &updateRequest{
		facts:   make([]insertFact, len(parsed.Facts)),
		allXIDs: make(map[string]struct{}, 2*len(parsed.Facts)),
	}
	for i, quad := range parsed.Facts {
		referencesXIDs := referencedExternalIDs(i, quad)
		assignsXID, res := assignsExternalID(i, quad)
		if res != nil {
			return nil, res
		}
		update.facts[i] = insertFact{
			quad:           quad,
			referencesXIDs: referencesXIDs,
			assignsXID:     assignsXID,
		}
		for _, xid := range referencesXIDs {
			update.allXIDs[xid] = struct{}{}
		}
		if assignsXID != "" {
			if _, found := update.allXIDs[assignsXID]; found {
				return nil, &api.InsertResult{
					Status: api.InsertStatus_SchemaViolation,
					Error: fmt.Sprintf("can't use and then manually assign external ID %q "+
						"(fact %d)",
						assignsXID, i+1),
				}
			}
			update.allXIDs[assignsXID] = struct{}{}
		}
	}
	return update, nil
}

// read looks up all the supporting values needed for an update request. The
// lookups are performed as of the given log index.
func (update *updateRequest) read(ctx context.Context, store Store, index blog.Index,
) (*updateReadSet, error) {
	allXIDs := make([]string, 0, len(update.allXIDs))
	for xid := range update.allXIDs {
		allXIDs = append(allXIDs, xid)
	}
	xidToKID, err := ResolveExternalIDs(ctx, store, index, allXIDs)
	if err != nil {
		return nil, err
	}
	existingFactIDs, err := update.lookupFacts(ctx, store, index, xidToKID)
	if err != nil {
		return nil, err
	}
	return &updateReadSet{
		index:           index,
		xidToKID:        xidToKID,
		existingFactIDs: existingFactIDs,
	}, nil
}

// lookupFacts determines which facts to be updated already exist. It executes
// the lookups at the given index. It returns a slice of the same length as
// update.facts. A value of 0 at offset i indicates update.facts[i] did not
// exist as of the given log index; a nonzero value indicates the fact ID of the
// existing fact.
func (update *updateRequest) lookupFacts(ctx context.Context, store lookups.SPO,
	index blog.Index, xidToKID map[string]uint64,
) ([]uint64, error) {
	// existingKIDs will be returned upon successful completion.
	existingKIDs := make([]uint64, len(update.facts))
	// If done[i] is true, this has already looked up facts[i].
	done := make([]bool, len(update.facts))
	// converter is used to resolve external IDs and variables in the update
	// request into KIDs for the RPC requests.
	converter := conv.ParserToRPC{
		ExternalIDs: xidToKID,
		Variables:   make(map[string]uint64),
	}

	// The first iteration of this loop looks up standard static facts. The
	// second iteration looks up metafacts. The third iteration looks up
	// meta-metafacts, etc.
	for {
		req := rpc.LookupSPORequest{
			Index:   index,
			Lookups: make([]rpc.LookupSPORequest_Item, 0, len(update.facts)),
		}
		lookupOffsetToFactOffset := make([]int, 0, len(update.facts))
		for offset, fact := range update.facts {
			if done[offset] {
				continue
			}
			subject, ready1 := converter.KID(fact.quad.Subject)
			predicate, ready2 := converter.KID(fact.quad.Predicate)
			object, ready3 := converter.KGObject(fact.quad.Object)
			if ready1 && ready2 && ready3 {
				req.Lookups = append(req.Lookups, rpc.LookupSPORequest_Item{
					Subject:   subject,
					Predicate: predicate,
					Object:    object,
				})
				lookupOffsetToFactOffset = append(lookupOffsetToFactOffset, offset)
				done[offset] = true
			}
		}
		if len(req.Lookups) == 0 {
			break
		}

		resCh := make(chan *rpc.LookupChunk, 4)
		again := false
		wait := parallel.Go(func() {
			for chunk := range resCh {
				for _, f := range chunk.Facts {
					factOffset := lookupOffsetToFactOffset[f.Lookup]
					existingKIDs[factOffset] = f.Fact.Id
					if id, ok := update.facts[factOffset].quad.ID.(*parser.Variable); ok {
						converter.Variables[id.Name] = existingKIDs[factOffset]
						again = true
					}
				}
			}
		})
		err := store.LookupSPO(ctx, &req, resCh)
		wait()
		if err != nil {
			return nil, err
		}
		if !again {
			break
		}
	}
	return existingKIDs, nil
}

// buildStartTxCommand creates a start-insert-transaction command to be appended
// to the log to satisfy an insert request. It returns a non-nil
// api.InsertResult if the insert has completed or failed with no need to append
// a command. Otherwise, it returns a non-nil InsertTxCommand to append to the
// log.
func (update *updateRequest) buildStartTxCommand(reads *updateReadSet,
) (*logentry.InsertTxCommand, *api.InsertResult) {
	// Dont access this directly; use the nextOffset function.
	nextOffsetToUse := uint64(1)
	nextOffset := func() int32 {
		r := int32(nextOffsetToUse)
		nextOffsetToUse++
		return r
	}
	converter := conv.ParserToLog{
		ExternalIDs: make(map[string]logentry.KIDOrOffset),
		Variables:   make(map[string]logentry.KIDOrOffset),
	}
	for xid, kid := range reads.xidToKID {
		if kid != 0 {
			converter.ExternalIDs[xid] = logentry.KIDOrOffset{
				Value: &logentry.KIDOrOffset_Kid{Kid: kid},
			}
		}
	}

	cmd := &logentry.InsertTxCommand{
		Facts: make([]logentry.InsertFact, 0, len(update.facts)),
	}
	for i, fact := range update.facts {
		// For existing facts, just capture their fact IDs into variables.
		if reads.existingFactIDs[i] != 0 {
			v, ok := fact.quad.ID.(*parser.Variable)
			if ok {
				if _, found := converter.Variables[v.Name]; found {
					// This should have been caught already by the parser's validations.
					panic(fmt.Sprintf("Variable %v assigned more than once", v.Name))
				}
				converter.Variables[v.Name] = logentry.KIDOrOffset{
					Value: &logentry.KIDOrOffset_Kid{
						Kid: reads.existingFactIDs[i],
					},
				}
			}
			continue
		}

		// Add facts for newly referenced external IDs.
		for _, xid := range fact.referencesXIDs {
			if _, ok := converter.ExternalIDs[xid]; ok {
				continue
			}
			factIDOffset := nextOffset()
			offset := nextOffset()
			converter.ExternalIDs[xid] = logentry.KIDOrOffset{
				Value: &logentry.KIDOrOffset_Offset{Offset: offset},
			}
			cmd.Facts = append(cmd.Facts, logentry.InsertFact{
				FactIDOffset: factIDOffset,
				Subject: logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Offset{
					Offset: offset,
				}},
				Predicate: logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{
					Kid: wellknown.HasExternalID,
				}},
				Object: logentry.KGObject{Value: &logentry.KGObject_AString{
					AString: xid,
				}},
			})
		}

		// Add the fact.
		offset := nextOffset()
		cmd.Facts = append(cmd.Facts, logentry.InsertFact{
			FactIDOffset: offset,
			Subject:      converter.MustKIDOrOffset(fact.quad.Subject),
			Predicate:    converter.MustKIDOrOffset(fact.quad.Predicate),
			Object:       converter.MustKGObject(fact.quad.Object),
		})

		// Capture fact offset into variable.
		v, isVar := fact.quad.ID.(*parser.Variable)
		if isVar {
			if _, found := converter.Variables[v.Name]; found {
				// This should have been caught already by the parser's validations.
				panic(fmt.Sprintf("Variable %v assigned more than once", v.Name))
			}
			converter.Variables[v.Name] = logentry.KIDOrOffset{
				Value: &logentry.KIDOrOffset_Offset{
					Offset: offset,
				},
			}
		}

		// Save manually assigned XID for future reference.
		if fact.assignsXID != "" {
			subject := converter.MustKIDOrOffset(fact.quad.Subject)
			existing, found := converter.ExternalIDs[fact.assignsXID]
			if found && !subject.Equal(existing) {
				return nil, &api.InsertResult{
					Status: api.InsertStatus_SchemaViolation,
					Error: fmt.Sprintf("can't manually re-assign external ID %q (fact %d)",
						fact.assignsXID, i+1),
				}
			}
			converter.ExternalIDs[fact.assignsXID] = subject
		}
	}

	if nextOffsetToUse > logread.MaxOffset {
		return nil, &api.InsertResult{
			Status: api.InsertStatus_AtomicRequestTooBig,
			Error: fmt.Sprintf("insert request too big: need %v KIDs but hard limit is %v",
				nextOffsetToUse-1, logread.MaxOffset-1),
		}
	}
	if len(cmd.Facts) == 0 {
		return nil, &api.InsertResult{
			Status: api.InsertStatus_OK,
			Index:  reads.index,
		}
	}
	return cmd, nil
}

// wasExternallyAborted is called to determine whether a transaction was really
// committed or whether some other process (the transaction timer view) aborted
// it first. 'txIdx' is the index at which 'txCmd' was appended to the log.
// The given txCmd must contain at least one fact.
//
// wasExternallyAborted returns true and an error if it could not confirm
// anything. Otherwise, it returns true if the transaction was certainly aborted
// or false if the transaction was certainly committed, along with a nil error.
func wasExternallyAborted(ctx context.Context, store lookups.SPO,
	txIdx blog.Index, txCmd *logentry.InsertTxCommand,
) (bool, error) {
	// Attempt a LookupSPO on an arbitrary fact from the txCmd. The view(s) will
	// block this lookup until the transaction has been decided.
	probe := logread.ToRPCFact(txIdx, &txCmd.Facts[0])
	req := rpc.LookupSPORequest{
		Index: txIdx,
		Lookups: []rpc.LookupSPORequest_Item{{
			Subject:   probe.Subject,
			Predicate: probe.Predicate,
			Object:    probe.Object,
		}},
	}
	resCh := make(chan *rpc.LookupChunk, 4)
	aborted := true
	wait := parallel.Go(func() {
		for chunk := range resCh {
			for range chunk.Facts {
				aborted = false
			}
		}
	})
	err := store.LookupSPO(ctx, &req, resCh)
	wait()
	if err != nil {
		return true, err
	}
	return aborted, nil
}

// appendInsertTxBegin appends a single transaction message to the log
// containing req, and returns the index of the entry.
func appendInsertTxBegin(ctx context.Context, aLog ALog,
	req *logentry.InsertTxCommand,
) (blog.Index, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "append startTx")
	span.SetTag("facts", len(req.Facts))
	defer span.Finish()
	enc := logencoder.Encode(req)
	return aLog.AppendSingle(ctx, enc)
}

// appendTxDecide appends a single tx decision message to the log and returns
// the index of the entry.
func appendTxDecide(ctx context.Context, aLog ALog,
	idx blog.Index, commit bool,
) (blog.Index, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "append decideTx")
	span.SetTag("commit", commit)
	defer span.Finish()
	dm := &logentry.TxDecisionCommand{Tx: idx, Commit: commit}
	enc := logencoder.Encode(dm)
	return aLog.AppendSingle(ctx, enc)
}

// equalValues returns true if the two readSets are identical except for their
// log indexes. The two reads must be for the same update request.
func (reads *updateReadSet) equalValues(other *updateReadSet) bool {
	if len(reads.xidToKID) != len(other.xidToKID) {
		panic(fmt.Sprintf("Two reads for the same update request must "+
			"have the same len XIDToKID map (got %v and %v)",
			len(reads.xidToKID), len(other.xidToKID)))
	}
	if len(reads.existingFactIDs) != len(other.existingFactIDs) {
		panic(fmt.Sprintf("Two reads for the same update request must "+
			"have the same len ExistingFactIDs (got %v and %v)",
			len(reads.existingFactIDs), len(other.existingFactIDs)))
	}
	for xid, kid := range reads.xidToKID {
		if other.xidToKID[xid] != kid {
			return false
		}
	}
	for offset, kid := range reads.existingFactIDs {
		if other.existingFactIDs[offset] != kid {
			return false
		}
	}
	return true
}
