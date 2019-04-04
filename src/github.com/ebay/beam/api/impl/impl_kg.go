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

package impl

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ebay/beam/api"
	"github.com/ebay/beam/blog"
	"github.com/ebay/beam/logentry"
	"github.com/ebay/beam/logentry/logencoder"
	"github.com/ebay/beam/logentry/logread"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/cmp"
	"github.com/ebay/beam/util/parallel"
	"github.com/ebay/beam/util/tracing"
	"github.com/ebay/beam/viewclient/lookups"
	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
)

const (
	maxInsertRetry = 32
)

// equalKeys returns true if the 2 maps have the same
// set of keys in them. It does not compare any values.
func equalKeys(a, b map[int]uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, exists := b[k]; !exists {
			return false
		}
	}
	return true
}

// InsertFacts implements the gRPC InsertFacts function of the BeamFactStore service. This will
// upsert 1 or more facts described in the request, using a transaction to safely determine if
// the fact(s) already exist.
// The result includes the log index that the facts were committed at, subsequent requests to
// Read may not yet be at the supplied log index. Callers should include the returned log index
// in the subsequent read if they expect to see these write(s).
func (s *Server) InsertFacts(ctx context.Context, req *api.InsertFactsRequest) (*api.InsertFactsResult, error) {
	return insertFacts(ctx, s.source, s.beamLog, req)
}

// logAppendSingle provides an abstraction for the log interactions used by
// Insert. This makes it easier to write unit tests.
type logAppendSingle interface {
	AppendSingle(ctx context.Context, data []byte) (blog.Index, error)
}

// insertFacts provides the implementation of the InsertFacts public API. It's
// extracted out this way to aid in testing.
func insertFacts(ctx context.Context,
	source lookups.SPO,
	logAppender logAppendSingle,
	req *api.InsertFactsRequest) (*api.InsertFactsResult, error) {

	log.Debugf("InsertFacts %+v", req)

	applyUnicodeNormalizationToInsertFactsRequest(req)
	if err := validateInsertFactsRequest(req); err != nil {
		return nil, err
	}
	// insert has to determine which facts in the request exist at the time of the write, we use a transaction
	// to do this safely. Insert performs a loop of
	//		1. write an InsertTxCommand of the facts
	//		2. use the log from (1) to execute a LookupSPO of all facts in the request that could exist
	//		3. if there was an existing fact in the InsertTxCommand then
	//				write a decide(abort) log message
	//				build a filtered InsertTxCommand removing all the facts we found as existing
	//				goto back to 1 and try again
	//		4. if 2/3 didn't discover a different set of existing facts since the previous loop
	//         we can commit and we're done
	//		5. if all the facts in the request are existing, then we're done
	//
	lookupForExisting := rpc.LookupSPORequest{
		Lookups: make([]rpc.LookupSPORequest_Item, 0, len(req.Facts)-len(req.NewSubjectVars)),
	}
	lookupOffsetToFactOffset := make([]int, 0, len(req.Facts)-len(req.NewSubjectVars))
	for i, f := range req.Facts {
		if f.Subject.GetVar() == "" && f.Predicate.GetVar() == "" && f.Object.GetVar() == "" {
			lookupOffsetToFactOffset = append(lookupOffsetToFactOffset, i)
			lookupForExisting.Lookups = append(lookupForExisting.Lookups, rpc.LookupSPORequest_Item{
				Subject:   f.Subject.GetKid(),
				Predicate: f.Predicate.GetKid(),
				Object:    rpc.KGObjectFromAPI(*f.Object.GetObject()),
			})
		}
	}
	// lookup facts from the request at the log index. Returns a map containing the IDs of the existing facts
	// the map has the offset of the InsertFact in the request as the key.
	lookup := func(idx blog.Index) (map[int]uint64, error) {
		// its possible that all facts in the insert reference a variable and are therefore
		// going to be new facts, and so there are zero existing facts to look for, in which
		// case we don't have anything to do
		if len(lookupForExisting.Lookups) == 0 {
			return nil, nil
		}
		span, ctx := opentracing.StartSpanFromContext(ctx, "lookup existing facts")
		span.SetTag("index", idx)
		span.SetTag("lookup_count", len(lookupForExisting.Lookups))
		defer span.Finish()
		existing := make(map[int]uint64)
		resCh := make(chan *rpc.LookupChunk, 4)
		wait := parallel.Go(func() {
			for chunk := range resCh {
				for _, f := range chunk.Facts {
					reqFactOffset := lookupOffsetToFactOffset[f.Lookup]
					existing[reqFactOffset] = f.Fact.Id
				}
			}
		})
		lookupForExisting.Index = idx
		err := source.LookupSPO(ctx, &lookupForExisting, resCh)
		wait()
		if err != nil {
			return nil, err
		}
		span.SetTag("existing_count", len(existing))
		return existing, nil
	}
	logReq, vars := convertAPIInsertToLogCommand(req)
	existingLastAttempt := map[int]uint64{}
	filteredLogReq := logReq
	var insertIdx blog.Index

	for attempt := 0; attempt < maxInsertRetry; attempt++ {
		attemptSpan, attemptCtx := opentracing.StartSpanFromContext(ctx, "insert_attempt")
		attemptSpan.SetTag("attempt", attempt)
		var err error
		insertIdx, err = appendInsertTxBegin(attemptCtx, logAppender, &filteredLogReq)
		if err != nil {
			attemptSpan.Finish()
			return nil, fmt.Errorf("unable to write tx message: %v", err)
		}
		existing, err := lookup(insertIdx - 1)
		commit := err == nil && equalKeys(existing, existingLastAttempt)

		if _, decideErr := appendTxDecide(attemptCtx, logAppender, insertIdx, commit); decideErr != nil {
			attemptSpan.Finish()
			return nil, fmt.Errorf("unable to write decide message: %v", decideErr)
		}
		if err != nil {
			attemptSpan.Finish()
			return nil, fmt.Errorf("unable to lookup existing facts: %v", err)
		}
		// if all the facts in the insert exist then commit will be false
		// but there's no more work to do, so we're done at that point
		if commit || len(req.Facts) == len(existing) {
			attemptSpan.Finish()
			return insertResult(req, &logReq, existing, insertIdx, vars), nil
		}
		existingLastAttempt = existing
		filteredLogReq = excludeExisting(logReq, existing)
		attemptSpan.Finish()
	}
	// give up
	return nil, fmt.Errorf("insert exhausted all retries (%d) while attempting to resolve which facts do/don't exist", maxInsertRetry)
}

// appendInsertTxBegin appends a single transaction message to the log containing req, and returns the index of the entry.
func appendInsertTxBegin(ctx context.Context, logAppender logAppendSingle, req *logentry.InsertTxCommand) (blog.Index, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "append startTx")
	span.SetTag("facts", len(req.Facts))
	defer span.Finish()
	enc := logencoder.Encode(req)
	return logAppender.AppendSingle(ctx, enc)
}

// appendTxDecide appends a single tx decision message to the log and returns the index of the entry.
func appendTxDecide(ctx context.Context, logAppender logAppendSingle, idx blog.Index, commit bool) (blog.Index, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "append decideTx")
	span.SetTag("commit", commit)
	defer span.Finish()
	dm := &logentry.TxDecisionCommand{Tx: idx, Commit: commit}
	enc := logencoder.Encode(dm)
	return logAppender.AppendSingle(ctx, enc)
}

// Wipe implements the Wipe function in the BeamFactStore gRPC API. If Wiping is enabled in the
// configuration this will delete all facts in the store. It is expected that this is only enabled
// in development environments. This blocks until all active views have processed the Wipe request.
func (s *Server) Wipe(ctx context.Context, w *api.WipeRequest) (*api.WipeResult, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "append wipe")
	enc := logencoder.Encode(&logentry.WipeCommand{})
	index, err := s.beamLog.AppendSingle(ctx, enc)
	span.Finish()
	if err != nil {
		return nil, err
	}

	res := &api.WipeResult{Index: int64(index)}
	waitFor := w.GetWaitFor()
	if waitFor > 0 && waitFor < time.Minute*1 {
		span, ctx := opentracing.StartSpanFromContext(ctx, "wait for index")
		res.AtIndex = int64(s.waitForIndex(ctx, index, waitFor))
		span.Finish()
	}
	return res, nil
}

// LookupSp returns facts containing the supplied subject,predicate
func (s *Server) LookupSp(ctx context.Context, req *api.LookupSPRequest) (*api.LookupSPResult, error) {
	index, err := s.resolveIndex(ctx, uint64(req.Index))
	if err != nil {
		return nil, err
	}
	vreq := &rpc.LookupSPRequest{
		Index:   index,
		Lookups: make([]rpc.LookupSPRequest_Item, len(req.Lookups)),
	}
	for i := range req.Lookups {
		vreq.Lookups[i].Subject = req.Lookups[i].Subject
		vreq.Lookups[i].Predicate = req.Lookups[i].Predicate
	}
	res := &api.LookupSPResult{
		Index:   int64(index),
		Results: make([]api.LookupSPResult_Items, len(vreq.Lookups)),
	}
	resCh := make(chan *rpc.LookupChunk, 4)
	wait := parallel.Go(func() {
		for chunk := range resCh {
			for _, f := range chunk.Facts {
				r := &res.Results[f.Lookup]
				r.Item = append(r.Item, api.LookupSPResult_Item{
					Id:     f.Fact.Id,
					Object: f.Fact.Object.ToAPIObject(),
				})
			}
		}
	})
	err = s.source.LookupSP(ctx, vreq, resCh)
	wait()
	return res, err
}

// LookupPo returns facts with matching predicate & object values
func (s *Server) LookupPo(ctx context.Context, req *api.LookupPORequest) (*api.LookupPOResult, error) {
	log.Debugf("LookupPO %+v", req)
	resCh := make(chan *rpc.LookupChunk, 4)
	index, err := s.resolveIndex(ctx, uint64(req.Index))
	if err != nil {
		return nil, err
	}
	rpcReq := rpc.LookupPORequest{
		Index:   index,
		Lookups: make([]rpc.LookupPORequest_Item, len(req.Object)),
	}
	for i := range req.Object {
		rpcReq.Lookups[i] = rpc.LookupPORequest_Item{
			Predicate: req.Predicate,
			Object:    rpc.KGObjectFromAPI(req.Object[i]),
		}
	}
	lookupWait := parallel.GoCaptureError(func() error {
		return s.source.LookupPO(ctx, &rpcReq, resCh)
	})
	res := api.LookupPOResult{
		Index:   int64(index),
		Results: make([]api.LookupPOResult_Items, len(req.Object)),
	}
	for chunk := range resCh {
		for _, fact := range chunk.Facts {
			currentRes := &res.Results[fact.Lookup]
			currentRes.Item = append(currentRes.Item,
				api.LookupPOResult_Item{
					Id:      fact.Fact.Id,
					Subject: fact.Fact.Subject,
				})
		}
	}
	return &res, lookupWait()
}

// resolveIndex is useful for determining a log index against which lookup
// requests can be made. If idx is 0, the latest linearizable index is
// fetched from the log and returned. Otherwise the index provided is
// returned. If the call fails, either a context error or a ClosedError
// is returned at which point a caller should abort.
func (s *Server) resolveIndex(ctx context.Context, idx blog.Index) (blog.Index, error) {
	if idx > 0 {
		return idx, nil
	}
	// only create the span if we're going to do something
	span, ctx := opentracing.StartSpanFromContext(ctx, "resolve index")
	tracing.UpdateMetric(span, metrics.resolveIndexLatencySeconds)
	defer span.Finish()

	info, err := s.beamLog.Info(ctx)
	if err != nil {
		return 0, err
	}
	return info.LastIndex, nil
}

// beamLogInfo is the subset of the blog.BeamLog interface used by
// resolveIndexConstraint & fetchLatestLogIndex.
type beamLogInfo interface {
	Info(ctx context.Context) (*blog.Info, error)
}

// fetchRecentLogIndex returns the last known log index, which is probably
// stale. As a special consideration, it only returns 0 if the log was empty at
// the time the function was invoked.
func (s *Server) fetchRecentLogIndex(ctx context.Context, log beamLogInfo) (blog.Index, error) {
	s.lock.Lock()
	recent := s.locked.recentIndex
	s.lock.Unlock()
	if recent == 0 {
		return s.fetchLatestLogIndex(ctx, log)
	}
	return recent, nil
}

// resolveIndexConstraint will apply the index & constraints specified in 'idx'
// and return a suitable log index to use. If 'idx' specifies an index we don't
// know how to resolve, or has an invalid combination of index and constraint an
// error is returned.
func (s *Server) resolveIndexConstraint(ctx context.Context, log beamLogInfo, idx api.LogIndex) (blog.Index, error) {
	switch idx.Constraint {
	case api.LogIndexExact:
		if idx.Index == 0 {
			// gogoproto does not allow us to distingush between the caller
			// specifying 0 and not specifying Index at all. 0 is a valid log
			// index. If 0 is specified but the log is not empty that's probably
			// a mistake.
			recent, err := s.fetchRecentLogIndex(ctx, log)
			if err != nil {
				return 0, err
			}
			if recent > 0 {
				return 0, errors.New("a log index constraint of Exact requires a log index to be set")
			}
		}
		return idx.Index, nil

	case api.LogIndexAtLeast:
		recent, err := s.fetchRecentLogIndex(ctx, log)
		if err != nil {
			return 0, err
		}
		return cmp.MaxUint64(recent, idx.Index), nil

	case api.LogIndexRecent:
		if idx.Index != 0 {
			return 0, errors.New("a log index can't be specified when using the Recent constraint")
		}
		return s.fetchRecentLogIndex(ctx, log)

	case api.LogIndexLatest:
		if idx.Index != 0 {
			return 0, errors.New("a log index can't be specified when using the Latest constraint")
		}
		return s.fetchLatestLogIndex(ctx, log)

	default:
		return 0, fmt.Errorf("LogIndex constraint of %s is not supported", idx.Constraint)
	}
}

// fetchLatestLogIndex requests the latest log index from the log store and
// returns it.
func (s *Server) fetchLatestLogIndex(ctx context.Context, log beamLogInfo) (blog.Index, error) {
	info, err := log.Info(ctx)
	if err != nil {
		return 0, err
	}
	s.lock.Lock()
	s.locked.recentIndex = cmp.MaxUint64(s.locked.recentIndex, info.LastIndex)
	s.lock.Unlock()
	return info.LastIndex, nil
}

// insertResult builds an insert facts result from the request, existing ids and
// log index. logReq should be the InsertTxCommand that was first created from
// the Insert request, it should not be the version that has the existing facts
// filtered out of it.
func insertResult(apiReq *api.InsertFactsRequest, logReq *logentry.InsertTxCommand, existing map[int]uint64, idx blog.Index, vars map[string]int32) *api.InsertFactsResult {
	res := &api.InsertFactsResult{
		VarResults: make([]uint64, len(apiReq.NewSubjectVars)),
		FactIds:    make([]uint64, len(apiReq.Facts)),
		Index:      int64(idx),
	}
	for i := range apiReq.Facts {
		if id, isExisting := existing[i]; isExisting {
			res.FactIds[i] = id
		} else {
			res.FactIds[i] = logread.KID(idx, logReq.Facts[i].FactIDOffset)
		}
	}
	for i, vn := range apiReq.NewSubjectVars {
		res.VarResults[i] = logread.KID(idx, vars[vn])
	}
	return res
}

// excludeExisting builds a new InsertTxCommand excluding facts where there's an existing fact.
// This does not filter in place as it requires the full InsertTxCommand as the starting point
// and that is used later on, so we don't want to mutate it.
func excludeExisting(req logentry.InsertTxCommand, existing map[int]uint64) logentry.InsertTxCommand {
	n := logentry.InsertTxCommand{
		Facts: make([]logentry.InsertFact, 0, len(req.Facts)-len(existing)),
	}
	for i, f := range req.Facts {
		if _, isExisting := existing[i]; !isExisting {
			n.Facts = append(n.Facts, f)
		}
	}
	return n
}
