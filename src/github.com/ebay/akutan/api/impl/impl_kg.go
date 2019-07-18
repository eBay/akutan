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

	"github.com/ebay/akutan/api"
	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/logentry"
	"github.com/ebay/akutan/logentry/logencoder"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/cmp"
	"github.com/ebay/akutan/util/parallel"
	"github.com/ebay/akutan/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
)

// Wipe implements the Wipe function in the FactStore gRPC API. If Wiping is enabled in the
// configuration this will delete all facts in the store. It is expected that this is only enabled
// in development environments. This blocks until all active views have processed the Wipe request.
func (s *Server) Wipe(ctx context.Context, w *api.WipeRequest) (*api.WipeResult, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "append wipe")
	enc := logencoder.Encode(&logentry.WipeCommand{})
	index, err := s.aLog.AppendSingle(ctx, enc)
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

	info, err := s.aLog.Info(ctx)
	if err != nil {
		return 0, err
	}
	return info.LastIndex, nil
}

// aLogInfo is the subset of the blog.AkutanLog interface used by
// resolveIndexConstraint & fetchLatestLogIndex.
type aLogInfo interface {
	Info(ctx context.Context) (*blog.Info, error)
}

// fetchRecentLogIndex returns the last known log index, which is probably
// stale. As a special consideration, it only returns 0 if the log was empty at
// the time the function was invoked.
func (s *Server) fetchRecentLogIndex(ctx context.Context, log aLogInfo) (blog.Index, error) {
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
func (s *Server) resolveIndexConstraint(ctx context.Context, log aLogInfo, idx api.LogIndex) (blog.Index, error) {
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
func (s *Server) fetchLatestLogIndex(ctx context.Context, log aLogInfo) (blog.Index, error) {
	info, err := log.Info(ctx)
	if err != nil {
		return 0, err
	}
	s.lock.Lock()
	s.locked.recentIndex = cmp.MaxUint64(s.locked.recentIndex, info.LastIndex)
	s.lock.Unlock()
	return info.LastIndex, nil
}
