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

package debug

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/bytes"
	"github.com/ebay/beam/util/clocks"
	"github.com/ebay/beam/util/parallel"
	"github.com/ebay/beam/viewclient/lookups"
)

// queryLookupStats tracks execution stats for all lookup RPCs used during a
// single query execution
type queryLookupStats struct {
	clock       clocks.Source
	impl        lookups.All
	lookups     lookupStats
	lookupsp    lookupStats
	lookupspo   lookupStats
	lookuppo    lookupStats
	lookuppocmp lookupStats
}

// dump writes a summary of the collected lookup stats to 'w'
func (s *queryLookupStats) dump(w bytes.StringWriter) {
	s.lookups.dump(w, "LookupS")
	s.lookupsp.dump(w, "LookupSP")
	s.lookupspo.dump(w, "LookupSPO")
	s.lookuppo.dump(w, "LookupPO")
	s.lookuppocmp.dump(w, "LookupPOCmp")
}

func (s *queryLookupStats) LookupS(ctx context.Context, req *rpc.LookupSRequest, resCh chan *rpc.LookupChunk) error {
	return s.lookups.track(s.clock, len(req.Subjects), func() error {
		wait, counterResCh := s.lookups.goCountResults(resCh)
		defer wait()
		return s.impl.LookupS(ctx, req, counterResCh)
	})
}

func (s *queryLookupStats) LookupSP(ctx context.Context, req *rpc.LookupSPRequest, resCh chan *rpc.LookupChunk) error {
	return s.lookupsp.track(s.clock, len(req.Lookups), func() error {
		wait, counterResCh := s.lookupsp.goCountResults(resCh)
		defer wait()
		return s.impl.LookupSP(ctx, req, counterResCh)
	})
}

func (s *queryLookupStats) LookupSPO(ctx context.Context, req *rpc.LookupSPORequest, resCh chan *rpc.LookupChunk) error {
	return s.lookupspo.track(s.clock, len(req.Lookups), func() error {
		wait, counterResCh := s.lookupspo.goCountResults(resCh)
		defer wait()
		return s.impl.LookupSPO(ctx, req, counterResCh)
	})
}

func (s *queryLookupStats) LookupPO(ctx context.Context, req *rpc.LookupPORequest, resCh chan *rpc.LookupChunk) error {
	return s.lookuppo.track(s.clock, len(req.Lookups), func() error {
		wait, counterResCh := s.lookuppo.goCountResults(resCh)
		defer wait()
		return s.impl.LookupPO(ctx, req, counterResCh)
	})
}

func (s *queryLookupStats) LookupPOCmp(ctx context.Context, req *rpc.LookupPOCmpRequest, resCh chan *rpc.LookupChunk) error {
	return s.lookuppocmp.track(s.clock, len(req.Lookups), func() error {
		wait, counterResCh := s.lookuppocmp.goCountResults(resCh)
		defer wait()
		return s.impl.LookupPOCmp(ctx, req, counterResCh)
	})
}

// lookupStats tracks execution stats for a single lookup type.
type lookupStats struct {
	lock   sync.Mutex
	locked struct {
		duration     time.Duration
		lookupsTotal int
		resultsTotal int
		rpcCount     int
	}
}

// goCountResults starts a goroutine that will count the number of facts
// returned from a lookup RPC. It forwards the results on the supplied resCh. It
// returns a function that can be used to wait for this goroutine to exit. It
// also returns a new channel that the lookup RPC should send its results to.
func (l *lookupStats) goCountResults(resCh chan<- *rpc.LookupChunk) (func(), chan *rpc.LookupChunk) {
	lookupResCh := make(chan *rpc.LookupChunk, 4)
	return parallel.Go(func() {
		totalFacts := 0
		for c := range lookupResCh {
			totalFacts += len(c.Facts)
			resCh <- c
		}
		close(resCh)
		l.lock.Lock()
		l.locked.resultsTotal += totalFacts
		l.lock.Unlock()

	}), lookupResCh
}

// track calls the supplied lookup function and times how long it took to
// execute. It updates the accumulated stats with the result, along with the
// number of total number of lookup items and RPCs.
func (l *lookupStats) track(clock clocks.Source, lookupCount int, lookup func() error) error {
	start := clock.Now()
	err := lookup()
	end := clock.Now()

	l.lock.Lock()
	defer l.lock.Unlock()
	l.locked.duration += end.Sub(start)
	l.locked.lookupsTotal += lookupCount
	l.locked.rpcCount++
	return err
}

func (l *lookupStats) dump(w bytes.StringWriter, label string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.locked.rpcCount == 0 {
		return
	}
	fmt.Fprintln(w, label)
	fmt.Fprintf(w, "\tRPC Count:      %d\n", l.locked.rpcCount)
	fmt.Fprintf(w, "\tAvg Lookups:    %1.1f\n", float64(l.locked.lookupsTotal)/float64(l.locked.rpcCount))
	fmt.Fprintf(w, "\tAvg Results:    %1.1f\n", float64(l.locked.resultsTotal)/float64(l.locked.rpcCount))
	fmt.Fprintf(w, "\tTotal RPC Time: %v\n", l.locked.duration)
	fmt.Fprintf(w, "\tAvg RPC time:   %v\n", l.locked.duration/time.Duration(l.locked.rpcCount))
}
