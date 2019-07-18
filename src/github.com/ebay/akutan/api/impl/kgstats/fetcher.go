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

// Package kgstats fetches and caches runtime statistics about the cluster and
// dataset.
package kgstats

import (
	"context"
	"sync"
	"time"

	"github.com/ebay/akutan/query/planner"
	"github.com/ebay/akutan/util/clocks"
	"github.com/sirupsen/logrus"
)

const (
	// How often to fetch new stats from views.
	updateInterval = 2 * time.Minute
	// If fetching stats from views fails and we have no stats at all, wait this
	// long before retrying.
	fetchBackoffEmpty = 5 * time.Second
	// If fetching stats from views fails and we have some stale stats, wait this
	// long before retrying.
	fetchBackoffStale = 30 * time.Second
)

// An alias for the normal clock. This is swapped out for some unit tests.
var clock = clocks.Wall

// FetchStats collects the latest statistics from the views.
// Upon success, it returns the statistics (must be non-nil).
// Otherwise, it returns a context error or some other transient error.
type FetchStats func(ctx context.Context) (planner.Stats, error)

// A Fetcher periodically collects stats and caches them.
type Fetcher struct {
	// Same as passed to NewFetcher.
	fetchStats FetchStats
	// ready is closed once the first fetch succeeds. If ready is closed, latest is
	// not nil.
	ready chan struct{}
	// Lock protecting changes to the 'latest' pointer (not the value).
	mutex sync.Mutex
	// The most recent statistics. Set to nil when a Fetcher is created, then never
	// nil again after the first fetch succeeds (and ready is closed).
	latest planner.Stats
}

// NewFetcher constructs a new Fetcher. The caller should subsequently call Run.
func NewFetcher(fetchStats FetchStats) *Fetcher {
	fetcher := &Fetcher{
		fetchStats: fetchStats,
		ready:      make(chan struct{}),
	}
	return fetcher
}

// Last blocks until any stats are available, then returns the latest cached
// stats. Upon success, it returns stats (non-nil) that are likely somewhat
// stale. The returned stats object is shared, so it should be treated as
// immutable. Otherwise, Last returns a context error.
func (fetcher *Fetcher) Last(ctx context.Context) (planner.Stats, error) {
	select {
	case <-fetcher.ready:
		return fetcher.getLatest(), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// getLatest returns the most recently fetched statistics without blocking. It
// may return nil if no statistics are yet available.
func (fetcher *Fetcher) getLatest() planner.Stats {
	fetcher.mutex.Lock()
	defer fetcher.mutex.Unlock()
	return fetcher.latest
}

// Run is a long-running blocking call that periodically updates the Fetcher's
// statistics. It exits once ctx is closed.
func (fetcher *Fetcher) Run(ctx context.Context) {
	fetchOnce := func(ctx context.Context) (planner.Stats, error) {
		ctx, cancel := context.WithTimeout(ctx, updateInterval)
		defer cancel()
		return fetcher.fetchStats(ctx)
	}

	for ctx.Err() == nil {
		logrus.Info("Fetching KG stats")
		stats, err := fetchOnce(ctx)
		if err != nil {
			var backoff time.Duration
			if fetcher.getLatest() == nil {
				backoff = fetchBackoffEmpty
			} else {
				backoff = fetchBackoffStale
			}
			logrus.WithFields(logrus.Fields{
				"error": err,
				"next":  clock.Now().Add(backoff),
			}).Warnf("Fetching KG stats failed")
			clock.SleepUntil(ctx, clock.Now().Add(backoff))
			continue
		}
		if stats == nil {
			logrus.Panicf("stats fetcher returned nil error and nil stats")
		}

		fetcher.mutex.Lock()
		if fetcher.latest == nil {
			close(fetcher.ready)
		}
		fetcher.latest = stats
		fetcher.mutex.Unlock()

		nextAt := clock.Now().Add(updateInterval)
		logrus.WithFields(logrus.Fields{
			"next": nextAt,
		}).Info("Updated KG stats")
		clock.SleepUntil(ctx, nextAt)
	}
}
