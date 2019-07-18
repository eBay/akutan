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

// Package query provides a high level entry point for executing AkutanQL queries.
// It runs the entire query processor, including the parser, planner, and
// executor.
package query

import (
	"context"
	"io"
	"os"
	"path"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/facts/cache"
	"github.com/ebay/akutan/query/exec"
	"github.com/ebay/akutan/query/internal/debug"
	"github.com/ebay/akutan/query/parser"
	"github.com/ebay/akutan/query/planner"
	"github.com/ebay/akutan/query/planner/search"
	"github.com/ebay/akutan/util/clocks"
	"github.com/ebay/akutan/util/graphviz"
	"github.com/ebay/akutan/util/tracing"
	"github.com/ebay/akutan/viewclient/lookups"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
)

// ResultChunk contains a part of the results of a query.
type ResultChunk = exec.ResultChunk

// Options contains various settings that affect the query processing.
type Options struct {
	// Format indicates which query text format to parse the query with. Can be
	// either parser.QuerySparql or parser.QueryFactPattern. Defaults to
	// parser.QuerySparql if not set.
	Format string
	// If set diagnostic information about the query processing will be collected into a report.
	Debug bool
	// By default the report is written to a file in $TMPDIR. If DebugOut is set, the report
	// will be written to that instead.
	DebugOut io.Writer
	// If set the Debug tracker will use this clock for generating timing information, if not
	// set it'll use clocks.Wall.
	Clock clocks.Source
}

// Engine provides a high level interface for running queries.
type Engine struct {
	statsProvider StatsProvider
	lookups       lookups.All
}

// StatsProvider allows the engine to obtain the current statistics for use in
// planning. This is called for every query execution.
type StatsProvider func(context.Context) (planner.Stats, error)

// New creates a new Engine, the resulting Engine can be used concurrently to
// execute queries.
func New(statsProvider StatsProvider, lookups lookups.All) *Engine {
	return &Engine{
		statsProvider: statsProvider,
		lookups:       lookups,
	}
}

// Query will execute a query starting from the string representation of the
// query all the way through the steps, Parse, Rewrite, Plan & Execute. The
// query is executed as of the supplied log index. Results will be written to
// the provided 'resCh' channel. The caller can apply backpressure to the query
// execution by reading slowly from this channel.
//
// This function will block until the query has completed and all results have
// been passed to the 'resCh' channel, or an error occurs. In all cases resCh
// will be closed before this function returns.
func (e *Engine) Query(ctx context.Context,
	index blog.Index, rawQuery string,
	opt Options, resCh chan<- ResultChunk) error {

	span, ctx := opentracing.StartSpanFromContext(ctx, "Query")
	defer span.Finish()

	tracker := debug.New(opt.Debug, opt.DebugOut, opt.Clock, index, rawQuery)
	defer tracker.Close()

	span, _ = opentracing.StartSpanFromContext(ctx, "parse query")
	tracing.UpdateMetric(span, metrics.parseQueryDurationSeconds)
	if opt.Format == "" {
		opt.Format = parser.QuerySparql
	}
	query, err := parser.Parse(opt.Format, rawQuery)
	tracker.Parsed(query, err)
	span.Finish()
	if err != nil {
		// You can't close an already closed channel. We can't defer
		// close(resCh) because under normal circumstances qexec.Execute will
		// close it. but we need to close it if we don't get that far.
		close(resCh)
		return err
	}

	span, cctx := opentracing.StartSpanFromContext(ctx, "rewrite query")
	tracing.UpdateMetric(span, metrics.rewriteQueryDurationSeconds)
	err = parser.Rewrite(cctx, e.lookups, index, query)
	tracker.Rewritten(query, err)
	span.Finish()
	if err != nil {
		close(resCh)
		return err
	}

	span, cctx = opentracing.StartSpanFromContext(ctx, "plan query")
	tracing.UpdateMetric(span, metrics.planQueryDurationSeconds)
	stats, err := e.statsProvider(cctx)
	if err != nil {
		span.Finish()
		close(resCh)
		return err
	}
	planOpts := search.Options{
		CheckInvariantsAfterMajorSteps: true,
	}
	space, plan, err := planner.Prepare(cctx, query, tracker.DecorateStats(stats), planOpts)
	tracker.Planned(space, plan, err)
	span.Finish()
	if err != nil {
		if space != nil {
			filename := path.Join(os.TempDir(), "lastfailedspace.pdf")
			graphviz.Create(filename, space.Graphviz, graphviz.Options{})
			logrus.WithFields(logrus.Fields{
				"error":            err,
				"space_written_to": filename,
			}).Warn("Planner failed")
		} else {
			logrus.WithFields(logrus.Fields{
				"error": err,
			}).Warn("Planner failed")
		}
		close(resCh)
		return err
	}

	span, cctx = opentracing.StartSpanFromContext(ctx, "execute query")
	tracing.UpdateMetric(span, metrics.executeQueryDurationSeconds)
	defer span.Finish()
	return exec.Execute(cctx, tracker.ExecEvents(plan), index, cache.New(), tracker.DecorateLookups(e.lookups), plan, resCh)
}
