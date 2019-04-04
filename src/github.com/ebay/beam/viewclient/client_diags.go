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
	"time"

	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/errors"
	"github.com/ebay/beam/util/parallel"
	"github.com/ebay/beam/util/tracing"
	"github.com/ebay/beam/viewclient/viewreg"
	opentracing "github.com/opentracing/opentracing-go"
)

// ViewStats contains statistics about the facts in a particular view
type ViewStats struct {
	View  viewreg.View
	Err   error
	Stats *rpc.StatsResult
}

// Stats makes a best effort attempt to collect general service statistics from
// all views that expose them. For each view consulted there will be an entry in
// the returned slice.
func (c *Client) Stats(ctx context.Context) []ViewStats {
	ctx, cancelFunc := context.WithTimeout(ctx, time.Second*2)
	defer cancelFunc()
	span, ctx := opentracing.StartSpanFromContext(ctx, "Diagnostics.Stats")
	tracing.UpdateMetric(span, metrics.diagnosticsStatsLatencySeconds)
	defer span.Finish()
	diagViews := c.DiagnosticsViews()
	numViews := len(diagViews)
	res := make([]ViewStats, numViews)
	_ = parallel.InvokeN(ctx, numViews,
		func(ctx context.Context, i int) error {
			res[i].View = diagViews[i].View
			res[i].Stats, res[i].Err = diagViews[i].Stub.Stats(ctx, &rpc.StatsRequest{})
			return nil // don't return err or InvokeN will cancel ctx
		})
	return res
}

// UpdateStats asks all the views to start recalculating their fact statistics
// if an error is encountered from one or more the views, it is returned, however
// that does not stop this trying to trigger the update on the remaining views.
func (c *Client) UpdateStats(ctx context.Context) error {
	diagViews := c.DiagnosticsViews()
	req := &rpc.UpdateStatsRequest{}
	// This attempts to start updating stats on as many servers as possible, even if
	// some RPCs fail. InvokeN will cancel the context upon seeing any error, so
	// this function hides errors from InvokeN and instead collects them here.
	errs := make([]error, len(diagViews))
	_ = parallel.InvokeN(ctx, len(diagViews),
		func(ctx context.Context, i int) error {
			_, errs[i] = diagViews[i].Stub.UpdateStats(ctx, req)
			return nil
		})
	return errors.Any(errs...)
}
