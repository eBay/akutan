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

// Package viewreg tracks all the known view servers in the cluster.
package viewreg

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ebay/akutan/discovery"
	"github.com/ebay/akutan/partitioning"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/bytes"
	"github.com/ebay/akutan/util/clocks"
	grpcclientutil "github.com/ebay/akutan/util/grpc/client"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// A Registry tracks all the known view servers in the cluster. It is intended
// to be a long-lived object, as it manages connections to those servers.
type Registry struct {
	// Closing this context causes the Registry's goroutines to exit and its
	// connections to close. The Registry is still usable, but it will only
	// return empty lists of views.
	ctx context.Context
	// Provides the gRPC ports of all view servers.
	locator discovery.Locator
	// Set to clocks.Wall except for unit testing.
	clock clocks.Source
	// Protects Registry.locked and monitoredView.locked. After writing to
	// either of these values but before releasing the write-lock, you should
	// invoke changedLocked().
	lock sync.RWMutex
	// Protected by 'lock'.
	locked struct {
		// The latest timestamp when any View was added, removed, or updated, or
		// when the Registry was created.
		lastChanged time.Time
		// When 'lastChanged' advances, this channel is closed and the field is
		// re-assigned to a new channel. Waiters should receive from this
		// channel without holding 'lock'. This is never nil.
		changedCh chan struct{}
		// Each known view server has an entry in this map. The map is keyed by
		// the server's Endpoint.HostPort(). If a monitoredView instance is not
		// in this map, then its Context has already been canceled.
		views map[string]*monitoredView
	}
}

// monitoredView represents a single view server. It contains a template of
// values that are used to create View instances; these are what users get from
// the Registry.
type monitoredView struct {
	// All of the connections to this view are created under this context, as
	// are background goroutines that monitor this view. This is a child of
	// Registry.ctx.
	ctx context.Context
	// Cancels 'ctx'.
	cancel func()
	// Protected by Registry.lock.
	locked struct {
		// This is copied out to users when they need a []View.
		//
		// template.Endpoint is written to by discover() only. Each Endpoint
		// value is immutable. discover() may occasionally update the pointer to
		// a new value, but the Host and Port fields will remain the same over
		// time.
		//
		// template.Partition, template.features, and template.pingHealthy are
		// written to by runPings() only.
		//
		// template.connReady is written to by watchConnState only.
		//
		// template.conn is never changed.
		template View
	}
}

// NewRegistry constructs a Registry. The registry will shut down all its
// connections and activity when 'ctx' is closed. It discovers the gRPC port for
// all view servers from 'servers'.
//
// Warning: it takes some time for a Registry to discover view servers and
// determine them to be healthy. As a result, it's a bad idea to create a
// Registry and immediately rely on it to make irreversible decisions. Callers
// may want to use LastChanged(), WaitForStable(), or WaitForChanged() to wait
// until the Registry is ready.
func NewRegistry(ctx context.Context, servers discovery.Locator) *Registry {
	registry := &Registry{
		ctx:     ctx,
		locator: servers,
		clock:   clocks.Wall,
	}
	registry.lock.Lock()
	registry.locked.changedCh = make(chan struct{})
	registry.locked.views = make(map[string]*monitoredView)
	registry.changedLocked()
	registry.lock.Unlock()
	go registry.discover()
	return registry
}

// AllViews returns all of the known views, even those believed to be unhealthy.
// The views are returned in no particular order. The caller is free to modify
// the returned slice and its values.
func (registry *Registry) AllViews() []View {
	registry.lock.RLock()
	defer registry.lock.RUnlock()
	res := make([]View, 0, len(registry.locked.views))
	for _, view := range registry.locked.views {
		res = append(res, view.locked.template) // copy
	}
	return res
}

// HealthyViews returns the set of views that are believed healthy and provide
// all of the given features. The views are returned in no particular order. The
// caller is free to modify the returned slice and its values.
//
// TODO: Carousel clients often want to exclude talking to themselves. Should
// this module try to filter out localhost?
func (registry *Registry) HealthyViews(feature ...Feature) []View {
	registry.lock.RLock()
	defer registry.lock.RUnlock()
	res := make([]View, 0, len(registry.locked.views))
	for _, view := range registry.locked.views {
		tpl := view.locked.template
		if tpl.Healthy() && tpl.Provides(feature...) {
			res = append(res, tpl) // copy
		}
	}
	return res
}

// LastChanged returns the latest timestamp when any View was added, removed, or
// updated. Callers can use this to predict whether the Registry is likely to
// change in the near future.
// Two versions of the Registry are guaranteed to have distinct
// timestamps.
func (registry *Registry) LastChanged() time.Time {
	registry.lock.RLock()
	res := registry.locked.lastChanged
	registry.lock.RUnlock()
	return res
}

// WaitForStable blocks until the registry hasn't changed for at least duration
// 'd'. This can be used to predict that the Registry is unlikely to change in
// the near future. It returns nil if the registry hasn't changed for 'd', or
// ctx.Err() if the given Context expires first.
func (registry *Registry) WaitForStable(ctx context.Context, d time.Duration) error {
	for {
		wake := registry.LastChanged().Add(d)
		if !registry.clock.Now().Before(wake) {
			return nil
		}
		err := registry.clock.SleepUntil(ctx, wake)
		if err != nil {
			return err
		}
	}
}

// WaitForChange blocks until Registry.LastChanged() is after 'lastChanged'.
// This can be used to wait until the Registry satisfies some condition. It
// returns nil if successful, or ctx.Err() if the given Context expires first.
func (registry *Registry) WaitForChange(ctx context.Context, lastChanged time.Time) error {
	registry.lock.RLock()
	changed := registry.locked.lastChanged
	wait := registry.locked.changedCh
	registry.lock.RUnlock()
	if changed.After(lastChanged) {
		return nil
	}
	select {
	case <-wait:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Dump writes detailed information about all the views to 'b'.
func (registry *Registry) Dump(b bytes.StringWriter) {
	// Get a consistent snapshot of the state by holding a read-lock. Note that
	// read-locks are reentrant, so it's ok to call AllViews() here.
	registry.lock.RLock()
	views := registry.AllViews()
	changed := registry.clock.Now().Sub(registry.locked.lastChanged)
	registry.lock.RUnlock()

	sort.Slice(views, func(i, j int) bool {
		ei := views[i].Endpoint
		ej := views[j].Endpoint
		return ei.Host < ej.Host || (ei.Host == ej.Host && ei.Port < ej.Port)
	})
	fmt.Fprintf(b, "View Registry\n")
	fmt.Fprintf(b, "Last changed: %v ago\n", changed.Round(100*time.Millisecond))
	for i, view := range views {
		fmt.Fprintf(b, "%v: %v\n", i, view)
	}
}

// Summary returns some information about the registry for use in debug log
// messages about the Registry.
func (registry *Registry) Summary() logrus.Fields {
	registry.lock.RLock()
	defer registry.lock.RUnlock()
	res := make(map[string]interface{}, 3+len(allFeatures))
	res["timeSinceLastChange"] = registry.clock.Now().Sub(registry.locked.lastChanged)
	res["allViews"] = len(registry.locked.views)
	var healthyViews int
	featureCounts := make([]int, len(allFeatures))
	for _, view := range registry.locked.views {
		if view.locked.template.Healthy() {
			healthyViews++
			for i, f := range allFeatures {
				if view.locked.template.Provides(f) {
					featureCounts[i]++
				}
			}
		}
	}
	res["healthyViews"] = healthyViews
	for i, f := range allFeatures {
		res[f.String()+"Views"] = featureCounts[i]
	}
	return res
}

// changedLocked is called every time the registry has changed. It must
// be called while still holding the write-lock.
func (registry *Registry) changedLocked() {
	// Update lastChanged, ensuring it's advanced.
	now := registry.clock.Now()
	for registry.locked.lastChanged == now {
		// This shouldn't happen with nanosecond-level precision. Just spin.
		now = registry.clock.Now()
	}
	registry.locked.lastChanged = now

	// Notify any waiters.
	close(registry.locked.changedCh)
	registry.locked.changedCh = make(chan struct{})

	// Update metrics.
	metrics.lastChangedTimestamp.Set(float64(now.UnixNano()) / 1e9)
	metrics.changesTotal.Inc()
	metrics.allViews.Set(float64(len(registry.locked.views)))
	healthyViews := 0
	for _, view := range registry.locked.views {
		if view.locked.template.Healthy() {
			healthyViews++
		}
	}
	metrics.healthyViews.Set(float64(healthyViews))
}

// discover is a long-running goroutine that waits for new service discovery
// results. It returns once registry.ctx is closed.
func (registry *Registry) discover() {
	oldVersion := uint64(0)
	for {
		result, err := registry.locator.WaitForUpdate(registry.ctx, oldVersion)
		if err != nil {
			if registry.ctx.Err() == nil {
				panic(fmt.Sprintf("Unexpected error from discovery.Locator: %v", err))
			}
			break
		}
		registry.loadEndpoints(result.Endpoints)
		oldVersion = result.Version
	}

	// Shut down.
	registry.lock.Lock()
	defer registry.lock.Unlock()
	// The views' contexts are already closed, since they're children of
	// registry.ctx. Therefore, it's safe to orphan them.
	registry.locked.views = nil
	registry.changedLocked()
}

// loadEndpoints updates the registry with new service discovery endpoints
// and removes views that service discovery no longer supplies.
func (registry *Registry) loadEndpoints(endpoints []*discovery.Endpoint) {
	// latest initially contains all the result's endpoints.
	// It is mutated later to remove known endpoints.
	latest := make(map[string]*discovery.Endpoint, len(endpoints))
	for _, endpoint := range endpoints {
		latest[endpoint.HostPort()] = endpoint
	}

	registry.lock.Lock()
	defer registry.lock.Unlock()
	changed := false

	// Update or remove each existing view.
	for key, view := range registry.locked.views {
		if endpoint, keep := latest[key]; keep {
			if endpoint != view.locked.template.Endpoint {
				changed = true
				view.locked.template.Endpoint = endpoint
			}
			delete(latest, key)
		} else {
			changed = true
			metrics.removalsTotal.Inc()
			view.cancel()
			delete(registry.locked.views, key)
		}
	}

	// The endpoints remaining in 'latest' are new. Add them.
	for key, endpoint := range latest {
		changed = true
		metrics.additionsTotal.Inc()
		ctx, cancel := context.WithCancel(registry.ctx)
		conn := grpcclientutil.InsecureDialContext(ctx, endpoint.HostPort())
		view := &monitoredView{
			ctx:    ctx,
			cancel: cancel,
		}
		view.locked.template = View{
			Endpoint: endpoint,
			conn:     conn,
		}
		registry.locked.views[key] = view
		go registry.watchConnState(view, conn)
		go registry.runPings(view,
			rpc.NewDiagnosticsClient(conn),
			endpoint.String())
	}

	if changed {
		registry.changedLocked()
	}
}

// watchConnState is a long-running goroutine that tracks the state of a
// particular grpc.ClientConn. There is exactly one of these goroutines per
// monitoredView. watchConnState returns once view.ctx is closed.
//
// This method's job is to set 'view.locked.template.connReady' to true when the
// connection is in a READY state, and false otherwise. When connReady is false,
// the view is considered unhealthy.
//
// The gRPC connection is only usable when it's in a READY state. If the
// underlying TCP connection gets dropped, the gRPC connection will transition
// to TRANSIENT_FAILURE right away, then recover on its own. In such scenarios,
// the connection state usually changes faster than the pinger can detect a
// problem.
func (registry *Registry) watchConnState(view *monitoredView, conn *grpc.ClientConn) {
	isReady := func(state connectivity.State) bool {
		return state == connectivity.Ready
	}
	prev := connectivity.Shutdown
	for view.ctx.Err() == nil {
		conn.WaitForStateChange(view.ctx, prev)
		state := conn.GetState()
		if isReady(state) != isReady(prev) {
			registry.lock.Lock()
			view.locked.template.connReady = isReady(state)
			registry.changedLocked()
			registry.lock.Unlock()
		}
		prev = state
	}
}

// runPings is a long-running goroutine that periodically issues RPCs to a
// particular view server to determine its health and its properties (features,
// etc). There is exactly one of these goroutines per monitoredView. runPings
// returns once view.ctx is closed.
//
// Doing these health checks repeatedly helps discover issues with the server
// application. grpc.ClientConn will react to changes at the TCP level, but it
// doesn't appear to check for higher-level problems. For example, gRPC won't
// detect that a server process was paused with `kill -STOP`. This runPings
// method makes sure the server is able to respond to requests.
func (registry *Registry) runPings(view *monitoredView, stub pingStub, server string) {
	var prevResult pingResult
	for view.ctx.Err() == nil {
		waitUntil := registry.clock.Now().Add(10 * time.Second)
		prevResult = registry.ping(view, server, stub, prevResult)
		registry.clock.SleepUntil(view.ctx, waitUntil)
	}
}

// pingStub is the subset of rpc.DiagnosticsClient used by runPings.
type pingStub interface {
	Stats(context.Context, *rpc.StatsRequest, ...grpc.CallOption) (*rpc.StatsResult, error)
}

// ping issues a single Stats request and processes the result. 'view',
// 'server', and 'stub' correspond to the target server. 'prevResult' is the
// result of the previous ping, or the zero value if none; this is used to only
// update the registry when there's been a real change.
func (registry *Registry) ping(
	view *monitoredView, server string, stub pingStub,
	prevResult pingResult) pingResult {

	logrus.WithFields(logrus.Fields{
		"server": server,
	}).Debug("Sending Diagnostics.Stats RPC")
	rpcCtx, cancel := context.WithTimeout(view.ctx, 5*time.Second)
	stats, err := stub.Stats(rpcCtx, &rpc.StatsRequest{})
	cancel()

	var (
		result  pingResult
		outcome string
		msg     string
	)
	if err == nil {
		result, err = parsePingResult(stats)
		if err == nil {
			outcome = "ok"
		} else {
			outcome = err.Error()
		}
		msg = "Got RPC Diagnostics.Stats reply"
	} else {
		msg = "RPC Diagnostics.Stats failed"
		outcome = err.Error()
	}
	logger := logrus.WithFields(logrus.Fields{
		"server":      server,
		"pingHealthy": result.healthy,
		"result":      outcome,
	})
	if result.Equals(prevResult) {
		logger.Debug(msg)
	} else {
		logger.Info(msg)
		// Update the registry with the new state.
		registry.lock.Lock()
		defer registry.lock.Unlock()
		view.locked.template.pingHealthy = result.healthy
		view.locked.template.features = result.features
		view.locked.template.Partition = result.partition
		registry.changedLocked()
	}
	return result
}

// pingResult summarizes the outcome of a ping RPC.
type pingResult struct {
	// True if the RPC returned a reply that was parsed successfully, false
	// otherwise.
	healthy bool
	// features parsed from the reply, 0 otherwise.
	features Feature
	// partition parsed from the reply, nil otherwise.
	partition partitioning.FactPartition
}

// Equals returns true if 'a' and 'b' have equal values, false otherwise.
func (a pingResult) Equals(b pingResult) bool {
	if a.healthy != b.healthy {
		return false
	}
	if a.features != b.features {
		return false
	}
	if a.partition == nil && b.partition == nil {
		return true
	}
	if a.partition == nil || b.partition == nil {
		return false
	}
	if a.partition.Encoding() != b.partition.Encoding() {
		return false
	}
	return a.partition.HashRange().Equals(b.partition.HashRange())
}

// parsePingResult extracts information from the given StatsResult. If it can't
// do so, it returns a zero result (with 'healthy' set to false) and a
// descriptive error.
//
// TODO: Should we create a dedicated RPC for this purpose?
func parsePingResult(stats *rpc.StatsResult) (pingResult, error) {
	var res pingResult
	switch {
	case strings.HasPrefix(stats.ViewType, "diskview/"):
		var err error
		res.partition, err = partitioning.PartitionFromCarouselHashPartition(stats.HashPartition)
		if err != nil {
			return pingResult{}, err
		}
		switch res.partition.Encoding() {
		case rpc.KeyEncodingPOS:
			res.features = DiagnosticsFeature | CarouselFeature | HashPOFeature
		case rpc.KeyEncodingSPO:
			res.features = DiagnosticsFeature | CarouselFeature | HashSPFeature
		default:
			return pingResult{}, fmt.Errorf("unsupported DiskView partition encoding: %v",
				res.partition.Encoding().String())
		}
	case stats.ViewType == "txtimeout":
		res.features = DiagnosticsFeature
	default:
		return pingResult{}, fmt.Errorf("unsupported view type: %v", stats.ViewType)
	}
	res.healthy = true
	return res, nil
}
