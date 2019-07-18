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

package viewreg

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ebay/akutan/discovery"
	"github.com/ebay/akutan/partitioning"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/clocks"
	grpcclientutil "github.com/ebay/akutan/util/grpc/client"
	"github.com/ebay/akutan/util/parallel"
	"github.com/ebay/akutan/util/random"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// Tests that a new Registry with no views returns nothing.
func Test_Registry_empty(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	registry := NewRegistry(ctx, discovery.NewStaticLocator(nil))
	assert.False(start.After(registry.LastChanged()),
		"NewRegistry should set lastChanged")
	assert.Len(registry.AllViews(), 0)
	cancel()
	assert.Len(registry.AllViews(), 0)
}

// This tests AllViews(), HealthyViews(), and Summary().
func Test_Registry_AllViews_HealthyViews_Summary(t *testing.T) {
	assert := assert.New(t)
	clock := advancingClock{clocks.NewMock(), time.Nanosecond}
	registry := &Registry{
		clock: clock,
	}
	views := make(map[string]*monitoredView)
	addView := func(host string, healthy bool, features Feature) {
		view := &monitoredView{}
		view.cancel = func() {}
		view.locked.template.connReady = healthy
		view.locked.template.pingHealthy = healthy
		view.locked.template.Endpoint = &discovery.Endpoint{Network: "tcp", Host: host, Port: "80"}
		view.locked.template.features = features
		views[host+":80"] = view
	}
	addView("v1", false, DiagnosticsFeature)
	addView("v2", true, DiagnosticsFeature)
	addView("v3", true, DiagnosticsFeature|CarouselFeature|HashPOFeature)
	addView("v4", true, DiagnosticsFeature|CarouselFeature|HashSPFeature)
	addView("v5", true, CarouselFeature|HashSPFeature)
	addView("v6", true, CarouselFeature|HashPOFeature)
	addView("v7", true, 0)
	addView("v8", true, DiagnosticsFeature|CarouselFeature|HashSPFeature|HashPOFeature)
	addView("v9", false, DiagnosticsFeature|CarouselFeature|HashSPFeature|HashPOFeature)

	registry.lock.Lock()
	registry.locked.views = views
	registry.locked.lastChanged = registry.clock.Now()
	registry.lock.Unlock()

	assert.Equal("v1 v2 v3 v4 v5 v6 v7 v8 v9", getHosts(registry.AllViews()), "AllViews")
	assert.Equal("v2 v3 v4 v5 v6 v7 v8", getHosts(registry.HealthyViews()), "Diagnostics")
	assert.Equal("v2 v3 v4 v8", getHosts(registry.HealthyViews(DiagnosticsFeature)), "Diagnostics")
	assert.Equal("v3 v4 v5 v6 v8", getHosts(registry.HealthyViews(CarouselFeature)), "Carousel")
	assert.Equal("v3 v6 v8", getHosts(registry.HealthyViews(HashPOFeature)), "HashPO")
	assert.Equal("v4 v5 v8", getHosts(registry.HealthyViews(HashSPFeature)), "HashSP")
	assert.Equal("v3 v4 v8", getHosts(registry.HealthyViews(DiagnosticsFeature, CarouselFeature)),
		"Diagnostics and Carousel")

	// Test Summary(), which is convenient to do in here after all the above setup.
	var fields []string
	clock.Advance(1234*time.Millisecond - time.Nanosecond)
	for key, value := range registry.Summary() {
		fields = append(fields, fmt.Sprintf("%v: %v", key, value))
	}
	sort.Strings(fields)
	assert.Equal(`
CarouselViews: 5
DiagnosticsViews: 4
HashPOViews: 3
HashSPViews: 3
allViews: 9
healthyViews: 7
timeSinceLastChange: 1.234s`,
		"\n"+strings.Join(fields, "\n"))
}

// getHosts returns a space-delimited, sorted list of endpoint hosts from the given views.
func getHosts(views []View) string {
	hosts := make([]string, len(views))
	for i := range views {
		hosts[i] = views[i].Endpoint.Host
	}
	sort.Strings(hosts)
	return strings.Join(hosts, " ")
}

func Test_Registry_WaitForStable(t *testing.T) {
	assert := assert.New(t)
	clock := clocks.NewMock()
	registry := &Registry{
		clock: clock,
	}
	registry.locked.lastChanged = clock.Now().Add(-time.Second)
	registry.locked.changedCh = make(chan struct{})

	// Expired and no need to wait -> return immediately, no error.
	err := registry.WaitForStable(expiredContext(), 0*time.Second)
	assert.NoError(err)

	// Expired and need to wait -> return immediately with error.
	err = registry.WaitForStable(expiredContext(), time.Hour)
	assert.EqualError(err, "context canceled")

	// Really need to wait.
	start := clock.Now()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for ctx.Err() == nil {
			clock.Advance(10 * time.Minute)
			registry.lock.Lock()
			registry.changedLocked()
			registry.lock.Unlock()
		}
	}()
	err = registry.WaitForStable(ctx, time.Hour)
	cancel()
	assert.NoError(err)
	assert.True(registry.LastChanged().Sub(start) > time.Hour)
}

// expiredContext returns a context that's already been canceled.
func expiredContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func Test_Registry_WaitForChange(t *testing.T) {
	assert := assert.New(t)
	registry := &Registry{
		clock: clocks.Wall,
	}
	registry.locked.lastChanged = time.Now()
	registry.locked.changedCh = make(chan struct{})
	start := registry.LastChanged()

	// Context expired but registry already changed -> no error.
	err := registry.WaitForChange(expiredContext(), start.Add(-time.Nanosecond))
	assert.NoError(err)

	// Context expired but registry hasn't changed -> error.
	err = registry.WaitForChange(expiredContext(), start)
	assert.EqualError(err, "context canceled")

	// As if WaitForChange had to wait on the channel.
	wait := parallel.Go(func() {
		registry.lock.RLock()
		ch := registry.locked.changedCh
		registry.lock.RUnlock()
		ch <- struct{}{} // rendezvous
		registry.lock.Lock()
		registry.locked.lastChanged = time.Now()
		registry.lock.Unlock()
	})
	err = registry.WaitForChange(context.Background(), start)
	assert.NoError(err)
	wait()
	assert.True(registry.LastChanged().After(start))
}

func Test_Registry_Dump(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	endpoints := []*discovery.Endpoint{
		// Hope no one runs a gRPC server on port 7 or 8.
		{Network: "tcp", Host: "localhost", Port: "7"},
		{Network: "tcp", Host: "localhost", Port: "8"},
	}
	registry := NewRegistry(ctx, discovery.NewStaticLocator(endpoints))
	clock := advancingClock{clocks.NewMock(), time.Nanosecond}
	registry.clock = clock
	waitForViews(t, registry, 2)
	registry.lock.Lock()
	for _, view := range registry.locked.views {
		view.locked.template.conn.Close()
	}
	clock.Advance(time.Second)
	registry.changedLocked()
	registry.lock.Unlock()
	var b strings.Builder
	clock.Advance(1234 * time.Millisecond)
	registry.Dump(&b)
	assert.Equal(`
View Registry
Last changed: 1.2s ago
0: View{Endpoint:"tcp://localhost:7" Partition: Features: Healthy:false}
1: View{Endpoint:"tcp://localhost:8" Partition: Features: Healthy:false}
`, "\n"+b.String())
}

// waitForViews sleeps until the registry has 'count' views or 1s goes by. It
// then returns true if the registry has exactly 'count' views, or it fails the
// test and returns false.
func waitForViews(t *testing.T, registry *Registry, count int) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for ctx.Err() == nil {
		start := registry.LastChanged()
		if len(registry.AllViews()) == count {
			return true
		}
		registry.WaitForChange(ctx, start)
	}
	return assert.Len(t, registry.AllViews(), count)
}

// Registry.Summary was tested up above in
// Test_Registry_AllViews_HealthyViews_Summary.

// changedLocked is covered reasonably well in other tests.
// This makes sure it deals with a very slow clock.
func Test_Registry_changedLocked_slowClock(t *testing.T) {
	assert := assert.New(t)
	clock := slowlyAdvancingClock{clocks.NewMock()}
	registry := &Registry{
		clock: clock,
	}
	registry.lock.Lock()
	start := clock.Now()
	startCh := make(chan struct{})
	registry.locked.lastChanged = start
	registry.locked.changedCh = startCh
	registry.changedLocked()
	end := registry.locked.lastChanged
	endCh := registry.locked.changedCh
	registry.lock.Unlock()
	assert.True(end.After(start))
	assert.False(clock.Now().Before(end))
	assert.NotEqual(startCh, endCh)
	<-startCh
	select {
	case <-endCh:
		assert.FailNow("endCh should be open")
	default:
	}
}

// Seed the pseudo-random number generator for slowlyAdvancingClock.
func init() {
	random.SeedMath()
}

// A Mock clock that advances by 1 ns with a 10% chance each time it's called.
type slowlyAdvancingClock struct {
	*clocks.Mock
}

func (c slowlyAdvancingClock) Now() time.Time {
	if rand.Float32() < .1 {
		c.Advance(time.Nanosecond)
	}
	return c.Mock.Now()
}

func Test_Registry_discover(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	endpoints := []*discovery.Endpoint{
		// Hope no one runs a gRPC server on port 7 or 8.
		{Network: "tcp", Host: "localhost", Port: "7"},
		{Network: "tcp", Host: "localhost", Port: "8"},
	}
	locator := discovery.NewStaticLocator(endpoints[:1])
	registry := NewRegistry(ctx, locator)
	if waitForViews(t, registry, 1) {
		view := registry.AllViews()[0]
		assert.Equal(endpoints[0], view.Endpoint)
		assert.Nil(view.Partition)
		assert.NotNil(view.conn)
		assert.False(view.Provides(DiagnosticsFeature))
	}
	assert.Len(registry.HealthyViews(DiagnosticsFeature), 0)
	assert.Len(registry.HealthyViews(CarouselFeature), 0)

	// Test that discover() continues to find more views.
	locator.Set(endpoints)
	waitForViews(t, registry, 2)

	// Test shutdown path of discover().
	shutdownStart := time.Now()
	cancel()
	waitForViews(t, registry, 0)
	assert.False(shutdownStart.After(registry.LastChanged()),
		"discover shutdown should update lastChanged")
}

func Test_Registry_loadEndpoints(t *testing.T) {
	assert := assert.New(t)
	registry := &Registry{
		// Use expired context here so gRPC won't initiate any connections.
		ctx:   expiredContext(),
		clock: clocks.Wall,
	}
	registry.locked.changedCh = make(chan struct{})
	goodCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	views := make(map[string]*monitoredView)
	addView := func(endpoint *discovery.Endpoint, healthy bool, features Feature) *monitoredView {
		view := &monitoredView{}
		view.ctx, view.cancel = context.WithCancel(goodCtx)
		view.locked.template.connReady = healthy
		view.locked.template.pingHealthy = healthy
		view.locked.template.Endpoint = endpoint
		view.locked.template.features = features
		views[endpoint.HostPort()] = view
		return view
	}
	endpoints := []*discovery.Endpoint{
		{Host: "removed", Port: "80"},
		{Host: "same", Port: "80"},
		{Host: "updated", Port: "80"},
		{Host: "added", Port: "80"},
	}
	removed := addView(endpoints[0], true, DiagnosticsFeature)
	same := addView(endpoints[1], true, DiagnosticsFeature)
	updated := addView(endpoints[2], true, DiagnosticsFeature)

	registry.lock.Lock()
	registry.locked.views = views
	registry.lock.Unlock()

	endpoints[2] = &discovery.Endpoint{Host: "updated", Port: "80", Network: "pigeon"}
	start := time.Now()
	registry.loadEndpoints(endpoints[1:])
	assert.False(start.After(registry.LastChanged()),
		"loadEndpoints should update lastChanged")
	assert.Equal("added same updated", getHosts(registry.AllViews()))
	assert.Error(removed.ctx.Err())
	assert.NoError(same.ctx.Err())
	assert.NoError(updated.ctx.Err())
	registry.lock.Lock()
	assert.Equal("pigeon", updated.locked.template.Endpoint.Network)
	registry.lock.Unlock()

	start = time.Now()
	registry.loadEndpoints(endpoints[1:])
	assert.False(registry.LastChanged().After(start),
		"loadEndpoints shouldn't have updated lastChanged")
	assert.Equal("added same updated", getHosts(registry.AllViews()))
}

func Test_Registry_watchConnState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := &Registry{
		clock: clocks.Wall,
	}
	registry.locked.changedCh = make(chan struct{})
	view := &monitoredView{
		ctx: ctx,
	}

	waitForConnReady := func(shouldBeReady bool) {
		for {
			registry.lock.RLock()
			start := registry.locked.lastChanged
			connReady := view.locked.template.connReady
			registry.lock.RUnlock()
			if connReady == shouldBeReady {
				break
			}
			registry.WaitForChange(ctx, start)
		}
	}

	server := newMockServer(t)
	defer server.close()
	conn := grpcclientutil.InsecureDialContext(ctx, server.endpoint.HostPort())
	defer conn.Close()

	go registry.watchConnState(view, conn)
	waitForConnReady(true)
	server.close()
	waitForConnReady(false)
}

// Testing runPings would be too much trouble due to I/O and clock.

func Test_ping(t *testing.T) {
	tests := []struct {
		name         string
		stats        func(req *rpc.StatsRequest) (*rpc.StatsResult, error)
		expHealthy   bool
		expFeatures  Feature
		expPartition string
	}{
		{
			name: "ok",
			stats: func(req *rpc.StatsRequest) (*rpc.StatsResult, error) {
				return &rpc.StatsResult{
					ViewType: "diskview/sqlite",
					HashPartition: rpc.CarouselHashPartitionRequest{
						Encoding: rpc.KeyEncodingSPO,
						Start:    0x10,
						End:      0x20,
					},
				}, nil
			},
			expHealthy:   true,
			expFeatures:  DiagnosticsFeature | CarouselFeature | HashSPFeature,
			expPartition: "SPO: 0x0000000000000010 - 0x0000000000000020",
		},
		{
			name: "RPC failed",
			stats: func(req *rpc.StatsRequest) (*rpc.StatsResult, error) {
				return nil, errors.New("ants in pants")
			},
			expHealthy:   false,
			expFeatures:  0,
			expPartition: "(nil)",
		},
		{
			name: "parsePingResult error",
			stats: func(req *rpc.StatsRequest) (*rpc.StatsResult, error) {
				return &rpc.StatsResult{
					ViewType: "banana",
				}, nil
			},
			expHealthy:   false,
			expFeatures:  0,
			expPartition: "(nil)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			logrus.SetLevel(logrus.TraceLevel)
			registry := &Registry{
				clock: clocks.Wall,
			}
			registry.locked.changedCh = make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stub := &mockPingStub{stats: test.stats}
			view := &monitoredView{
				ctx: ctx,
			}
			var result pingResult
			result = registry.ping(view, "myServer", stub, result)

			registry.lock.RLock()
			defer registry.lock.RUnlock()
			assert.Equal(test.expHealthy, result.healthy, "result.healthy")
			assert.Equal(test.expHealthy, view.locked.template.pingHealthy, "view.pingHealthy")
			assert.Equal(test.expFeatures, result.features, "result.features")
			assert.Equal(test.expFeatures, view.locked.template.features, "view.features")
			assert.Equal(test.expPartition,
				partitioning.String(result.partition), "result.partition")
			assert.Equal(test.expPartition,
				partitioning.String(view.locked.template.Partition), "view.Partition")
		})
	}
}

type mockPingStub struct {
	stats func(req *rpc.StatsRequest) (*rpc.StatsResult, error)
}

func (stub *mockPingStub) Stats(ctx context.Context, req *rpc.StatsRequest, opts ...grpc.CallOption) (*rpc.StatsResult, error) {
	return stub.stats(req)
}

type mockServer struct {
	listener net.Listener
	endpoint discovery.Endpoint
	grpc     *grpc.Server
}

func newMockServer(t *testing.T) *mockServer {
	server := &mockServer{}
	var err error
	server.listener, err = net.Listen("tcp", "127.0.0.1:")
	require.NoError(t, err, "net.Listen")

	addr := server.listener.Addr()
	server.endpoint.Network = addr.Network()
	server.endpoint.Host, server.endpoint.Port, err = net.SplitHostPort(addr.String())
	require.NoError(t, err, "net.SplitHostPort(server.listener...)")

	server.grpc = grpc.NewServer()

	go server.grpc.Serve(server.listener)
	return server
}

func (server *mockServer) close() {
	server.listener.Close()
	server.grpc.Stop()
}

// If this test fails, the grpc library has changed its set of states, and the
// code in monitor() needs to be audited.
func Test_grpc_clientConn_States(t *testing.T) {
	assert.Equal(t, []int{0, 1, 2, 3, 4}, []int{
		int(connectivity.Idle),
		int(connectivity.Connecting),
		int(connectivity.Ready),
		int(connectivity.TransientFailure),
		int(connectivity.Shutdown),
	})
	assert.Equal(t, "Invalid-State",
		connectivity.State(5).String())
}

func Test_pingResult_Equals(t *testing.T) {
	vals := map[string]pingResult{
		"zero": pingResult{},
		"healthyButUseless": pingResult{
			healthy: true,
		},
		"txtimeout": pingResult{
			healthy:  true,
			features: DiagnosticsFeature,
		},
		"hashpo0/0": pingResult{
			healthy:  true,
			features: DiagnosticsFeature | CarouselFeature | HashPOFeature,
		},
		"hashpo1/2": pingResult{
			healthy:   true,
			features:  DiagnosticsFeature | CarouselFeature | HashPOFeature,
			partition: partitioning.NewHashPredicateObjectPartition(1, 2),
		},
		"hashpo0/1": pingResult{
			healthy:   true,
			features:  DiagnosticsFeature | CarouselFeature | HashPOFeature,
			partition: partitioning.NewHashPredicateObjectPartition(0, 1),
		},
		"hashsp1/2": pingResult{
			healthy:   true,
			features:  DiagnosticsFeature | CarouselFeature | HashPOFeature,
			partition: partitioning.NewHashSubjectPredicatePartition(1, 2),
		},
		"hashsp0/1": pingResult{
			healthy:   true,
			features:  DiagnosticsFeature | CarouselFeature | HashPOFeature,
			partition: partitioning.NewHashSubjectPredicatePartition(0, 1),
		},
	}
	// Assert that all values equal themselves and don't equal each other.
	for name, val := range vals {
		for otherName, otherVal := range vals {
			assert.Equal(t,
				name == otherName,
				val.Equals(otherVal),
				"%s == %s", name, otherName)
		}
	}
}

func Test_parsePingResult(t *testing.T) {
	tests := []struct {
		name      string
		err       string
		stats     rpc.StatsResult
		features  Feature
		partition string
	}{
		{
			name: "all zero",
			err:  "unsupported view type: ",
		},
		{
			name: "megaview",
			err:  "unsupported view type: megaview",
			stats: rpc.StatsResult{
				ViewType: "megaview",
			},
		},
		{
			name: "sqlite",
			err:  "unexpected FactKeyEncoding value of KeyEncodingUnknown",
			stats: rpc.StatsResult{
				ViewType: "diskview/sqlite",
			},
		},
		{
			name: "bad encoding",
			err:  "unexpected FactKeyEncoding value of 1957",
			stats: rpc.StatsResult{
				ViewType: "diskview/sqlite",
				HashPartition: rpc.CarouselHashPartitionRequest{
					Encoding: 1957,
				},
			},
		},
		{
			name: "ok HashPO",
			stats: rpc.StatsResult{
				ViewType: "diskview/sqlite",
				HashPartition: rpc.CarouselHashPartitionRequest{
					Encoding: rpc.KeyEncodingPOS,
					Start:    0,
					End:      0, // means infinity
				},
			},
			features:  DiagnosticsFeature | CarouselFeature | HashPOFeature,
			partition: "POS: 0x00.. - ∞",
		},
		{
			name: "ok HashSP",
			stats: rpc.StatsResult{
				ViewType: "diskview/sqlite",
				HashPartition: rpc.CarouselHashPartitionRequest{
					Encoding: rpc.KeyEncodingSPO,
					Start:    0x10,
					End:      0x20,
				},
			},
			features:  DiagnosticsFeature | CarouselFeature | HashSPFeature,
			partition: "SPO: 0x0000000000000010 - 0x0000000000000020",
		},
		{
			name: "ok txtimeout",
			stats: rpc.StatsResult{
				ViewType: "txtimeout",
				// ignored
				HashPartition: rpc.CarouselHashPartitionRequest{
					Encoding: rpc.KeyEncodingSPO,
					Start:    0x10,
					End:      0x20,
				},
			},
			features: DiagnosticsFeature,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			result, err := parsePingResult(&test.stats)
			if test.err == "" {
				assert.NoError(err)
				assert.True(result.healthy)
			} else {
				assert.EqualError(err, test.err)
				assert.False(result.healthy)
			}
			assert.Equal(test.features, result.features, "features")
			if test.partition == "" {
				assert.Nil(result.partition, "partition")
			} else {
				if assert.NotNil(result.partition, "partition") {
					assert.Equal(test.partition, partitioning.String(result.partition),
						"partition")
				}
			}
		})
	}
}

// A Mock clock that advances by 1 ns every time it's called.
type advancingClock struct {
	*clocks.Mock
	amount time.Duration
}

func (c advancingClock) Now() time.Time {
	c.Advance(time.Nanosecond)
	return c.Mock.Now()
}
