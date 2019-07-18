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

// Package txtimeoutview implements a view service that times out slow
// transactions and measures the log's latency.
package txtimeoutview

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ebay/akutan/blog"
	_ "github.com/ebay/akutan/blog/kafka"         // side-effect: registers "kafka" blog implementation
	_ "github.com/ebay/akutan/blog/logspecclient" // side-effect: registers "logspec" blog implementation
	"github.com/ebay/akutan/config"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/txtimeoutview/logping"
	"github.com/ebay/akutan/txtimeoutview/txtimer"
	"github.com/ebay/akutan/util/clocks"
	"github.com/ebay/akutan/util/cmp"
	grpcserverutil "github.com/ebay/akutan/util/grpc/server"
	"github.com/ebay/akutan/util/profiling"
	"github.com/ebay/akutan/viewclient"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

var startTime = time.Now()

// New returns a new View that implements the transaction timeout functionality. This
// watches the log for transactions that were started but not committed or aborted in
// a reasonable timeframe, and writes an abort entry to the log for them.
//
// The given context is for the background activity of the view, not just for
// the call to New.
func New(backgroundCtx context.Context, cfg *config.Akutan) (*View, error) {
	if cfg.TxTimeoutView == nil {
		return nil, fmt.Errorf("txTimeoutView field missing in Akutan config")
	}
	ctx, abort := context.WithCancel(backgroundCtx)
	v := &View{
		ctx: ctx,
		cfg: cfg,
	}
	var err error
	v.viewClient, err = viewclient.New(cfg)
	if err != nil {
		abort()
		return nil, err
	}
	go func() {
		<-ctx.Done()
		v.viewClient.Close()
	}()
	v.aLog, err = blog.New(ctx, cfg)
	if err != nil {
		abort()
		return nil, err
	}
	v.logPinger, err = logping.New(ctx, cfg, v.getReplayPosition, logping.Options{
		Appender:             v.aLog,
		PingsUntilDisconnect: 10,
	})
	if err != nil {
		abort()
		return nil, err
	}
	v.txTimer = txtimer.New(ctx, v.aLog, v.getReplayPosition, txtimer.Options{})
	// This is a workaround to silence go vet. ctx should only be canceled on the
	// failure paths above, since the ctx has the same lifetime as the View. go vet
	// wants it to be canceled even on the success path. Assigning anything to
	// 'abort' suppresses the warning.
	abort = nil
	return v, nil
}

// View is an instance of the transaction timeout view
type View struct {
	// The background goroutines exit when this context is closed.
	ctx        context.Context
	cfg        *config.Akutan
	viewClient *viewclient.Client
	logPinger  *logping.Pinger
	txTimer    *txtimer.TxTimer
	aLog       blog.AkutanLog
}

// Start will start the gRPC server, and the monitoring the log, aborting relevant transactions
func (view *View) Start() error {
	go view.txTimer.Run()
	go view.logPinger.Run()
	err := view.startGRPCServer()
	if err != nil {
		return fmt.Errorf("error starting gRPC server: %v", err)
	}
	view.startHTTPServer()
	return nil
}

func (view *View) startHTTPServer() {
	if view.cfg.TxTimeoutView.MetricsAddress == "" {
		log.Warnf("Cannot start HTTP server as 'metricsAddress' configuration not set")
		return
	}
	http.Handle("/metrics", promhttp.Handler())
	log.Infof("Starting HTTP server for metrics on %v",
		view.cfg.TxTimeoutView.MetricsAddress)
	go func() {
		err := http.ListenAndServe(view.cfg.TxTimeoutView.MetricsAddress, nil)
		if err != nil {
			log.WithError(err).Panic("Failed to start HTTP server for Prometheus endpoint")
		}
	}()
}

// getReplayPosition finds the best position to start log replay for the
// txtimer. For simplicity, getReplayPosition is also currently used to
// initialize the log pinger, although the pinger would boot more efficiently if
// it just used the maximum known position from any server. See
// txtimer.GetReplayPosition and logpinder.GetReplayPosition for details on what they expect.
func (view *View) getReplayPosition(ctx context.Context) (rpc.LogPosition, error) {
	// Try once to get the replay position from the DiskView carousel service, with a timeout.
	attempt := func(ctx context.Context) (rpc.LogPosition, error) {
		ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()

		carouselViews := view.viewClient.CarouselViews(nil)
		status, err := view.viewClient.LogStatus(ctx, carouselViews)
		if err != nil {
			log.WithError(err).Warnf("Failed to get LogStatus")
			return rpc.LogPosition{}, err
		}
		replayPosition, err := status.ReplayPosition()
		if err != nil {
			log.WithError(err).Warnf("Failed to get replay position from LogStatus result")
			return rpc.LogPosition{}, err
		}
		return replayPosition, nil
	}

	// Try repeatedly to get the replay index.
	for {
		replayPosition, err := attempt(ctx)
		if err != nil {
			if err == ctx.Err() {
				return rpc.LogPosition{}, err
			}
			// Avoid spamming the DiskView servers.
			err = clocks.Wall.SleepUntil(ctx, clocks.Wall.Now().Add(time.Second))
			if err != nil {
				return rpc.LogPosition{}, err
			}
			continue
		}
		return replayPosition, nil
	}
}

func (view *View) startGRPCServer() error {
	log.Infof("Starting gRPC server on %v", view.cfg.TxTimeoutView.GRPCAddress)
	listener, err := net.Listen("tcp", view.cfg.TxTimeoutView.GRPCAddress)
	if err != nil {
		return err
	}
	grpcServer := grpcserverutil.NewServer()
	rpc.RegisterDiagnosticsServer(grpcServer, view)
	grpc_prometheus.Register(grpcServer)
	go grpcServer.Serve(listener)
	return nil
}

// SetLogLevel will change the debug log level to the new level described in the request. The log
// level will remain at the provided setting until either another SetLogLevel request is processed
// or the process is restarted.
// This is part of the implementation of the Diagnostics RPC service.
func (view *View) SetLogLevel(ctx context.Context, req *rpc.LogLevelRequest) (*rpc.LogLevelResult, error) {
	log.SetLevel(log.Level(req.NewLevel))
	return &rpc.LogLevelResult{}, nil
}

// Profile will start CPU profiling the process, as described in the ProfileRequest.
// This is part of the implementation of the Diagnostics RPC service.
func (view *View) Profile(ctx context.Context, req *rpc.ProfileRequest) (*rpc.ProfileResult, error) {
	err := profiling.CPUProfileForDuration(req.Filename, req.Duration)
	return &rpc.ProfileResult{}, err
}

// Stats returns the current values for the standard set of view statistics. This is part of the
// implementation of the Diagnostics RPC interface
func (view *View) Stats(ctx context.Context, req *rpc.StatsRequest) (*rpc.StatsResult, error) {
	s := rpc.StatsResult{
		ViewType: "txtimeout",
		UpTime:   time.Since(startTime),
		Hostname: view.cfg.TxTimeoutView.GRPCAddress,
	}
	if strings.HasPrefix(s.Hostname, ":") {
		// Under Kubernetes, the GRPCAddress is usually just a :port.
		osHostname, _ := os.Hostname()
		s.Hostname = osHostname + s.Hostname
	}

	s.LastIndex = uint64(cmp.MinUint64(
		view.txTimer.LastApplied(),
		view.logPinger.LastApplied()))
	s.Txs = uint32(view.txTimer.PendingTxs())
	return &s, nil
}

// UpdateStats is part of the implementation of the Diagnostics RPC API.
// For this view, we have no fact stats, so this is a no-op.
func (view *View) UpdateStats(ctx context.Context, req *rpc.UpdateStatsRequest) (*rpc.UpdateStatsResult, error) {
	return &rpc.UpdateStatsResult{}, nil
}
