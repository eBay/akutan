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
	"net/http"
	_ "net/http/pprof" // enable pprof endpoints
	"sync"

	"github.com/ebay/akutan/api/impl/kgstats"
	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/config"
	"github.com/ebay/akutan/query"
	"github.com/ebay/akutan/viewclient"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

// New returns a new instance of the API server, this exposes both an HTTP & gRPC API for consumers of the Akutan system to use.
// The returned Server instance will not start handling traffic until a subsequent call to Server.Run()
func New(cfg *config.Akutan, src *viewclient.Client, aLog blog.AkutanLog) *Server {
	s := &Server{
		cfg:    cfg,
		source: src,
		aLog:   aLog,
	}
	s.statsFetcher = kgstats.NewFetcher(s.getStats)
	s.queryEngine = query.New(s.statsFetcher.Last, s.source)
	return s
}

// Server is an implementation of the external gRPC & HTTP interfaces to Akutan
type Server struct {
	cfg          *config.Akutan
	source       *viewclient.Client
	aLog         blog.AkutanLog
	statsFetcher *kgstats.Fetcher
	queryEngine  queryEngine
	lock         sync.Mutex
	locked       struct {
		recentIndex blog.Index
	}
}

// queryEngine provides an abstraction from query execution to aid in testing.
type queryEngine interface {
	Query(ctx context.Context, index blog.Index, rawQuery string,
		opt query.Options, resCh chan<- query.ResultChunk) error
}

// Run will start listening for gRPC & HTTP requests.
// This function will block until the server is shutdown.
func (s *Server) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.statsFetcher.Run(ctx)

	if err := s.startGRPC(); err != nil {
		return err
	}

	m := httprouter.New()

	m.GET("/ride", s.rideCarousel)
	m.GET("/factStats", s.factStats)
	m.POST("/q", s.queryHTTP)

	m.POST("/profile", s.profile)
	// prometheus metrics
	m.Handler("GET", "/metrics", promhttp.Handler())
	m.GET("/stats", s.stats)
	m.GET("/stats.txt", s.statsTable)
	m.POST("/updateStats", s.updateStats)
	m.GET("/viewreg", s.dumpViewRegistry)
	m.POST("/logLevel/:viewIndex", s.setLogLevel)

	m.NotFound = http.DefaultServeMux
	logger := func(w http.ResponseWriter, r *http.Request) {
		log.Debugf("[API] %v %v", r.Method, r.URL)
		m.ServeHTTP(w, r)
	}
	return http.ListenAndServe(s.cfg.API.HTTPAddress, http.HandlerFunc(logger))
}
