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

// Command akutan-diskview runs a DiskView daemon.
package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"

	_ "github.com/ebay/akutan/blog/kafka"         // side-effect: registers "kafka" blog implementation
	_ "github.com/ebay/akutan/blog/logspecclient" // side-effect: registers "logspec" blog implementation
	"github.com/ebay/akutan/config"
	"github.com/ebay/akutan/diskview"
	_ "github.com/ebay/akutan/diskview/rocksdb" // side-effect: registers "rocksdb" database implementation
	"github.com/ebay/akutan/util/debuglog"
	"github.com/ebay/akutan/util/profiling"
	"github.com/ebay/akutan/util/signals"
	"github.com/ebay/akutan/util/tracing"
	log "github.com/sirupsen/logrus"
)

func main() {
	debuglog.Configure(debuglog.Options{})
	cfgFile := flag.String("cfg", "config.json", "Config file")
	startProfile := flag.String("sp", "", "If set will generate a CPU Profile to the named file for startup processing [unless the view hits the HWM]")
	logLevel := flag.String("log", "info", "Initial logging level [can be changed later via API call]")
	pprof := flag.String("pprof", "", "If set will start a HTTP server with the pprof endpoints enabled")
	flag.Parse()

	ll, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.Fatalf("Unable to parse logLevel: %v", err)
	}
	log.SetLevel(ll)

	cfg, err := config.Load(*cfgFile)
	if err != nil {
		log.Fatalf("Unable to load configuration: %v", err)
	}
	log.Infof("Using config: %+v", cfg)

	tracer, err := tracing.New("akutan-diskview", cfg.Tracing)
	if err != nil {
		log.Fatalf("Unable to initialize distributed tracing: %v", err)
	}
	defer tracer.Close()

	if *pprof != "" {
		log.Infof("Starting pprof http endpoint on %s", *pprof)
		go http.ListenAndServe(*pprof, nil)
	}

	dv, err := diskview.New(cfg)
	if err != nil {
		log.Fatalf("Unable to initialize partition: %v", err)
	}
	defer dv.Close()
	err = dv.Start()
	if err != nil {
		log.WithError(err).Panic("Error starting disk view")
	}
	if *startProfile != "" {
		profiling.StartupProfile(*startProfile, dv)
	}

	signals.WaitForQuit()
	log.Info("Akutan DiskView Exiting")
}
