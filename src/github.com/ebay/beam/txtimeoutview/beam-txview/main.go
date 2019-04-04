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

// Command beam-txview runs a TxTimeoutView daemon.
package main

import (
	"context"
	"flag"

	"github.com/ebay/beam/config"
	"github.com/ebay/beam/txtimeoutview"
	"github.com/ebay/beam/util/debuglog"
	"github.com/ebay/beam/util/signals"
	"github.com/ebay/beam/util/tracing"
	log "github.com/sirupsen/logrus"
)

func main() {
	debuglog.Configure(debuglog.Options{})
	cfgFile := flag.String("cfg", "config.json", "Beam config file")
	flag.Parse()

	cfg, err := config.Load(*cfgFile)
	if err != nil {
		log.Fatalf("Unable to load configuration: %v", err)
	}
	log.Infof("Using config: %+v", cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tracer, err := tracing.New("beam-txtimeout", cfg.Tracing)
	if err != nil {
		log.Fatalf("Unable to initialize distributed tracing: %v", err)
	}
	defer tracer.Close()

	ps, err := txtimeoutview.New(ctx, cfg)
	if err != nil {
		log.Fatalf("Unable to initialize tx timeoutview: %v", err)
	}
	err = ps.Start()
	if err != nil {
		log.WithError(err).Panic("Error starting tx timeout view.")
	}
	signals.WaitForQuit()
	log.Warn("Beam TxTimeoutView Exiting")
}
