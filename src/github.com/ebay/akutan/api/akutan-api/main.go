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

// Command akutan-api runs an Akutan API server daemon.
package main

import (
	"context"
	"flag"
	"os"

	api "github.com/ebay/akutan/api/impl"
	"github.com/ebay/akutan/blog"
	_ "github.com/ebay/akutan/blog/kafka"         // side-effect: registers "kafka" blog implementation
	_ "github.com/ebay/akutan/blog/logspecclient" // side-effect: registers "logspec" blog implementation
	"github.com/ebay/akutan/config"
	"github.com/ebay/akutan/util/debuglog"
	"github.com/ebay/akutan/util/signals"
	"github.com/ebay/akutan/util/tracing"
	"github.com/ebay/akutan/viewclient"
	log "github.com/sirupsen/logrus"
	_ "google.golang.org/grpc/encoding/gzip" // imported for side-effect of registering compressor
)

func main() {
	debuglog.Configure(debuglog.Options{})
	cfgFile := flag.String("cfg", "config.json", "config file")
	flag.Parse()

	cfg, err := config.Load(*cfgFile)
	if err != nil {
		log.Fatalf("Unable to load configuration: %v", err)
	}
	if cfg.API == nil {
		log.Fatal("api field missing in config")
	}
	log.Infof("Using config: %+v", cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tracer, err := tracing.New("akutan-api", cfg.Tracing)
	if err != nil {
		log.Fatalf("Unable to initialize distributed tracing: %v", err)
	}
	defer tracer.Close()

	viewClient, err := viewclient.New(cfg)
	if err != nil {
		log.Fatalf("Unable to initialize view client: %v", err)
	}
	bLog, err := blog.New(ctx, cfg)
	if err != nil {
		log.Fatalf("Unable to initialize log: %v", err)
	}
	apiServer := api.New(cfg, viewClient, bLog)
	go func() {
		log.Infof("Server::Run returned %v", apiServer.Run())
		os.Exit(-1)
	}()

	signals.WaitForQuit()
	log.Info("Akutan API server exiting")
}
