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

// Command plank implements a logspec server by storing entries in local memory
// only.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/ebay/beam/util/debuglog"
	"github.com/ebay/beam/util/signals"
	"github.com/sirupsen/logrus"
)

func main() {
	debuglog.Configure(debuglog.Options{})
	var verbosity string
	var options Options
	flag.StringVar(&options.Address, "address", "localhost:20011",
		"Listen address for gRPC server")
	flag.IntVar(&options.Limit, "limit", 1000,
		"If >0, enables auto-discarding but keeps at least this number of entries")
	flag.StringVar(&verbosity, "verbosity", "info",
		"Debug logging level such as 'warn' or 'debug'")
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "plank implements the logspec service API by storing entries in local memory only.")
		fmt.Fprintf(w, "\n")
		fmt.Fprintf(w, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	level, err := logrus.ParseLevel(verbosity)
	if err != nil {
		panic(fmt.Sprintf("Bad value for -verbosity: %v", err))
	}
	logrus.SetLevel(level)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err = NewServer(ctx, &options)
	if err != nil {
		panic(fmt.Sprintf("Could not start sever: %v", err))
	}
	logrus.Infof("plank server started at %v", options.Address)
	signals.WaitForQuit()
	logrus.Warn("Exiting")
}
