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

// Command carousel-client is a low level carousel client tool for helping
// investigate performance etc.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/debuglog"
	log "github.com/sirupsen/logrus"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var fmtr = message.NewPrinter(language.English)

type options struct {
	cfgFile   string
	exclude   string
	host      string
	idFile    string
	dedicated bool
	quiet     bool
}

func main() {
	debuglog.Configure(debuglog.Options{})
	var options options
	flag.StringVar(&options.cfgFile, "cfg", "", "Cluster configuration file, must specify this or -s")
	flag.StringVar(&options.host, "s", ":9980", "Host and port of a single grpc carousel server to connect to (if not using cluster mode)")
	flag.StringVar(&options.idFile, "id", "", "File to write received facts to")
	flag.StringVar(&options.exclude, "x", "", "A comma separated list of views to exclude from the carousel request (only valid with -cfg)")
	flag.BoolVar(&options.dedicated, "dedicated", false, "Set to true to ask for a dedicated (fake) carousel")
	flag.BoolVar(&options.quiet, "quiet", false, "Output less")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage:\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  %s status\n", os.Args[0])
		fmt.Fprintf(flag.CommandLine.Output(), "  %s ride <partition> <numPartition>\n", os.Args[0])
		fmt.Fprintf(flag.CommandLine.Output(), "\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), "\n  Use -s to carousel against a single specific server, or use -cfg to carousel against a cluster\n")
	}
	flag.Parse()
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}
	src, err := options.source()
	if err != nil {
		log.Fatalf("Unable to initialize carousel source: %v\n", err)
	}
	switch flag.Arg(0) {
	case "status":
		if flag.NArg() != 1 {
			flag.Usage()
			os.Exit(1)
		}
		err = src.status()

	case "ride":
		if flag.NArg() != 3 {
			flag.Usage()
			os.Exit(1)
		}
		var p, n int
		p, n, err = parseRideArgs(flag.Args()[1:])
		if err == nil {
			err = src.ride(p, n)
		}
	default:
		flag.Usage()
		os.Exit(1)
	}
	if err != nil {
		log.Fatalf("%s\n", err)
	}
}

type carouselSource interface {
	status() error
	ride(partition, numPartitions int) error
}

func (o *options) source() (carouselSource, error) {
	if o.cfgFile != "" {
		return newClusterSource(o)
	}
	return newSingleSource(o)
}

func (o *options) factSink() (func(*rpc.Fact), func(), error) {
	writeFact := func(f *rpc.Fact) {}
	closer := func() {}
	if o.idFile != "" {
		idFile, err := os.Create(o.idFile)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to create id file: %v", err)
		}
		idWriter := bufio.NewWriter(idFile)
		closer = func() {
			idWriter.Flush()
			idFile.Close()
		}
		writeFact = func(f *rpc.Fact) {
			fmt.Fprintf(idWriter, "%d %d %d %s\n", f.Id, f.Subject, f.Predicate, f.Object)
		}
	}
	return writeFact, closer, nil
}

func (o *options) progressInterval() time.Duration {
	printFreq := time.Second
	if o.quiet {
		printFreq = 10 * time.Second
	}
	return printFreq
}

func parseRideArgs(args []string) (p, n int, err error) {
	p = mustParseInt(args[0])
	n = mustParseInt(args[1])
	if p >= n {
		return 0, 0, fmt.Errorf("partition number %d out range (0-%d)", p, n-1)
	}
	return p, n, nil
}

func mustParseInt(s string) int {
	v, err := strconv.Atoi(s)
	if v < 0 {
		log.Fatalf("Partition settings should not be negative: %d", v)
	}
	if err != nil {
		log.Fatalf("Unable to parse number: %v", err)
	}
	return v
}
