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

// Command log-client is a tool for low-level access to Beam log servers. It's
// intended for diagnosing problems, recovering from disasters, and manual
// operations.
package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"

	docopt "github.com/docopt/docopt-go"
	"github.com/ebay/beam/blog"
	"github.com/ebay/beam/blog/logspecclient"
	"github.com/ebay/beam/config"
	"github.com/ebay/beam/discovery"
	"github.com/ebay/beam/logentry"
	"github.com/ebay/beam/logentry/logencoder"
	"github.com/ebay/beam/util/clocks"
	"github.com/ebay/beam/util/debuglog"
	"github.com/sirupsen/logrus"
)

const usage = `log-client is a command-line tool for low-level access to Beam log servers.

Usage:
  log-client info [SERVER...]
  log-client diff [-s START -e END -m MAX_ERRORS] [SERVER...]
  log-client appendVersion --force VERSION [SERVER...]
  log-client discardPrefix --force FIRST_INDEX [SERVER...]

Options:
  -s START --start START                The Log Index to start from. Defaults to the start of the log.
  -e END --end END                      The Log Index to end at. Defaults to the end of the log.
  -m MAX_ERRORS --maxErrors MAX_ERRORS  Abort the diff after this many errors found [default: 9999].
  --force                               Indicates command(s) will create side effect which can't be rolled back

Examples:
  # Get the log store statistics.
  log-client info

  # diff the entire log on these two log server nodes.
  log-client diff localhost:20011 localhost:20012

  # diff the first 1M log entries across 3 log server nodes.
  log-client diff -e 1000000 localhost:20011 localhost:20021 localhost:20031

  # Append version command to the log with the specified log version. All of the
  # Beam services will panic if the specified version is not supported.
  log-client appendVersion 1

  # Discard every entry up to and excluding the specified index. After the
  # execution of this command the log's first index will be set to FIRST_INDEX.
  log-client discardPrefix 20
`

type options struct {
	Servers   []string `docopt:"SERVER"`
	endpoints []*discovery.Endpoint
	// Info
	Info bool `docopt:"info"`
	// AppendVersion
	AppendVersion bool `docopt:"appendVersion"`
	// DiscardPrefix
	DiscardPrefix bool `docopt:"discardPrefix"`
	//  Version
	Version int32 `docopt:"VERSION"`
	// First log index
	FirstIndexString string `docopt:"FIRST_INDEX"`
	FirstIndex       uint64
	Force            bool
	// Diff
	Diff             bool   `docopt:"diff"`
	StartIndexString string `docopt:"-s,--start"`
	StartIndex       uint64
	EndIndexString   string `docopt:"-e,--end"`
	EndIndex         uint64
	MaxErrors        int `docopt:"--maxErrors"`
}

func parseArgs(args []string) (*options, error) {
	opts, err := docopt.ParseArgs(usage, args, "")
	if err != nil {
		return nil, fmt.Errorf("error parsing command-line arguments: %v", err)
	}
	var options options
	err = opts.Bind(&options)
	if err != nil {
		return nil, fmt.Errorf("error binding command-line arguments: %v\nfrom: %+v", err, opts)
	}
	if len(options.Servers) == 0 {
		options.Servers = []string{"localhost:20011"}
	}
	for _, server := range options.Servers {
		host, port, err := net.SplitHostPort(server)
		if err != nil {
			return nil, fmt.Errorf("invalid server address: %s, error: %v", server, err)
		}
		e := discovery.Endpoint{Network: "tcp", Host: host, Port: port}
		options.endpoints = append(options.endpoints, &e)
	}
	uint64Param := func(paramName, val string, def uint64) (uint64, error) {
		if val != "" {
			out, err := strconv.ParseUint(val, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("unable to parse %s value %v: %v", paramName, val, err)
			}
			return out, nil
		}
		return def, nil
	}
	options.FirstIndex, err = uint64Param("FIRST_INDEX", options.FirstIndexString, 0)
	if err != nil {
		return nil, err
	}
	options.StartIndex, err = uint64Param("start", options.StartIndexString, 1)
	if err != nil {
		return nil, err
	}
	options.EndIndex, err = uint64Param("end", options.EndIndexString, 0)
	return &options, err
}

func main() {
	debuglog.Configure(debuglog.Options{})
	options, err := parseArgs(os.Args[1:])
	if err != nil {
		logrus.Fatalf("Command failure: %v", err)
	}
	ctx := context.Background()
	locator := discovery.NewStaticLocator(options.endpoints)
	beamLog, err := logspecclient.New(ctx, &config.Beam{}, locator)
	if err != nil {
		logrus.Fatalf("Unable to connect to log store: %v", err)
	}

	switch {
	case options.Info:
		err := info(ctx, beamLog, os.Stdout)
		if err != nil {
			logrus.Fatalf("Error executing Info: %v", err)
		}
	case options.AppendVersion:
		err := appendVersion(ctx, beamLog, options.Version, os.Stdout)
		if err != nil {
			logrus.Fatalf("Error executing AppendVersion: %v", err)
		}
	case options.DiscardPrefix:
		err := discardPrefix(ctx, beamLog, options.FirstIndex, os.Stdout)
		if err != nil {
			logrus.Fatalf("Error executing DiscardPrefix: %v", err)
		}
	case options.Diff:
		if err := diffLog(ctx, options, os.Stdout); err != nil {
			logrus.Fatalf("Error diffing log: %v", err)
		}

	default:
		logrus.Fatalf("Command not supported")
	}
}

// An alias for the normal clock. This is swapped out for unit tests.
var clock = clocks.Wall

func info(ctx context.Context, beamLog blog.BeamLog, w io.Writer) error {
	fmt.Fprintf(w, "\nInvoking Info\n\n")
	start := clock.Now()
	info, err := beamLog.Info(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintln(w, "Got Info Reply")
	fmt.Fprintf(w, "FirstIndex: %10d\n", info.FirstIndex)
	fmt.Fprintf(w, "LastIndex:  %10d\n", info.LastIndex)
	fmt.Fprintf(w, "BytesUsed:  %10s\n", formatBytes(info.BytesUsed))
	fmt.Fprintf(w, "BytesTotal: %10s\n", formatBytes(info.BytesTotal))

	fmt.Fprintf(w, "\nInfo took %s\n", clock.Now().Sub(start))
	return nil
}

func formatBytes(bytes uint64) string {
	units := []string{"B", "KB", "MB", "GB", "TB"}
	v := float64(bytes)
	unitIdx := 0
	decimals := 0
	for unitIdx < len(units)-1 && v > 10240 {
		v = v / 1024
		unitIdx++
		decimals = 1
	}

	return fmt.Sprintf("%.*f%s", decimals, v, units[unitIdx])
}

func appendVersion(ctx context.Context, beamLog blog.BeamLog, version int32, w io.Writer) error {
	fmt.Fprintf(w,
		"\nInvoking Append (VersionCommand with MoveToVersion %v)\n", version)
	start := clock.Now()
	cmd := logentry.VersionCommand{
		MoveToVersion: version,
	}
	data := logencoder.Encode(&cmd)
	index, err := beamLog.AppendSingle(ctx, data)
	if err != nil {
		return err
	}
	fmt.Fprintf(w,
		"Appended VersionCommand with version %v at index %v\n", version, index)
	fmt.Fprintf(w, "AppendVersion took %s\n", clock.Now().Sub(start))
	return nil
}

func discardPrefix(ctx context.Context, beamLog blog.BeamLog, firstIndex uint64, w io.Writer) error {
	fmt.Fprintf(w, "\nInvoking Discard with firstIndex %v\n", firstIndex)
	start := clock.Now()
	err := beamLog.Discard(ctx, firstIndex)
	if err != nil {
		return err
	}
	fmt.Fprint(w, "Discard completed\n")
	fmt.Fprintf(w, "Discard took %s\n", clock.Now().Sub(start))
	return nil
}
