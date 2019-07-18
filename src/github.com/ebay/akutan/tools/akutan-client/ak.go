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

// Command bc provides command line access to the akutan GRPC API
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	docopt "github.com/docopt/docopt-go"
	"github.com/ebay/akutan/api"
	"github.com/ebay/akutan/config"
	"github.com/ebay/akutan/msg/facts"
	"github.com/ebay/akutan/query/parser"
	"github.com/ebay/akutan/util/debuglog"
	grpcclientutil "github.com/ebay/akutan/util/grpc/client"
	"github.com/ebay/akutan/util/table"
	"github.com/ebay/akutan/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var fmtr = message.NewPrinter(language.English)

const usage = `akutan-client is a command-line tool for calling the Akutan API service.

Usage:
  akutan-client [--api=HOST -t=DUR --trace=HOST] query [-i=NUM] FILE
  akutan-client [--api=HOST -t=DUR --trace=HOST --format=FORMAT] insert FILE
  akutan-client [--api=HOST -t=DUR --trace=HOST] queryfacts [-i=NUM] [--noextids] [QUERYSTRING]
  akutan-client [--api=HOST -t=DUR --trace=HOST] lookupsp [-i=NUM] SUBJECT PREDICATE
  akutan-client [--api=HOST -t=DUR --trace=HOST] lookuppo [-i=NUM] PREDICATE OBJECT
  akutan-client [--api=HOST -t=DUR --trace=HOST] wipe [-w=WAITFOR]

Options:
  --api=HOST                        Host and port of Akutan API server to connect to [default: localhost:9987]
  -t=DUR, --timeout=DUR             Timeout for RPC calls to Akutan API tier [default: 10s]
  -i=NUM, --index=NUM               Log index to specify in request.
  -w=WAITFOR, --waitfor=WAITFOR     Duration to wait for wipe to complete.
  --noextids                        Don't resolve/print KID externalIDs in fact results.
  --trace=HOST                      Send OpenTracing traces to this collector.
  --format=FORMAT                   File format for inserting facts [default: tsv]

Examples:
  # Multiple line insert fact usage.
  akutan-client insert - <<EOF  
  <car1>  <fits> 	  <part1>
  <part1> rdf:type  motors:part
EOF

  # Query all cars that accept part1.
  akutan-client queryfacts "?c <fits> <part1>"

  # Query all parts that fit car1.
  akutan-client queryfacts "<car1> <fits> ?p"

  # Multiple line query usage to get all parts that fit car1.
  akutan-client query - <<EOF
  SELECT ?p WHERE {
  <car1> <fits> ?p
  ?p rdf:type product:part
  }
EOF
  
  # Lookup facts by subject and predicate.
  akutan-client lookupsp 1430001 1454001
  # Lookup facts by predicate and object at the Log index.
  akutan-client lookuppo --index 99552 1454001 99452001

  # Wipe all existing data from the database.
  akutan-client wipe

`

type options struct {
	// Options
	Index                int64
	NoResolveExternalIDs bool   `docopt:"--noextids"`
	Server               string `docopt:"--api"`
	// Timeout is never zero; it's set to 1 hour if the user passes 0s.
	Timeout          time.Duration
	TimeoutString    string `docopt:"--timeout"`
	TracingCollector string `docopt:"--trace"`

	ObjectString    string `docopt:"OBJECT"`
	Predicate       parser.LiteralID
	PredicateString string `docopt:"PREDICATE"`
	SubjectString   string `docopt:"SUBJECT"`

	// LookupPO
	LookupPO bool `docopt:"lookuppo"`
	Object   api.KGObject

	// LookupSP
	LookupSP bool `docopt:"lookupsp"`
	Subject  parser.LiteralID

	// Insert
	Insert   bool
	Filename string `docopt:"FILE"`
	Format   string

	// Wipe
	Wipe              bool
	WipeWaitFor       time.Duration
	WipeWaitForString string `docopt:"--waitfor"`

	// Query
	Query       bool   `docopt:"query"`
	QueryFacts  bool   `docopt:"queryfacts"` // legacy query format
	QueryString string `docopt:"QUERYSTRING"`
}

func parseArgs() *options {
	opts, err := docopt.ParseDoc(usage)
	if err != nil {
		log.Fatalf("Error parsing command-line arguments: %v", err)
	}
	var options options
	err = opts.Bind(&options)
	if err != nil {
		log.Fatalf("Error binding command-line arguments: %v\nfrom: %+v", err, opts)
	}
	if options.TimeoutString != "" {
		options.Timeout, err = time.ParseDuration(options.TimeoutString)
		if err != nil {
			log.Fatalf("Unable to parse timeout value: %v", err)
		}
	}
	if options.Timeout == 0 {
		options.Timeout = time.Hour
	}
	if options.Wipe {
		if options.WipeWaitForString != "" {
			options.WipeWaitFor, err = time.ParseDuration(options.WipeWaitForString)
			if err != nil {
				log.Fatalf("Unable to parse waitfor value: %v", err)
			}
		}
	}
	return &options
}

func main() {
	debuglog.Configure(debuglog.Options{})
	options := parseArgs()
	ctx := context.Background()

	if options.TracingCollector != "" {
		tracer, err := tracing.New("akutan-client", &config.Tracing{
			Type: "jaeger",
			Locator: config.Locator{
				Type:      "static",
				Addresses: []string{options.TracingCollector},
			},
		})
		if err != nil {
			log.WithError(err).Warn("Could not initialize OpenTracing tracer")
		} else {
			defer tracer.Close()
		}
	}
	span, ctx := opentracing.StartSpanFromContext(ctx, "akutan-client run")
	defer span.Finish()

	conn := grpcclientutil.InsecureDialContext(ctx, options.Server)

	factStore := api.NewFactStoreClient(conn)
	fmt.Println()

	// Most subcommands run under timeoutCtx. However, a few subcommands
	// allow users to enter text from standard input. These use 'ctx' rather
	// than timeoutCtx so that the timeout doesn't count against the user
	// input.
	timeoutCtx, cancelFunc := context.WithTimeout(ctx, options.Timeout)
	defer cancelFunc()

	switch {
	case options.Insert:
		err := insert(ctx, factStore, options)
		if err != nil {
			log.Fatalf("Error executing Insert: %v", err)
		}
	case options.LookupPO:
		err := lookupPO(timeoutCtx, factStore, options)
		if err != nil {
			log.Fatalf("Error executing LookupPO: %v", err)
		}
	case options.LookupSP:
		err := lookupSP(timeoutCtx, factStore, options)
		if err != nil {
			log.Fatalf("Error executing LookupSP: %v", err)
		}
	case options.Wipe:
		err := wipe(timeoutCtx, factStore, options)
		if err != nil {
			log.Fatalf("Error executing wipe: %v", err)
		}
	case options.QueryFacts:
		err := queryFacts(ctx, factStore, options)
		if err != nil {
			log.Fatalf("Error executing query: %v", err)
		}
	case options.Query:
		if err := query(ctx, factStore, options); err != nil {
			log.Fatalf("Error executing query: %v", err)
		}
	default:
		log.Fatalf("command not implemented")
	}
}

func fetchExternalIDsOfkIDs(ctx context.Context, store api.FactStoreClient, kids []uint64) map[uint64]string {
	extIDLookup := api.LookupSPRequest{
		Lookups: make([]api.LookupSPRequest_Item, 0, len(kids)),
	}
	for _, kid := range kids {
		lookup := api.LookupSPRequest_Item{Subject: kid, Predicate: facts.HasExternalID}
		extIDLookup.Lookups = append(extIDLookup.Lookups, lookup)
	}

	lookupResult, err := store.LookupSp(ctx, &extIDLookup)
	if err != nil {
		log.Fatalf("Unable to perform lookupSp: %v", err)
	}
	res := make(map[uint64]string, len(extIDLookup.Lookups))
	for i, r := range lookupResult.Results {
		for _, lr := range r.Item {
			if lr.Object.GetAString() != "" {
				res[extIDLookup.Lookups[i].Subject] = lr.Object.GetAString()
			}
		}
	}
	return res
}

func fetchExternalIDsOfFacts(ctx context.Context, store api.FactStoreClient, factList []api.ResolvedFact) map[uint64]string {
	kids := make([]uint64, 0, len(factList)*3)
	for _, f := range factList {
		kids = append(kids, f.Subject, f.Predicate)
		if f.Object.GetAKID() != 0 {
			kids = append(kids, f.Object.GetAKID())
		}
		if f.Object.GetUnitID() != 0 {
			kids = append(kids, f.Object.GetUnitID())
		}
		if f.Object.GetLangID() != 0 {
			kids = append(kids, f.Object.GetLangID())
		}
	}
	return fetchExternalIDsOfkIDs(ctx, store, kids)
}

func dumpFacts(ctx context.Context, store api.FactStoreClient, idx int64, facts []api.ResolvedFact, title string, opt *options) {
	if len(title) > 0 {
		fmt.Println(title)
	}
	fmt.Printf("As of Index: %d, Num Facts: %d\n", idx, len(facts))
	kidPrinter := kidPrinter{}
	if !opt.NoResolveExternalIDs {
		kidPrinter.kidToXID = fetchExternalIDsOfFacts(ctx, store, facts)
	}
	t := [][]string{
		{"Index", "Fact ID", "Subject", "Predicate", "Object", "Unit", "Language"},
	}
	for _, f := range facts {
		row := make([]string, len(t[0]))
		makeTableRow(0, row, &f, kidPrinter.format)
		t = append(t, row)
	}
	table.PrettyPrint(os.Stdout, t, table.HeaderRow|table.RightJustify)
}

func makeTableRow(offset int, tableRow []string, fact *api.ResolvedFact, formatKID func(uint64) string) {
	tableRow[offset+0] = fmtr.Sprintf("%d", fact.Index)
	tableRow[offset+1] = formatKID(fact.Id)
	tableRow[offset+2] = formatKID(fact.Subject)
	tableRow[offset+3] = formatKID(fact.Predicate)
	if fact.Object.GetAKID() != 0 {
		tableRow[offset+4] = formatKID(fact.Object.GetAKID())
	} else {
		tableRow[offset+4] = fact.Object.String()
	}
	if fact.Object.GetUnitID() != 0 {
		tableRow[offset+5] = formatKID(fact.Object.GetUnitID())
	} else {
		tableRow[offset+5] = ""
	}
	if fact.Object.GetLangID() != 0 {
		tableRow[offset+6] = formatKID(fact.Object.GetLangID())
	} else {
		tableRow[offset+6] = ""
	}
}

type kidPrinter struct {
	kidToXID map[uint64]string
}

// format a kid as a string if it exists within the map
func (k *kidPrinter) format(kid uint64) string {
	extID := k.kidToXID[kid]
	if len(extID) > 0 {
		// Do not wrap QName (qualified name) with <>
		if strings.Contains(extID, ":") {
			return fmtr.Sprintf("%d: %s", kid, extID)
		}
		return fmtr.Sprintf("%d: <%s>", kid, extID)
	}
	return fmtr.Sprintf("%d", kid)
}
