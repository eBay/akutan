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

// Command bc provides command line access to the beam GRPC API
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	docopt "github.com/docopt/docopt-go"
	"github.com/ebay/beam/api"
	"github.com/ebay/beam/config"
	"github.com/ebay/beam/msg/facts"
	"github.com/ebay/beam/query/parser"
	"github.com/ebay/beam/util/debuglog"
	grpcclientutil "github.com/ebay/beam/util/grpc/client"
	"github.com/ebay/beam/util/table"
	"github.com/ebay/beam/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var fmtr = message.NewPrinter(language.English)

const usage = `beam-client is a command-line tool for calling the Beam API service.

Usage:
  beam-client [--api=HOST -t=DUR --trace=HOST] query [-i=NUM] FILE
  beam-client [--api=HOST -t=DUR --trace=HOST --format=FORMAT] insert FILE
  beam-client [--api=HOST -t=DUR --trace=HOST] insertfact [--new=VAR] SUBJECT PREDICATE OBJECT
  beam-client [--api=HOST -t=DUR --trace=HOST] queryfacts [-i=NUM] [--noextids] [QUERYSTRING]
  beam-client [--api=HOST -t=DUR --trace=HOST] lookupsp [-i=NUM] SUBJECT PREDICATE
  beam-client [--api=HOST -t=DUR --trace=HOST] lookuppo [-i=NUM] PREDICATE OBJECT
  beam-client [--api=HOST -t=DUR --trace=HOST] wipe [-w=WAITFOR]

Options:
  --api=HOST                        Host and port of Beam API server to connect to [default: localhost:9987]
  -t=DUR, --timeout=DUR             Timeout for RPC calls to Beam API tier [default: 10s]
  --new=VAR                         Create a new entity to reference in the inserted fact.
  -i=NUM, --index=NUM               Log index to specify in request.
  -w=WAITFOR, --waitfor=WAITFOR     Duration to wait for wipe to complete.
  --noextids                        Don't resolve/print KID externalIDs in fact results.
  --trace=HOST                      Send OpenTracing traces to this collector.
  --format=FORMAT                   File format for inserting facts [default: tsv]

Examples:
  # To create a fact '<car1> <fits> <part1>', first create KIDs for 'car1', 'fits' and 'part1'
  # by using HasExternalID(#4) predicate, and then create a fact with the KIDs.
  beam-client insertfact --new=c '?c' '#4' '"car1"'  # returned VarResults: 1430001
  beam-client insertfact --new=f '?f' '#4' '"fits"'  # returned VarResults: 1454001
  beam-client insertfact --new=p '?p' '#4' '"part1"' # returned VarResults: 1487001
  beam-client insertfact '#1430001' '#1454001' '#1487001'

  # Multiple line insert fact usage.
  beam-client insert - <<EOF  
  <car1>  <fits> 	  <part1>
  <part1> rdf:type  motors:part
EOF

  # Query all cars that accept part1.
  beam-client queryfacts "?c <fits> <part1>"

  # Query all parts that fit car1.
  beam-client queryfacts "<car1> <fits> ?p"

  # Multiple line query usage to get all parts that fit car1.
  beam-client query - <<EOF
  SELECT ?p WHERE {
  <car1> <fits> ?p
  ?p rdf:type product:part
  }
EOF
  
  # Lookup facts by subject and predicate.
  beam-client lookupsp 1430001 1454001
  # Lookup facts by predicate and object at the Log index.
  beam-client lookuppo --index 99552 1454001 99452001

  # Wipe all existing data from the database.
  beam-client wipe

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

	// InsertFact
	InsertFact      bool   `docopt:"insertfact"`
	LangID          uint64 `docopt:"--langid"`
	UnitID          uint64 `docopt:"--unitid"`
	NewSubjectVar   string `docopt:"--new"`
	InsertSubject   api.KIDOrVar
	InsertPredicate api.KIDOrVar
	InsertObject    api.KGObjectOrVar

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
	if options.InsertFact {
		options.InsertSubject, err = parseKIDOrVar(options.SubjectString)
	} else {
		options.Subject, err = parseLiteralID(options.SubjectString)
	}
	if err != nil {
		log.Fatalf("Unable to parse subject value: %v", err)
	}
	if options.InsertFact {
		options.InsertPredicate, err = parseKIDOrVar(options.PredicateString)
	} else {
		options.Predicate, err = parseLiteralID(options.PredicateString)
	}
	if err != nil {
		log.Fatalf("Unable to parse predicate value: %v", err)
	}
	if options.InsertFact {
		options.InsertObject, err = parseKGObjectOrVar(options.ObjectString)
	}
	if err != nil {
		log.Fatalf("Unable to parse object value: %v", err)
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

// parseKIDOrVar parses the query term and returns the api object or variable for 'insert'
func parseKIDOrVar(in string) (api.KIDOrVar, error) {
	term, err := parser.ParseTerm(in)
	if err != nil {
		return api.KIDOrVar{}, err
	}
	if variable, ok := term.(*parser.Variable); ok {
		return api.KIDOrVar{Value: &api.KIDOrVar_Var{Var: variable.Name}}, nil
	}
	if literalID, ok := term.(*parser.LiteralID); ok {
		return api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: literalID.Value}}, nil
	}
	if literalInt, ok := term.(*parser.LiteralInt); ok {
		if literalInt.Value > 0 {
			return api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: uint64(literalInt.Value)}}, nil
		}
	}
	return api.KIDOrVar{}, fmt.Errorf("invalid kid or var: '%s' %T", in, term)
}

// parseKGObjectOrVar parses the query term and returns the api object or variable for 'insert'
func parseKGObjectOrVar(in string) (api.KGObjectOrVar, error) {
	term, err := parser.ParseTerm(in)
	if err != nil {
		return api.KGObjectOrVar{}, err
	}
	if variable, ok := term.(*parser.Variable); ok {
		return api.KGObjectOrVar{Value: &api.KGObjectOrVar_Var{Var: variable.Name}}, nil
	}
	// TODO Once language and units semantics are finalized and implemented,
	// need to make it available at the client side.
	if literal, ok := term.(parser.Literal); ok {
		obj := literal.Literal()
		return api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{Object: obj}}, nil
	}
	return api.KGObjectOrVar{}, fmt.Errorf("invalid kgobject or var: '%s'", in)
}

func main() {
	debuglog.Configure(debuglog.Options{})
	options := parseArgs()
	ctx := context.Background()

	if options.TracingCollector != "" {
		tracer, err := tracing.New("beam-client", &config.Tracing{
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
	span, ctx := opentracing.StartSpanFromContext(ctx, "beam-client run")
	defer span.Finish()

	conn := grpcclientutil.InsecureDialContext(ctx, options.Server)

	factStore := api.NewBeamFactStoreClient(conn)
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
	case options.InsertFact:
		err := insertFact(timeoutCtx, factStore, options)
		if err != nil {
			log.Fatalf("Error executing InsertFact: %v", err)
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

func fetchExternalIDsOfkIDs(ctx context.Context, store api.BeamFactStoreClient, kids []uint64) map[uint64]string {
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

func fetchExternalIDsOfFacts(ctx context.Context, store api.BeamFactStoreClient, factList []api.ResolvedFact) map[uint64]string {
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

func dumpFacts(ctx context.Context, store api.BeamFactStoreClient, idx int64, facts []api.ResolvedFact, title string, opt *options) {
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

// parseLiteralID parses an input string into a KID or returns an error.
func parseLiteralID(in string) (parser.LiteralID, error) {
	if in == "" {
		return parser.LiteralID{}, nil
	}
	literal, err := parser.ParseLiteral(in)
	if err != nil {
		return parser.LiteralID{}, err
	}
	if lint, ok := literal.(*parser.LiteralInt); ok {
		if lint.Value > 0 {
			return parser.LiteralID{Value: uint64(lint.Value)}, nil
		}
		return parser.LiteralID{}, fmt.Errorf("unable to parse literal id: '%s'", in)
	}
	if lid, ok := literal.(*parser.LiteralID); ok {
		return *lid, nil
	}
	return parser.LiteralID{}, fmt.Errorf("invalid literal id: '%s'", in)
}
