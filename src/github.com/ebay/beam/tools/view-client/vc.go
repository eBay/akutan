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

// Command view-client is command line tool for calling Beam views.
package main

import (
	"context"
	"os"
	"time"

	docopt "github.com/docopt/docopt-go"
	"github.com/ebay/beam/blog"
	_ "github.com/ebay/beam/blog/kafka"         // side-effect: registers "kafka" blog implementation
	_ "github.com/ebay/beam/blog/logspecclient" // side-effect: registers "logspec" blog implementation
	"github.com/ebay/beam/config"
	"github.com/ebay/beam/query/parser"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/debuglog"
	statsutil "github.com/ebay/beam/util/stats"
	"github.com/ebay/beam/viewclient"
	log "github.com/sirupsen/logrus"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var fmtr = message.NewPrinter(language.English)

const usage = `view-client is a command-line tool for calling Beam views.

Usage:
  view-client [--cfg=FILE -t=DUR] lookups [-i=NUM --noextids] SUBJECT
  view-client [--cfg=FILE -t=DUR] lookupsp [-i=NUM --noextids] SUBJECT PREDICATE
  view-client [--cfg=FILE -t=DUR] lookupspo [-i=NUM --langid=NUM --unitid=NUM --noextids] SUBJECT PREDICATE OBJECT
  view-client [--cfg=FILE -t=DUR] lookuppo [-i=NUM --langid=NUM --unitid=NUM --noextids] PREDICATE OBJECT
  view-client [--cfg=FILE -t=DUR] lookupcmp [-i=NUM --langid=NUM  --unitid=NUM --noextids] PREDICATE OPERATOR OBJECT [END_OBJECT]
  view-client [--cfg=FILE -t=DUR] infersp [-i=NUM --noextids] SUBJECT PREDICATE
  view-client [--cfg=FILE -t=DUR] inferspo [-i=NUM --langid=NUM --unitid=NUM --noextids] SUBJECT PREDICATE OBJECT
  view-client [--cfg=FILE -t=DUR] inferpo [-i=NUM --langid=NUM --unitid=NUM --noextids] PREDICATE OBJECT
  view-client [--cfg=FILE -t=DUR] ids [NAMES]
  view-client [--cfg=FILE -t=DUR] stats [--noextids]

Options:
  --cfg=FILE               JSON file describing cluster configuration [default: config.json]
  -i=NUM, --index=NUM      Log index to specify in request.
  -t=DUR, --timeout=DUR    Timeout for RPC calls [default: 10s]
  --langid=NUM             Language id of string literal, if specified
  --unitid=NUM             Unit id of literal, if specified
  --noextids               Don't resolve/print KID externalIDs in fact results.
  OPERATOR                 One of <, <=, =, >=, >, RangeIncInc, RangeIncExc, Prefix (Range operators require END_OBJECT)

Parsing:
  The subject, predicate and object are parsed by the query language.  Both subject and predicate are assumed to
  be ids and can be specified as either:
   - 695675026 or "#695675026"
  The object field allows for complex types, and is parsed in precedence order:
  - literal ids:     "#695675026"      (<IPhone_7>)
  - literal floats:  138.0             (<IPhone_7> <hasWeight>)
  - literal ints:    2313328           (<Vancouver> <hasNumberOfPeople>)
  - literal strings: '"IPhone_7"'      (<IPhone_7>)
  - literal times:   "'2016-09-16'"    (<IPhone_7> <wasCreatedOn>)
  For more complete documentation of the query language see: docs/query.md

Examples:
  view-client stats                                                         # returns a table of the subject-predicates, predicate-objects and their usage counts
  view-client ids IPhone_7,Vancouver,British_Columbia                       # returns subject ids based on the <hasExternalID> predicate
  view-client ids "IPhone_*"                                                # returns all subject ids based on the <hasExternalID> predicate
  view-client lookups 695675026                                             # <IPhone_7>
  view-client lookups "#695675026"                                          # <IPhone_7>
  view-client lookupsp 695675026 470627000                                  # <IPhone_7> skos:prefLabel
  view-client lookupsp 10085 88421000                                       # <Vancouver> <isLocatedIn>
  view-client lookupsp 10085 728561000                                      # <Vancouver> <hasNumberOfPeople>
  view-client lookupspo --langid 470629000 695675026 470627000 '"IPhone 7"' # <IPhone_7> skos:prefLabel <en>
  view-client lookupspo --unitid 4139119000 695675026 727820000 138.0       # <IPhone_7> <hasWeight> 136.0 <g>
  view-client lookuppo --unitid 1141829000 728940000 49.5                   # <hasLatitude> 49.25
  view-client lookuppo --unitid 809287000 728667000 "'2016-09-16'"          # <wasCreatedOnDate> <Sept 16, 2016> <xsd:date>
  view-client lookuppo --unitid 4045560000 728561000 2313328                # <hasNumberOfPeople> 2313328
  view-client lookupcmp --unitid 4139119000 727820000 "<" 150.0             # <hasWeight> < 150.0 <g>
  view-client infersp 10085 88421000                                        # <Vancouver> <isLocatedIn>
  view-client inferspo 10085 88421000 "#110271076"                          # <Vancouver> <isLocatedIn> <Earth>
  view-client inferpo 88421000 "#2287057"                                   # <isLocatedIn> <British_Columbia>
`

type options struct {
	// Options
	ConfigFile           string `docopt:"--cfg"`
	Index                blog.Index
	Timeout              time.Duration
	TimeoutString        string `docopt:"--timeout"`
	LangID               int64  `docopt:"--langid"`
	UnitID               int64  `docopt:"--unitid"`
	NoResolveExternalIDs bool   `docopt:"--noextids"`

	// Lookup{SP,SPO,PO}
	LookupS     bool `docopt:"lookups"`
	LookupSP    bool `docopt:"lookupsp"`
	LookupSPO   bool `docopt:"lookupspo"`
	InferSPO    bool `docopt:"inferspo"`
	LookupPO    bool `docopt:"lookuppo"`
	LookupPOCmp bool `docopt:"lookupcmp"`
	InferPO     bool `docopt:"inferpo"`
	InferSP     bool `docopt:"infersp"`

	Subject         parser.LiteralID
	SubjectString   string `docopt:"SUBJECT"`
	Predicate       parser.LiteralID
	PredicateString string `docopt:"PREDICATE"`
	Object          rpc.KGObject
	ObjectString    string `docopt:"OBJECT"`
	OperatorString  string `docopt:"OPERATOR"`
	Operator        parser.Operator
	EndObject       rpc.KGObject
	EndObjectString string `docopt:"END_OBJECT"`

	// Ids
	IDs        bool   `docopt:"ids"`
	NameString string `docopt:"NAMES"`

	// Stats
	Stats bool `docopt:"stats"`
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
	options.Subject, err = parseLiteralID(options.SubjectString)
	if err != nil {
		log.Fatalf("Unable to parse subject value: %v", err)
	}
	options.Predicate, err = parseLiteralID(options.PredicateString)
	if err != nil {
		log.Fatalf("Unable to parse predicate value: %v", err)
	}
	if options.OperatorString != "" {
		options.Operator, err = parseOperator(options.OperatorString)
		if err != nil {
			log.Fatalf("Unable to parse operator value: %v", err)
		}
	}
	langID := uint64(options.LangID)
	unitID := uint64(options.UnitID)
	object, err := parseLiteralObject(options.ObjectString, langID, unitID)
	if err != nil {
		log.Fatalf("Unable to parse object value: %v", err)
	}
	options.Object = rpc.KGObjectFromAPI(object)
	if options.EndObjectString != "" {
		endObject, err := parseLiteralObject(options.EndObjectString, langID, unitID)
		if err != nil {
			log.Fatalf("Unable to parse end object value: %v", err)
		}
		options.EndObject = rpc.KGObjectFromAPI(endObject)
	}
	log.Infof("Options: %+v", options)
	return &options
}

func main() {
	debuglog.Configure(debuglog.Options{})
	options := parseArgs()
	cfg, err := config.Load(options.ConfigFile)
	if err != nil {
		log.Fatalf("Unable to read config file: %v", err)
	}
	client, err := viewclient.New(cfg)
	if err != nil {
		log.Fatalf("Unable to create viewclient: %v", err)
	}

	// Wait until the view registry is ready. For now, we just wait until it
	// hasn't changed for 5s.
	//
	// TODO: Wait until a particular condition is met, like having enough HashPO
	// and HashSP coverage.
	log.Info("Waiting until view registry is ready")
	// It's safe to ignore the returned error: the only error from
	// WaitForStable() is ctx.Err(), and context.Background() won't ever expire.
	_ = client.Registry.WaitForStable(context.Background(), 5*time.Second)
	summary := client.Registry.Summary()
	log.WithFields(summary).Info("Assuming view registry is ready")

	ctx := context.Background()
	if options.Timeout != 0 {
		newCtx, cancelFunc := context.WithTimeout(ctx, options.Timeout)
		defer cancelFunc()
		ctx = newCtx
	}
	if options.Index == 0 {
		log.Printf("Fetching last index from Beam log")
		beamLog, err := blog.New(ctx, cfg)
		if err != nil {
			log.Fatalf("Error from NewBeamLog: %v", err)
		}
		info, err := beamLog.Info(ctx)
		if err != nil {
			log.Fatalf("Error fetching log info: %v", err)
		}
		options.Index = info.LastIndex
		log.Printf("Using log index %v", options.Index)
	}
	switch {
	case options.LookupS:
		err = lookupS(ctx, client, options)
		if err != nil {
			log.Fatalf("Error executing LookupS: %v", err)
		}
	case options.LookupSP:
		err = lookupSP(ctx, client, options)
		if err != nil {
			log.Fatalf("Error executing LookupSP: %v", err)
		}
	case options.LookupSPO:
		err = lookupSPO(ctx, client, options)
		if err != nil {
			log.Fatalf("Error executing LookupSPO: %v", err)
		}
	case options.LookupPO:
		err = lookupPO(ctx, client, options)
		if err != nil {
			log.Fatalf("Error executing LookupPO: %v", err)
		}
	case options.LookupPOCmp:
		if err = lookupPOCmp(ctx, client, options); err != nil {
			log.Fatalf("Error executing LookupPOCmp: %v", err)
		}
	case options.InferSP:
		if err := inferSP(ctx, client, options); err != nil {
			log.Fatalf("Error executing InferSP: %v", err)
		}
	case options.InferSPO:
		if err = inferSPO(ctx, client, options); err != nil {
			log.Fatalf("Error executing LookupSPOInference: %v", err)
		}
	case options.InferPO:
		if err := inferPO(ctx, client, options); err != nil {
			log.Fatalf("Error executing InferPO: %v", err)
		}
	case options.IDs:
		if err := lookupIDs(ctx, client, options); err != nil {
			log.Fatalf("Error executing LookupPOCmp: %v", err)
		}
	case options.Stats:
		if err = showStats(ctx, client, options); err != nil {
			log.Fatalf("Error executing stats: %v", err)
		}
	default:
		log.Fatalf("command not implemented")
	}
}

func showStats(ctx context.Context, client *viewclient.Client, options *options) error {
	log.Printf("Requesting stats")
	stats, err := client.FactStats(ctx, new(rpc.FactStatsRequest))
	if err != nil {
		return err
	}
	log.Printf("Found %v total facts", stats.NumFacts())
	return statsutil.PrettyPrint(ctx, os.Stdout, stats.ToRPCFactStats(), client, options.Index, !options.NoResolveExternalIDs)
}
