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

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ebay/akutan/msg/facts"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/parallel"
	"github.com/ebay/akutan/util/table"
	"github.com/ebay/akutan/viewclient"
	log "github.com/sirupsen/logrus"
)

func goDumpFacts(ctx context.Context, client *viewclient.Client, chunks chan *rpc.LookupChunk, options *options) func() {
	totalResults := 0
	add := func(facts []rpc.Fact, c *rpc.LookupChunk) []rpc.Fact {
		if facts == nil {
			facts = make([]rpc.Fact, 0, len(c.Facts))
		}
		for _, cf := range c.Facts {
			facts = append(facts, cf.Fact)
		}
		totalResults += len(c.Facts)
		return facts
	}
	return parallel.Go(func() {
		var facts []rpc.Fact
		for chunk := range chunks {
			facts = add(facts, chunk)
		outer:
			for {
				select {
				// accumulate any more chunks that are ready
				case more, open := <-chunks:
					if open {
						facts = add(facts, more)
					} else {
						break outer
					}
				default:
					break outer
				}
			}
			dumpFacts(ctx, client, facts, options)
			facts = facts[:0]
		}
		fmt.Printf("Total # of Facts: %d\n", totalResults)
	})
}

func dumpFacts(ctx context.Context, client *viewclient.Client, facts []rpc.Fact, options *options) {
	var idToExternalID map[uint64]string
	if !options.NoResolveExternalIDs {
		idToExternalID = fetchExternalIDsOfFacts(ctx, client, facts, options)
	}
	t := [][]string{
		{"Index", "Fact ID", "Subject", "Predicate", "Object", "Unit", "Language"},
	}
	formatKID := func(kid uint64) string {
		extID := idToExternalID[kid]
		if len(extID) > 0 {
			return fmtr.Sprintf("%d: <%s>", kid, extID)
		}
		return fmtr.Sprintf("%d", kid)
	}
	for _, f := range facts {
		row := []string{
			fmtr.Sprintf("%d", f.Index),
			formatKID(f.Id),
			formatKID(f.Subject),
			formatKID(f.Predicate),
		}
		if f.Object.ValKID() != 0 {
			row = append(row, formatKID(f.Object.ValKID()))
		} else {
			row = append(row, f.Object.String())
		}
		if f.Object.UnitID() != 0 {
			row = append(row, formatKID(f.Object.UnitID()))
		} else {
			row = append(row, "")
		}
		if f.Object.LangID() != 0 {
			row = append(row, formatKID(f.Object.LangID()))
		} else {
			row = append(row, "")
		}
		t = append(t, row)
	}
	table.PrettyPrint(os.Stdout, t, table.HeaderRow|table.RightJustify)
}

func fetchExternalIDsOfFacts(ctx context.Context, client *viewclient.Client, factList []rpc.Fact, options *options) map[uint64]string {
	kids := make([]uint64, 0, len(factList)*3)
	for _, f := range factList {
		kids = append(kids, f.Subject, f.Predicate)
		if f.Object.ValKID() != 0 {
			kids = append(kids, f.Object.ValKID())
		}
		if f.Object.UnitID() != 0 {
			kids = append(kids, f.Object.UnitID())
		}
		if f.Object.LangID() != 0 {
			kids = append(kids, f.Object.LangID())
		}
	}
	return fetchExternalIDsOfkIDs(ctx, client, kids, options)
}

func fetchExternalIDsOfkIDs(ctx context.Context, client *viewclient.Client, kids []uint64, options *options) map[uint64]string {
	extIDLookup := rpc.LookupSPRequest{
		Index:   options.Index,
		Lookups: make([]rpc.LookupSPRequest_Item, 0, len(kids)),
	}
	for _, kid := range kids {
		lookup := rpc.LookupSPRequest_Item{
			Subject:   kid,
			Predicate: facts.HasExternalID,
		}
		extIDLookup.Lookups = append(extIDLookup.Lookups, lookup)
	}
	resCh := make(chan *rpc.LookupChunk, 4)
	res := make(map[uint64]string, len(extIDLookup.Lookups))
	wait := parallel.Go(func() {
		for chunk := range resCh {
			for _, fact := range chunk.Facts {
				if fact.Fact.Object.ValString() != "" {
					kid := extIDLookup.Lookups[fact.Lookup].Subject
					res[kid] = fact.Fact.Object.ValString()
				}
			}
		}
	})
	err := client.LookupSP(ctx, &extIDLookup, resCh)
	wait()
	if err != nil {
		log.Warnf("Unable to perform lookupSP: %v", err)
	}
	return res
}
