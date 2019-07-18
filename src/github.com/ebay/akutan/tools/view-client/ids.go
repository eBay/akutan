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
	"sort"
	"strings"

	"github.com/ebay/akutan/msg/facts"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/parallel"
	"github.com/ebay/akutan/viewclient"
)

func lookupIDs(ctx context.Context, view *viewclient.Client, options *options) error {
	// Create bulk LookupPOCmp request.
	req := &rpc.LookupPOCmpRequest{
		Index: options.Index,
	}
	options.NameString = strings.TrimSpace(options.NameString)
	if len(options.NameString) == 0 {
		req.Lookups = []rpc.LookupPOCmpRequest_Item{{
			Predicate: facts.HasExternalID,
			Operator:  rpc.OpPrefix,
			Object:    rpc.AString("", 0),
		}}
	} else {
		parts := strings.Split(options.NameString, ",")
		req.Lookups = make([]rpc.LookupPOCmpRequest_Item, 0, len(parts))
		for i := range parts {
			name := strings.TrimSpace(parts[i])
			var lookup rpc.LookupPOCmpRequest_Item
			if strings.HasSuffix(name, "*") {
				lookup = rpc.LookupPOCmpRequest_Item{
					Predicate: facts.HasExternalID,
					Operator:  rpc.OpPrefix,
					Object:    rpc.AString(name[:len(name)-1], 0),
				}
			} else {
				lookup = rpc.LookupPOCmpRequest_Item{
					Predicate: facts.HasExternalID,
					Operator:  rpc.OpEqual,
					Object:    rpc.AString(name, 0),
				}
			}
			req.Lookups = append(req.Lookups, lookup)
		}
	}
	// Issue request and buffer results.
	resCh := make(chan *rpc.LookupChunk, 4)
	var facts []rpc.LookupChunk_Fact
	wait := parallel.Go(func() {
		for chunk := range resCh {
			facts = append(facts, chunk.Facts...)
		}
	})
	err := view.LookupPOCmp(ctx, req, resCh)
	wait()
	if err != nil {
		return err
	}
	// Sort and print results.
	sort.Slice(facts, func(i, j int) bool {
		return strings.Compare(
			facts[i].Fact.Object.ValString(),
			facts[j].Fact.Object.ValString()) < 0
	})
	for _, f := range facts {
		fmt.Printf("%20d : %s\n", f.Fact.Subject, f.Fact.Object)
	}
	return nil
}
