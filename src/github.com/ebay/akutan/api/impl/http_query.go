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

package impl

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/ebay/akutan/api"
	"github.com/ebay/akutan/msg/facts"
	"github.com/ebay/akutan/query/planner"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/space"
	"github.com/ebay/akutan/util/web"
	"github.com/ebay/akutan/viewclient"
	"github.com/ebay/akutan/viewclient/fanout"
	"github.com/julienschmidt/httprouter"
	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
)

type clusterStats Server

// NumSPOPartitions Implements planner.Stats.NumSPOPartitions().
func (s *clusterStats) NumSPOPartitions() int {
	views := s.source.ReadFactsSPViews()
	p := fanout.Partition(space.Range{Start: space.Hash64(0), End: space.Infinity}, views)
	return len(p.StartPoints)
}

// NumPOSPartitions Implements planner.Stats.NumPOSPartitions().
func (s *clusterStats) NumPOSPartitions() int {
	views := s.source.ReadFactsPOViews()
	p := fanout.Partition(space.Range{Start: space.Hash64(0), End: space.Infinity}, views)
	return len(p.StartPoints)
}

// getStats implements kgstats.FetchStats.
func (s *Server) getStats(ctx context.Context) (planner.Stats, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "factStats")
	defer span.Finish()
	factStats, err := s.source.FactStats(ctx, new(rpc.FactStatsRequest))
	if err != nil {
		return nil, err
	}
	// factStats + clusterStats together implement planner.Stats
	var stats planner.Stats = &struct {
		*viewclient.FactStats
		*clusterStats
	}{
		factStats,
		(*clusterStats)(s),
	}
	log.Printf("stats: %v SPO partitions, %v POS partitions, %v facts",
		stats.NumSPOPartitions(),
		stats.NumPOSPartitions(),
		stats.NumFacts())
	return stats, nil
}

func (s *Server) getIndex(r *http.Request) (int64, error) {
	index := int64(0)
	if r.Form.Get("index") != "" {
		idx, err := strconv.ParseInt(r.Form.Get("index"), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("unable to parse index: %v", err)
		}
		index = idx
	}
	return index, nil
}

// Structure to hold the JSON response for HTTP queries.
type queryResponse struct {
	Error       string       `json:"error"`
	QueryString string       `json:"query"`
	NumFacts    int64        `json:"numFacts"`
	Facts       [][][]string `json:"facts"`
}

// Querying the KG via HTTP comes through here.
//
// This isn't a supported way of  calling the KG: use the gRPC interface instead.
// For the purpose of demos and the like, we allow calling via HTTP for now.
//
// Deprecated: Users should call the KG via gRPC. This API is subject to change
// or removal at any time.
func (s *Server) queryHTTP(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	querySpan, ctx := opentracing.StartSpanFromContext(r.Context(), "query")
	defer querySpan.Finish()

	resp := queryResponse{}
	status := http.StatusOK
	// Always write out JSON, even for errors.
	defer func() {
		w.WriteHeader(status)
		web.Write(w, resp)
	}()

	if err := r.ParseForm(); err != nil {
		resp.Error = fmt.Sprintf("Unable to parse POST data: %v", err)
		status = http.StatusBadRequest
		return
	}
	index, err := s.getIndex(r)
	if err != nil {
		resp.Error = fmt.Sprintf("Unable to get index: %v", err)
		status = http.StatusInternalServerError
		return
	}

	resp.QueryString = r.Form.Get("q")
	log.Debugf("query string:\n%s", resp.QueryString)
	resolveKids := r.Form.Get("resolve") != "false"

	kidToXid := make(map[uint64]string)
	kidString := func(kid uint64) string {
		return fmt.Sprintf("#%d", kid)
	}
	entString := func(kid uint64) string {
		if xid := kidToXid[kid]; xid != "" {
			return fmt.Sprintf("<%s>", xid)
		}
		return kidString(kid)
	}
	objString := func(o api.KGObject) string {
		switch o.Value.(type) {
		case *api.KGObject_AKID:
			return entString(o.GetAKID())
		default:
			return o.String()
		}
	}
	factString := func(f *api.ResolvedFact) []string {
		return []string{kidString(f.Id), entString(f.Subject), entString(f.Predicate), objString(f.Object)}
	}
	facts := make([][]api.ResolvedFact, 0, 10)
	receive := func(chunk *api.QueryFactsResult) error {
		for _, results := range chunk.Results {
			numFacts := int64(len(results.Facts))
			if numFacts == 0 {
				continue
			}
			facts = append(facts, results.Facts)
			for _, f := range results.Facts {
				kidToXid[f.Subject] = ""
				kidToXid[f.Predicate] = ""
				if f.Object.GetAKID() != 0 {
					kidToXid[f.Object.GetAKID()] = ""
				}
				if f.Object.GetUnitID() != 0 {
					kidToXid[f.Object.GetUnitID()] = ""
				}
				if f.Object.GetLangID() != 0 {
					kidToXid[f.Object.GetLangID()] = ""
				}
			}
			resp.NumFacts += numFacts
		}
		return nil
	}
	req := api.QueryFactsRequest{
		Index: index,
		Query: resp.QueryString,
	}
	err = s.queryFactsImpl(ctx, &req, receive)
	if err != nil {
		resp.Error = fmt.Sprintf("Error during query: %v", err)
		status = http.StatusInternalServerError
	}
	if resolveKids {
		s.fetchExternalIDsOfkIDs(ctx, kidToXid)
	}
	for i, factSet := range facts {
		foundFacts := make([][]string, 0, i)
		for _, f := range factSet {
			foundFacts = append(foundFacts, factString(&f))
		}
		resp.Facts = append(resp.Facts, foundFacts)
	}
}

func (s *Server) fetchExternalIDsOfkIDs(ctx context.Context, names map[uint64]string) {
	extIDLookup := api.LookupSPRequest{
		Lookups: make([]api.LookupSPRequest_Item, 0, len(names)),
	}
	for kid := range names {
		lookup := api.LookupSPRequest_Item{Subject: kid, Predicate: facts.HasExternalID}
		extIDLookup.Lookups = append(extIDLookup.Lookups, lookup)
	}

	lookupResult, err := s.LookupSp(ctx, &extIDLookup)
	if err != nil {
		log.Fatalf("Unable to perform lookupSp: %v", err)
	}
	for i, r := range lookupResult.Results {
		for _, lr := range r.Item {
			if lr.Object.GetAString() != "" {
				names[extIDLookup.Lookups[i].Subject] = lr.Object.GetAString()
			}
		}
	}
}
