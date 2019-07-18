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
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/ebay/akutan/api"
	"github.com/ebay/akutan/util/table"
	log "github.com/sirupsen/logrus"
)

func query(ctx context.Context, store api.FactStoreClient, options *options) error {
	query, err := readFile(options.Filename)
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(ctx, options.Timeout)
	defer cancelFunc()
	req := api.QueryRequest{
		Query: query,
	}
	if options.Index == 0 {
		req.Index = api.LogIndex{
			Constraint: api.LogIndexLatest,
		}
	} else {
		req.Index = api.LogIndex{
			Index:      uint64(options.Index),
			Constraint: api.LogIndexExact,
		}
	}
	start := time.Now()
	stream, err := store.Query(ctx, &req)
	if err != nil {
		return err
	}
	var count uint64
	var totalResultSize uint64
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			if totalResultSize > 0 {
				fmtr.Printf("\n%d of %d results.\n", count, totalResultSize)
			} else {
				fmtr.Printf("\n%d results.\n", count)
			}
			log.Infof("QueryFacts took %s", time.Since(start))
			return nil
		}
		if err != nil {
			return err
		}
		if count > 0 {
			fmt.Print("\n")
		}
		dumpQueryResults(res)
		count += uint64(len(res.Columns[0].Cells))
		totalResultSize = res.TotalResultSize
	}
}

func dumpQueryResults(res *api.QueryResult) {
	// Convert the column-oriented result chunk to a slice of rows. The first row is a header.
	t := make([][]string, len(res.Columns[0].Cells)+1)
	t[0] = make([]string, len(res.Columns))
	for colIdx, c := range res.Columns {
		t[0][colIdx] = c.Name
		for rowIdx, v := range c.Cells {
			if colIdx == 0 {
				t[rowIdx+1] = make([]string, len(res.Columns))
			}
			t[rowIdx+1][colIdx] = v.String()
		}
	}
	table.PrettyPrint(os.Stdout, t, table.HeaderRow)
}

func queryFacts(ctx context.Context, store api.FactStoreClient, options *options) error {
	query := options.QueryString
	if "-" == query {
		all, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		query = string(all)
	}
	ctx, cancelFunc := context.WithTimeout(ctx, options.Timeout)
	defer cancelFunc()
	req := api.QueryFactsRequest{
		Index: options.Index,
		Query: query,
	}
	start := time.Now()
	stream, err := store.QueryFacts(ctx, &req)
	if err != nil {
		return err
	}
	var count int64
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			fmtr.Printf("\n%d facts.\n", count)
			log.Infof("QueryFacts took %s", time.Since(start))
			return nil
		}
		if err != nil {
			return err
		}
		for _, results := range res.Results {
			if count > 0 {
				fmt.Print("\n")
			}
			dumpFacts(ctx, store, req.Index, results.Facts, "", options)
			count += int64(len(results.Facts))
		}
	}
}
