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

// Package stats contains a pretty-printer for statistics about the facts stored on DiskViews.
package stats

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/ebay/akutan/msg/facts"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/parallel"
	"github.com/ebay/akutan/util/table"
	"github.com/ebay/akutan/viewclient/lookups"
	opentracing "github.com/opentracing/opentracing-go"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var fmtr = message.NewPrinter(language.English)

// PrettyPrint will collect fact stats from the views and write a pretty printed version of them
// to the supplied writer
func PrettyPrint(ctx context.Context, w io.Writer, res *rpc.FactStatsResult, vc lookups.SP, index uint64, withExternalIDs bool) error {
	id := func(s uint64) string {
		return fmt.Sprintf("%d", s)
	}
	if withExternalIDs {
		extIds, err := fetchExternalIds(ctx, vc, index, res)
		if err != nil {
			return fmt.Errorf("unable to resolve external IDs: %v", err)
		}
		id = func(s uint64) string {
			extID := extIds[s]
			if extID == "" {
				return fmt.Sprintf("%d", s)
			}
			return fmt.Sprintf("%d: <%s>", s, extID)
		}
	}

	span, _ := opentracing.StartSpanFromContext(ctx, "sort")
	SortStats(res)
	span.Finish()

	span, _ = opentracing.StartSpanFromContext(ctx, "write result")
	defer span.Finish()
	bw := bufio.NewWriter(w)
	defer bw.Flush()
	count := func(c uint64) string {
		return fmtr.Sprintf("%d", c)
	}

	t := [][]string{{"Subject", "Count"}}
	for _, s := range res.Subjects {
		t = append(t, []string{id(s.Subject), count(s.Count)})
	}
	table.PrettyPrint(bw, t, table.HeaderRow|table.SkipEmpty|table.RightJustify)
	bw.WriteRune('\n')

	t = [][]string{{"Subject", "Predicate", "Count"}}
	for _, s := range res.SubjectPredicates {
		t = append(t, []string{id(s.Subject), id(s.Predicate), count(s.Count)})
	}
	table.PrettyPrint(bw, t, table.HeaderRow|table.SkipEmpty|table.RightJustify)
	bw.WriteRune('\n')

	t = [][]string{{"Predicate", "Count"}}
	for _, s := range res.Predicates {
		t = append(t, []string{id(s.Predicate), count(s.Count)})
	}
	table.PrettyPrint(bw, t, table.HeaderRow|table.SkipEmpty|table.RightJustify)
	bw.WriteRune('\n')

	t = [][]string{{"Predicate", "Object", "Count"}}
	for _, s := range res.PredicateObjects {
		t = append(t, []string{id(s.Predicate), s.Object.String(), count(s.Count)})
	}
	table.PrettyPrint(bw, t, table.HeaderRow|table.SkipEmpty|table.RightJustify)
	return nil
}

// SortStats sorts the frequencies in the given stats in descending order by count.
func SortStats(stats *rpc.FactStatsResult) {
	sort.Slice(stats.Subjects, func(a, b int) bool {
		return stats.Subjects[a].Count > stats.Subjects[b].Count
	})
	sort.Slice(stats.SubjectPredicates, func(a, b int) bool {
		return stats.SubjectPredicates[a].Count > stats.SubjectPredicates[b].Count
	})
	sort.Slice(stats.Predicates, func(a, b int) bool {
		return stats.Predicates[a].Count > stats.Predicates[b].Count
	})
	sort.Slice(stats.PredicateObjects, func(a, b int) bool {
		return stats.PredicateObjects[a].Count > stats.PredicateObjects[b].Count
	})
}

func fetchExternalIds(ctx context.Context, vc lookups.SP, index uint64, stats *rpc.FactStatsResult) (map[uint64]string, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "fetchExternalIds")
	defer span.Finish()
	// collect up the set of subject, predicate Ids
	labels := make(map[uint64]string)
	for _, s := range stats.Subjects {
		labels[s.Subject] = ""
	}
	for _, s := range stats.SubjectPredicates {
		labels[s.Subject] = ""
		labels[s.Predicate] = ""
	}
	for _, s := range stats.Predicates {
		labels[s.Predicate] = ""
	}
	for _, s := range stats.PredicateObjects {
		labels[s.Predicate] = ""
		if s.Object.ValueType() == rpc.KtKID {
			labels[s.Object.ValKID()] = ""
		}
	}
	if len(labels) == 0 {
		return labels, nil
	}

	lookups := make([]rpc.LookupSPRequest_Item, 0, len(labels))
	for subject := range labels {
		lookups = append(lookups, rpc.LookupSPRequest_Item{
			Subject:   subject,
			Predicate: facts.HasExternalID,
		})
	}
	resCh := make(chan *rpc.LookupChunk, 4)
	wait := parallel.Go(func() {
		for chunk := range resCh {
			for _, fact := range chunk.Facts {
				labels[fact.Fact.Subject] = fact.Fact.Object.ValString()
			}
		}
	})
	err := vc.LookupSP(ctx, &rpc.LookupSPRequest{Index: index, Lookups: lookups}, resCh)
	wait()
	if err != nil {
		return nil, err
	}
	return labels, nil
}
