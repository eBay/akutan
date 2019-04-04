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

package fanout

import (
	"sort"

	"github.com/ebay/beam/space"
)

// Partition splits the given range into subranges, where each view serves one
// or more complete subranges. If there are insufficient views, some subranges
// will be served by no views.
//
// This function is useful when you need to gather results covering an entire
// range. To do so, split up the range using Partition, then pass the resulting
// PartitionedRange's StartPoints into Call. Call will collect results for each
// of the subranges. TODO: an example would help here.
func Partition(overallRange space.Range, views Views) space.PartitionedRange {
	boundaries := make([]space.Point, 0, 2*views.Len())
	boundaries = append(boundaries, overallRange.Start)
	for idx := 0; idx < views.Len(); idx++ {
		r := views.View(idx).Serves()
		if overallRange.Contains(r.Start) {
			boundaries = append(boundaries, r.Start)
		}
		if overallRange.Contains(r.End) {
			boundaries = append(boundaries, r.End)
		}
	}
	sort.Slice(boundaries, func(i, j int) bool {
		return boundaries[i].Less(boundaries[j])
	})
	unique := boundaries[:0]
	for _, hash := range boundaries {
		if len(unique) == 0 || hash != unique[len(unique)-1] {
			unique = append(unique, hash)
		}
	}
	return space.PartitionedRange{
		StartPoints: unique,
		End:         overallRange.End,
	}
}
