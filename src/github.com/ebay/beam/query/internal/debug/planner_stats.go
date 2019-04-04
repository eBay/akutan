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

package debug

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/ebay/beam/query/planner"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/bytes"
	"github.com/ebay/beam/util/cmp"
)

// plannerStats tracks which statistic the planner asked for during the planning
// phase.
type plannerStats struct {
	// The real planner.Stats provider.
	impl planner.Stats
	// A map of KIDs -> ExternalIDs, used to better show the statistics used.
	kids map[uint64]string
	// The set of stats requested.
	used []statItem
}

type statItem struct {
	// A formatted string describing the stat that was fetched.
	description string
	// The returned value for this statistic.
	value int
}

// fmtKID returns a formatted string for the supplied KID. It will use the
// externalID representation if we have it, otherwise it uses the standard
// literal ID format.
func (s *plannerStats) fmtKID(k uint64) string {
	r := s.kids[k]
	if r != "" {
		return r
	}
	return "#" + strconv.FormatUint(k, 10)
}

// fmtObj returns a formatted string for the supplied KGObject. If its of type
// KID, we'll try and return the externalID rather than KID value.
func (s *plannerStats) fmtObj(o rpc.KGObject) string {
	if o.IsType(rpc.KtKID) {
		return s.fmtKID(o.ValKID())
	}
	return o.String()
}

func (s *plannerStats) dump(w bytes.StringWriter) {
	sort.Slice(s.used, func(i, j int) bool {
		return s.used[i].description < s.used[j].description
	})
	maxDescLen := 0
	for _, u := range s.used {
		maxDescLen = cmp.MaxInt(maxDescLen, len(u.description))
	}
	prev := ""
	for _, u := range s.used {
		// The planner might ask for the same stat multiple times, so we filter
		// out those duplicates here.
		if u.description != prev {
			fmt.Fprintf(w, "%s%s %d\n",
				u.description, strings.Repeat(" ", maxDescLen-len(u.description)), u.value)
			prev = u.description
		}
	}
}

// How many partitions the SPO key-space has been hashed across.
func (s *plannerStats) NumSPOPartitions() int {
	r := s.impl.NumSPOPartitions()
	return s.track(r, "NumSPOPartitions")
}

// track will create a new statItem and add it to the list of stats used. The
// description is build from the supplied fmtString/fmtVals.
func (s *plannerStats) track(result int, fmtString string, fmtVals ...interface{}) int {
	desc := fmt.Sprintf(fmtString, fmtVals...)
	s.used = append(s.used, statItem{description: desc, value: result})
	return result
}

// How many partitions the POS key-space has been hashed across.
func (s *plannerStats) NumPOSPartitions() int {
	r := s.impl.NumPOSPartitions()
	return s.track(r, "NumPOSPartitions")
}

// The number of bytes of disk space used by a typical fact.
func (s *plannerStats) BytesPerFact() int {
	r := s.impl.BytesPerFact()
	return s.track(r, "BytesPerFact")
}

// The total number of facts.
func (s *plannerStats) NumFacts() int {
	r := s.impl.NumFacts()
	return s.track(r, "NumFacts")
}

// The number of facts with the given subject. The caller may pass 0 to ask for
// the number of facts for a typical subject.
func (s *plannerStats) NumFactsS(subject uint64) int {
	r := s.impl.NumFactsS(subject)
	return s.track(r, "NumFactsS S:%s", s.fmtKID(subject))
}

// Similar to NumFactsS.
func (s *plannerStats) NumFactsSP(subject uint64, predicate uint64) int {
	r := s.impl.NumFactsSP(subject, predicate)
	return s.track(r, "NumFactsSP S:%s P:%s", s.fmtKID(subject), s.fmtKID(predicate))
}

// Similar to NumFactsS.
func (s *plannerStats) NumFactsSO(subject uint64, object rpc.KGObject) int {
	r := s.impl.NumFactsSO(subject, object)
	return s.track(r, "NumFactsSO S:%s O:%s", s.fmtKID(subject), s.fmtObj(object))
}

// Similar to NumFactsS.
func (s *plannerStats) NumFactsP(predicate uint64) int {
	r := s.impl.NumFactsP(predicate)
	return s.track(r, "NumFactsP P:%s", s.fmtKID(predicate))
}

// Similar to NumFactsS.
func (s *plannerStats) NumFactsPO(predicate uint64, object rpc.KGObject) int {
	r := s.impl.NumFactsPO(predicate, object)
	return s.track(r, "NumFactsPO P:%s O:%s", s.fmtKID(predicate), s.fmtObj(object))
}

// Similar to NumFactsS.
func (s *plannerStats) NumFactsO(object rpc.KGObject) int {
	r := s.impl.NumFactsO(object)
	return s.track(r, "NumFactsO O:%s", s.fmtObj(object))
}
