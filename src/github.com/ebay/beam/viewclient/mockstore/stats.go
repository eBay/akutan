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

package mockstore

import (
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/cmp"
)

// stats is a wrapper around Snapshot to avoid cluttering the Snapshot type with
// a bunch of stats methods. stats implements the planner.Stats interface.
type stats struct {
	snap *Snapshot
}

func (s stats) NumSPOPartitions() int {
	return 4
}

func (s stats) NumPOSPartitions() int {
	return 4
}

func (s stats) BytesPerFact() int {
	return 100
}

func (s stats) NumFacts() int {
	return len(s.snap.AllFacts())
}

func (s stats) numFacts(pred factPredicate) int {
	count := 0
	for _, fact := range s.snap.AllFacts() {
		if pred(fact) {
			count++
		}
	}
	return cmp.MaxInt(1, count)
}

// TODO: handle subject/predicate/object == 0/nil cases for all these functions.

func (s stats) NumFactsS(subject uint64) int {
	return s.numFacts(hasSubject(subject))
}

func (s stats) NumFactsSP(subject uint64, predicate uint64) int {
	return s.numFacts(hasSubjectPredicate(subject, predicate))
}

func (s stats) NumFactsSO(subject uint64, object rpc.KGObject) int {
	return s.numFacts(func(f rpc.Fact) bool {
		return f.Subject == subject && f.Object.Equal(object)
	})
}

func (s stats) NumFactsP(predicate uint64) int {
	return s.numFacts(func(f rpc.Fact) bool {
		return f.Predicate == predicate
	})
}

func (s stats) NumFactsPO(predicate uint64, object rpc.KGObject) int {
	return s.numFacts(hasPredicateObject(predicate, object))
}

func (s stats) NumFactsO(object rpc.KGObject) int {
	return s.numFacts(func(f rpc.Fact) bool {
		return f.Object.Equal(object)
	})
}
