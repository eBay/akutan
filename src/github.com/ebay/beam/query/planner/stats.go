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

package planner

import (
	"github.com/ebay/beam/rpc"
)

// Stats is used by the query planner to more accurately estimate costs.
type Stats interface {
	// How many partitions the SPO key-space has been hashed across.
	NumSPOPartitions() int
	// How many partitions the POS key-space has been hashed across.
	NumPOSPartitions() int
	// The number of bytes of disk space used by a typical fact.
	BytesPerFact() int
	// The total number of facts.
	NumFacts() int
	// The number of facts with the given subject. The caller may pass 0 to ask for
	// the number of facts for a typical subject.
	NumFactsS(subject uint64) int
	// Similar to NumFactsS.
	NumFactsSP(subject uint64, predicate uint64) int
	// Similar to NumFactsS.
	NumFactsSO(subject uint64, object rpc.KGObject) int
	// Similar to NumFactsS.
	NumFactsP(predicate uint64) int
	// Similar to NumFactsS.
	NumFactsPO(predicate uint64, object rpc.KGObject) int
	// Similar to NumFactsS.
	NumFactsO(object rpc.KGObject) int
}

type mockStats struct{}

func (*mockStats) NumSPOPartitions() int {
	return 8
}

func (*mockStats) NumPOSPartitions() int {
	return 8
}

func (*mockStats) BytesPerFact() int {
	return 100
}

func (*mockStats) NumFacts() int {
	return 1e9
}

func (*mockStats) NumFactsS(subject uint64) int {
	return 20
}

func (*mockStats) NumFactsSP(subject uint64, predicate uint64) int {
	return 2
}

func (stats *mockStats) NumFactsSO(subject uint64, object rpc.KGObject) int {
	s := stats.NumFactsS(subject)
	o := stats.NumFactsO(object)
	if s < o {
		return s / 5
	}
	return o / 5
}

func (*mockStats) NumFactsP(predicate uint64) int {
	return 1000
}

func (*mockStats) NumFactsPO(predicate uint64, object rpc.KGObject) int {
	return 100
}

func (*mockStats) NumFactsO(object rpc.KGObject) int {
	return 1000
}
