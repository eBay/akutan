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
	"context"
	"sync"

	"github.com/ebay/beam/blog"
	"github.com/ebay/beam/logentry/logread"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/viewclient/lookups"
)

// A DB is a mutable database of facts. It is safe for concurrent access.
type DB struct {
	lock   sync.Mutex
	locked struct {
		// The set of facts, excluding well-known facts.
		facts []rpc.Fact
		// Greater than the index of any of the facts.
		nextIndex blog.Index
	}
}

// NewDB constructs a DB that contains only the well-known facts.
func NewDB() *DB {
	db := &DB{}
	db.locked.nextIndex = 1
	return db
}

// Lookups returns an object that can perform lookup operations against the
// database.
//
// Each of the lookups takes a log index and filters out facts with a larger
// index from the database. If you don't have an index to use in the requests,
// you can use math.MaxUint64 to filter no facts out.
func (db *DB) Lookups() lookups.All {
	// Note: storeLookups is defined in lookups.go.
	return storeLookups{
		snapshot: func(ctx context.Context, index blog.Index) (*Snapshot, error) {
			return db.Current().Filtered(index), nil
		},
	}
}

// Current returns a snapshot of the current state of the database.
func (db *DB) Current() *Snapshot {
	db.lock.Lock()
	defer db.lock.Unlock()
	return NewSnapshot(db.locked.facts)
}

// AddFact adds the given fact to the database. The fact's ID and index should
// be set by the caller.
func (db *DB) AddFact(fact rpc.Fact) {
	db.lock.Lock()
	defer db.lock.Unlock()
	if db.locked.nextIndex <= fact.Index {
		db.locked.nextIndex = fact.Index + 1
	}
	db.locked.facts = append(db.locked.facts, fact)
}

// AddSPO adds a new fact with the given subject, predicate, and object. The
// fact index will be greater than anything else in the database, and the fact
// ID will be derived from that.
func (db *DB) AddSPO(subject uint64, predicate uint64, object rpc.KGObject) rpc.Fact {
	db.lock.Lock()
	defer db.lock.Unlock()
	index := db.locked.nextIndex
	db.locked.nextIndex++
	fact := rpc.Fact{
		Index:     index,
		Id:        index*logread.MaxOffset + 1,
		Subject:   subject,
		Predicate: predicate,
		Object:    object,
	}
	db.locked.facts = append(db.locked.facts, fact)
	return fact
}

// SkipIndex returns an unused log index greater than anything else in the
// database. Subsequent calls to AddSPO will not use this index.
func (db *DB) SkipIndex() blog.Index {
	db.lock.Lock()
	index := db.locked.nextIndex
	db.locked.nextIndex++
	db.lock.Unlock()
	return index
}
