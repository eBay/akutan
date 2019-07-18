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

// Package cache provides for caching facts that were infered during a query and pottentially
// reusing them for subsequent operations with the same query.
package cache

import (
	"sync"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/rpc"
)

// Design notes/direction
// This contains a defintion of an interface to Cache that other code can use, the semantics
// are generally aimed to be loose enough that the same Cache interface could be used for both
// a query level cache, as well as longer term shared caches.
// I think when we have a shared cache for longer term caching, you'd layer a query time cache
// on top of that for the duration of a query execution.
// the current cache impl is aimed at the per-query cache, and is pretty dumb, it does nothing
// to cap the number of items cached, and will only cache facts where the Object is a KID.

// FactCache allows for code to find and cache facts (most typically inferred facts) to
// reduce the number of parts of the query that need to be fully executed against the views
type FactCache interface {
	// AddFact offers the cache a chance to remember this fact, if it does choose to remember
	// it, it should make a copy
	AddFact(f *rpc.Fact)

	// AddInferedFact offers the cache a change to remember this inferred fact, along with
	// the actual path that was used for the inference
	AddInferedFact(f *rpc.Fact, inferencePath []rpc.Fact)

	// AddFiction offers the cache a chance to remember that the fact <s><p><o> definitely
	// does not exist at the indicated index.
	AddFiction(index uint64, subject, predicate uint64, obj rpc.KGObject)

	// Fact checks to see if there's a fact in the cache with the SPO, and that is known
	// to exist at the index
	Fact(index uint64, subject, predicate uint64, obj rpc.KGObject) (fact *rpc.Fact, inferencePath []rpc.Fact)

	// IsFiction returns true if we know that the fact <s><p><o> does not exist at the
	// log index
	IsFiction(index uint64, subject, predicate uint64, obj rpc.KGObject) bool
}

// New returns a new, empty FactCache, it is safe for concurrent use
func New() FactCache {
	return &cache{
		facts:    make(map[spoKey]cachedFact),
		fictions: make(map[spoiKey]struct{}),
	}
}

// spoKey is a compound map key for the facts map. Because the generated code for api.KGObject
// is not conducive to the hashing behaviour we'd want, we only currently cache facts that have
// Objects that are KIDs
//
// [when the map type hashs a key struct with a pointer, it just hashes the pointer value, not what
// its pointing to, and api.KGObject is full of pointers because it uses oneof()]
type spoKey struct {
	subject   uint64
	predicate uint64
	objectKID uint64
}

// spoiKey is like spoKey except it also includes the log index in the key
type spoiKey struct {
	subject   uint64
	predicate uint64
	objectKID uint64
	index     blog.Index
}

// cachedFact contains information about a single fact that we want to cache
// it includes both the fact, and the inference path that created it (for
// infered facts)
type cachedFact struct {
	fact rpc.Fact
	path []rpc.Fact
}

type cache struct {
	factLock    sync.RWMutex
	facts       map[spoKey]cachedFact
	fictionLock sync.RWMutex
	fictions    map[spoiKey]struct{}
}

func (c *cache) AddFact(f *rpc.Fact) {
	if f.Object.ValKID() > 0 {
		k := spoKey{f.Subject, f.Predicate, f.Object.ValKID()}
		c.factLock.Lock()
		c.facts[k] = cachedFact{fact: *f}
		c.factLock.Unlock()
	}
}

func (c *cache) AddInferedFact(f *rpc.Fact, path []rpc.Fact) {
	if f.Object.ValKID() > 0 {
		k := spoKey{f.Subject, f.Predicate, f.Object.ValKID()}
		c.factLock.Lock()
		c.facts[k] = cachedFact{fact: *f, path: copyOfPath(path)}
		c.factLock.Unlock()
	}
}

func (c *cache) AddFiction(index blog.Index, subject, predicate uint64, obj rpc.KGObject) {
	if obj.ValKID() > 0 {
		k := spoiKey{subject, predicate, obj.ValKID(), index}
		c.fictionLock.Lock()
		c.fictions[k] = struct{}{}
		c.fictionLock.Unlock()
	}
}

func (c *cache) Fact(index blog.Index, subject, predicate uint64, obj rpc.KGObject) (*rpc.Fact, []rpc.Fact) {
	if obj.ValKID() == 0 {
		return nil, nil
	}
	k := spoKey{subject, predicate, obj.ValKID()}
	c.factLock.RLock()
	cf, exists := c.facts[k]
	c.factLock.RUnlock()
	if !exists || cf.fact.Index != index {
		return nil, nil
	}
	// copy the path so that if the caller subsequently messes with it, it doesn't futz with our cached copy
	// we don't have this problem with fact, as it was already copied out of the hashmap
	return &cf.fact, copyOfPath(cf.path)
}

func copyOfPath(p []rpc.Fact) []rpc.Fact {
	if len(p) == 0 {
		return nil
	}
	c := make([]rpc.Fact, len(p))
	copy(c, p)
	return c
}

func (c *cache) IsFiction(index uint64, subject, predicate uint64, obj rpc.KGObject) bool {
	if obj.ValKID() == 0 {
		return false
	}
	k := spoiKey{subject, predicate, obj.ValKID(), index}
	c.fictionLock.RLock()
	_, exists := c.fictions[k]
	c.fictionLock.RUnlock()
	return exists
}
