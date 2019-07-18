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
	"fmt"
	"math"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/logentry"
	wellknown "github.com/ebay/akutan/msg/facts"
	"github.com/ebay/akutan/query/parser"
	"github.com/ebay/akutan/query/planner"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/viewclient/lookups"
	"github.com/stretchr/testify/assert"
)

// A Snapshot is an immutable database of facts. All Snapshots include the
// well-known base facts. Snapshots are safe for concurrent access.
type Snapshot struct {
	// The set of facts, in no particular order, not including the well-known
	// base facts.
	facts []rpc.Fact
}

// NewSnapshot constructs a snapshot that includes the given facts as well as
// the wellknown base facts. The given facts must not overlap the well-known
// base facts. The caller is free to modify the given facts after this returns.
func NewSnapshot(stored []rpc.Fact) *Snapshot {
	return &Snapshot{
		facts: append([]rpc.Fact(nil), stored...),
	}
}

// StoredFacts returns a copy of the user-created facts in the snapshot,
// excluding the well-known base facts. The returned facts are in no particular
// order.
func (snap *Snapshot) StoredFacts() []rpc.Fact {
	return append([]rpc.Fact(nil), snap.facts...)
}

// AllFacts returns a copy of all the facts in the snapshot, including the
// well-known base facts. The returned facts are in no particular order.
func (snap *Snapshot) AllFacts() []rpc.Fact {
	return append(wellknown.BaseFacts(), snap.facts...)
}

// Filtered returns a new snapshot that excludes the facts that have an index
// greater than the limit provided.
func (snap *Snapshot) Filtered(limit blog.Index) *Snapshot {
	facts := make([]rpc.Fact, 0, len(snap.facts))
	for _, fact := range snap.facts {
		if fact.Index <= limit {
			facts = append(facts, fact)
		}
	}
	return NewSnapshot(facts)
}

// CheckInvariants asserts properties on the snapshot. Specifically, it checks
// that:
//     - No subject, predicate, or object is KID 0.
//     - No fact is duplicated.
//     - The same external ID does not refer to multiple KIDs.
func (snap *Snapshot) CheckInvariants(t assert.TestingT) {
	snap.checkKIDsNotZero(t)
	snap.checkFactUniqueness(t)
	snap.checkExternalIDUniqueness(t)
}

func (snap *Snapshot) checkKIDsNotZero(t assert.TestingT) {
	for _, fact := range snap.AllFacts() {
		assert.NotZero(t, fact.Subject,
			"fact subject can't be zero. fact: %v", fact)
		assert.NotZero(t, fact.Predicate,
			"fact predicate can't be zero. fact: %v", fact)
		if fact.Object.IsType(rpc.KtKID) {
			assert.NotZero(t, fact.Object.ValKID(),
				"fact object can't be KID zero. fact: %v", fact)
		}
	}
}

func (snap *Snapshot) checkFactUniqueness(t assert.TestingT) {
	type SPO struct {
		s uint64
		p uint64
		o rpc.KGObject
	}
	factSet := make(map[SPO]struct{})
	for _, fact := range snap.AllFacts() {
		spo := SPO{fact.Subject, fact.Predicate, fact.Object}
		if _, found := factSet[spo]; found {
			assert.Fail(t, "duplicate fact found",
				"fact: subject:%v predicate:%v object:%v",
				spo.s, spo.p, spo.o)
		}
		factSet[spo] = struct{}{}
	}
}

func (snap *Snapshot) checkExternalIDUniqueness(t assert.TestingT) {
	xidToKID := make(map[string]uint64)
	for _, fact := range snap.AllFacts() {
		if fact.Predicate == wellknown.HasExternalID {
			xid := fact.Object.ValString()
			if kid, found := xidToKID[xid]; found {
				assert.Fail(t, "duplicate ExternalID found",
					"external ID: %q -> %v and %v",
					xid, kid, fact.Subject)
			} else {
				xidToKID[xid] = fact.Subject
			}
		}
	}
}

// AssertFacts fails the test if any of the given facts are not included in the
// snapshot. The given facts may use variables to capture fact IDs and express
// metafacts.
func (snap *Snapshot) AssertFacts(t assert.TestingT, facts string) bool {
	parsed, err := parser.ParseInsert("tsv", facts)
	if err != nil {
		assert.NoError(t, err, "Failed to parse facts")
		return false
	}
	variables := make(map[string]uint64)
	allOk := true
	for _, fact := range parsed.Facts {
		exists, factID := snap.hasParserFact(
			fact.Subject, fact.Predicate, fact.Object,
			variables)
		if exists {
			if id, isVar := fact.ID.(*parser.Variable); isVar {
				variables[id.Name] = factID
			}
		} else {
			assert.Fail(t, "snapshot missing fact",
				"fact not found: %v %v %v\nvariables: %v",
				fact.Subject, fact.Predicate, fact.Object, variables)
			allOk = false
		}
	}
	return allOk
}

// hasParserFact returns true and the fact ID if the given fact exists in the
// snapshot, or false and 0 otherwise.
func (snap *Snapshot) hasParserFact(
	subject, predicate, object parser.Term,
	variables map[string]uint64,
) (bool, uint64) {
	resolve := func(term parser.Term) rpc.KGObject {
		switch term := term.(type) {
		case *parser.LiteralID:
			return rpc.AKID(term.Value)
		case *parser.Entity:
			return rpc.AKID(snap.ResolveXID(term.Value))
		case *parser.QName:
			return rpc.AKID(snap.ResolveXID(term.Value))
		case *parser.Variable:
			return rpc.AKID(variables[term.Name])
		case *parser.LiteralBool:
			return rpc.ABool(term.Value, snap.ResolveXID(term.Unit.Value))
		case *parser.LiteralFloat:
			return rpc.AFloat64(term.Value, snap.ResolveXID(term.Unit.Value))
		case *parser.LiteralInt:
			return rpc.AInt64(term.Value, snap.ResolveXID(term.Unit.Value))
		case *parser.LiteralString:
			return rpc.AString(term.Value, snap.ResolveXID(term.Language.Value))
		case *parser.LiteralTime:
			return rpc.ATimestamp(term.Value, logentry.TimestampPrecision(term.Precision),
				snap.ResolveXID(term.Unit.Value))
		default:
			panic(fmt.Sprintf("Unexpected term type %T", term))
		}
	}

	pred := hasSubjectPredicateObject(
		resolve(subject).ValKID(),
		resolve(predicate).ValKID(),
		resolve(object))
	for _, fact := range snap.AllFacts() {
		if pred(fact) {
			return true, fact.Id
		}
	}
	return false, 0
}

// ResolveXID returns the KID with the given external ID, if one exists.
// Otherwise, it returns 0.
func (snap *Snapshot) ResolveXID(externalID string) uint64 {
	req := rpc.LookupPORequest{
		Index: math.MaxUint64,
		Lookups: []rpc.LookupPORequest_Item{{
			Predicate: wellknown.HasExternalID,
			Object:    rpc.AString(externalID, 0),
		}},
	}
	resCh := make(chan *rpc.LookupChunk, 1)
	err := snap.Lookups().LookupPO(context.Background(), &req, resCh)
	if err != nil {
		panic(fmt.Sprintf("mockstore.Snapshot: unexpected error from LookupPO: %v", err))
	}
	for chunk := range resCh {
		for _, fact := range chunk.Facts {
			return fact.Fact.Subject
		}
	}
	return 0
}

// MustResolveXID returns the KID with the given external ID. It panics if the
// external ID has no mapping.
func (snap *Snapshot) MustResolveXID(externalID string) uint64 {
	kid := snap.ResolveXID(externalID)
	if kid == 0 {
		panic(fmt.Sprintf("MustResolveXID: no KID for %v", externalID))
	}
	return kid
}

// ExternalID returns an arbitrary external ID for the given KID, if any exists.
// Otherwise, it returns the empty string.
func (snap *Snapshot) ExternalID(kid uint64) string {
	req := rpc.LookupSPRequest{
		Index: math.MaxUint64,
		Lookups: []rpc.LookupSPRequest_Item{{
			Subject:   kid,
			Predicate: wellknown.HasExternalID,
		}},
	}
	resCh := make(chan *rpc.LookupChunk, 1)
	err := snap.Lookups().LookupSP(context.Background(), &req, resCh)
	if err != nil {
		panic(fmt.Sprintf("mockstore.Snapshot: unexpected error from LookupSP: %v", err))
	}
	for chunk := range resCh {
		for _, fact := range chunk.Facts {
			return fact.Fact.Object.ValString()
		}
	}
	return ""
}

// MustExternalID returns an arbitrary external ID for the given KID. It panics
// if the KID has no external ID assigned.
func (snap *Snapshot) MustExternalID(kid uint64) string {
	xid := snap.ExternalID(kid)
	if xid == "" {
		panic(fmt.Sprintf("MustExternalID: no external ID for #%v", kid))
	}
	return xid
}

// Lookups returns an object that can perform lookup operations against the
// snapshot.
//
// Each of the lookups takes a log index and filters out facts with a larger
// index from the snapshot. If you don't have an index to use in the requests,
// you can use math.MaxUint64 to filter no facts out.
func (snap *Snapshot) Lookups() lookups.All {
	// Note: the snapLookups struct and its methods are defined in stats.go.
	return snapLookups{snap}
}

// Stats returns statistics about the facts stored in the snapshot. These
// statistics also include information about a cluster with a handful of
// partitions.
func (snap *Snapshot) Stats() planner.Stats {
	// Note: the stats struct and its methods are defined in stats.go.
	return stats{snap}
}
