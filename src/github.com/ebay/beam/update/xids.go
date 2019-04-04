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

package update

import (
	"context"
	"fmt"

	"github.com/ebay/beam/api"
	"github.com/ebay/beam/blog"
	wellknown "github.com/ebay/beam/msg/facts"
	"github.com/ebay/beam/query/parser"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/parallel"
	"github.com/ebay/beam/viewclient/lookups"
)

// ResolveExternalIDs looks up a KID for each given external ID. It executes all
// the lookups at the given index. It returns an error if unable to complete the
// lookups. Otherwise, every string in 'externalIDs' becomes as a key in the
// returned map; the value is a corresponding KID if found or 0 otherwise.
//
// This is public because it might be useful elsewhere.
// TODO: Perhaps it should move into viewclient.
func ResolveExternalIDs(ctx context.Context, store lookups.PO,
	index blog.Index, externalIDs []string,
) (map[string]uint64, error) {
	if len(externalIDs) == 0 {
		return nil, nil
	}
	xidToKID := make(map[string]uint64, len(externalIDs))

	// Prepare the request and write zero values into xidToKID.
	req := rpc.LookupPORequest{
		Index:   index,
		Lookups: make([]rpc.LookupPORequest_Item, 0, len(externalIDs)),
	}
	for _, xid := range externalIDs {
		if _, found := xidToKID[xid]; found {
			continue
		}
		xidToKID[xid] = 0
		req.Lookups = append(req.Lookups, rpc.LookupPORequest_Item{
			Predicate: wellknown.HasExternalID,
			Object:    rpc.AString(xid, 0),
		})
	}

	// Make the request and accumulate the results.
	resCh := make(chan *rpc.LookupChunk, 4)
	wait := parallel.Go(func() {
		for chunk := range resCh {
			for _, fact := range chunk.Facts {
				xid := fact.Fact.Object.ValString()
				xidToKID[xid] = fact.Fact.Subject
			}
		}
	})
	err := store.LookupPO(ctx, &req, resCh)
	wait()
	if err != nil {
		return nil, err
	}
	return xidToKID, nil
}

// referencedExternalIDs returns every external ID (parser.Entity or
// parser.QName string) present in the given quad. The offset given is the index
// into a list of facts, for use in log messages. The returned slice is in no
// particular order and may include duplicates.
func referencedExternalIDs(offset int, quad *parser.Quad) []string {
	externalIDs := make([]string, 0, 5)
	add := func(xid string) {
		if xid != "" {
			externalIDs = append(externalIDs, xid)
		}
	}
	handleTerm := func(term parser.Term) {
		switch term := term.(type) {
		case *parser.Nil:
		case *parser.LiteralID:
		case *parser.QName:
			add(term.Value)
		case *parser.Entity:
			add(term.Value)
		case *parser.Variable:
		case *parser.LiteralBool:
			add(term.Unit.Value)
		case *parser.LiteralFloat:
			add(term.Unit.Value)
		case *parser.LiteralInt:
			add(term.Unit.Value)
		case *parser.LiteralString:
			add(term.Language.Value)
		case *parser.LiteralTime:
			add(term.Unit.Value)
		default:
			panic(fmt.Sprintf("Unexpected type %T in insert fact %d",
				term, offset+1))
		}
	}
	handleTerm(quad.ID)
	handleTerm(quad.Subject)
	handleTerm(quad.Predicate)
	handleTerm(quad.Object)
	return externalIDs
}

// assignsExternalID inspects facts that have <HasExternalID> as their
// predicate. If the fact does not have <HasExternalID> as its predicate,
// assignsExternalID returns the empty string and nil. If the fact has
// <HasExternalID> as its predicate and passes various local checks,
// assignsExternalID returns a non-empty external ID being assigned and nil.
// Otherwise, assignsExternalID returns the empty string and a non-nil
// description of a failed check.
func assignsExternalID(offset int, fact *parser.Quad) (string, *api.InsertResult) {
	if !isHasExternalID(fact.Predicate) {
		return "", nil
	}
	object, ok := fact.Object.(*parser.LiteralString)
	if !ok {
		return "", &api.InsertResult{
			Status: api.InsertStatus_SchemaViolation,
			Error: fmt.Sprintf("HasExternalID needs string object, got %T (fact %d)",
				fact.Object, offset+1),
		}
	}
	if object.Value == "" {
		return "", &api.InsertResult{
			Status: api.InsertStatus_SchemaViolation,
			Error: fmt.Sprintf("HasExternalID strings must be non-empty (fact %d)",
				offset+1),
		}
	}
	if object.Language.Value != "" {
		return "", &api.InsertResult{
			Status: api.InsertStatus_SchemaViolation,
			Error: fmt.Sprintf("HasExternalID strings can't have language, "+
				"got %v (fact %d)",
				object.Language.Value, offset+1),
		}
	}

	// This check prevents some clown from inserting:
	//     <HasExternalID> <HasExternalID> "Foo"
	//     <Bar> <Foo> "yellow"
	// If we allowed that, then we'd have to ensure "yellow" is not used as an
	// external ID already, which would add more complexity than it's worth.
	//
	// This probably doesn't matter, but we do allow inserting:
	//     <HasExternalID> <HasExternalID> "HasExternalID"
	// so that people can insert the base facts again without errors. In this
	// case, we pretend like the fact doesn't assign anything (since it doesn't
	// assign anything new).
	if isHasExternalID(fact.Subject) {
		if wellknown.ResolveExternalID(object.Value) == wellknown.HasExternalID {
			return "", nil
		}
		return "", &api.InsertResult{
			Status: api.InsertStatus_SchemaViolation,
			Error: fmt.Sprintf("the subject HasExternalID may not be assigned "+
				"additional external IDs (fact %d)",
				offset+1),
		}
	}

	return object.Value, nil
}

// isHasExternalID returns true if the given term refers to HasExternalID, false
// otherwise. This assumes that you can only refer to HasExternalID via its KID
// or its built-in name(s) in the set of well-known facts.
func isHasExternalID(value parser.Term) bool {
	switch value := value.(type) {
	case *parser.LiteralID:
		return value.Value == wellknown.HasExternalID
	case *parser.Entity:
		return wellknown.ResolveExternalID(value.Value) == wellknown.HasExternalID
	case *parser.QName:
		return wellknown.ResolveExternalID(value.Value) == wellknown.HasExternalID
	}
	return false
}
