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

// Package facts defines the well known base set of facts that are needed to bootstrap the graph
package facts

import (
	"fmt"

	"github.com/ebay/akutan/rpc"
)

const (
	// Predicate defines the well known subject KID for the subject predicate,
	// all predicates should have an InstanceOf path to this to be a Predicate
	Predicate uint64 = 1

	// InstanceOf defines the well known subject KID for the InstanceOf
	// predicate, this predicate is used to declare that a subject is an
	// instance of another subject At a minimum this is used to define the
	// relationship between Predicate types
	InstanceOf uint64 = 2

	// CanonicalLabel defines the well known subject KID for the CanonicalLabel
	// predicate, this predicate is used to delcare a label for a subject It is
	// expected that most (all?) Subjects have a Canonical Label
	CanonicalLabel uint64 = 3

	// HasExternalID defines the well know subject KID for the <HasExternalId>
	// predicate, this predicate is used to delcare an external identifier for a
	// subject. It is expected that most (all?) Subjects have an ExternalID.
	HasExternalID uint64 = 4

	// InLanguage defines the well known subject KID for the <InLanguage>
	// predicate used to define the language of a subject.
	InLanguage uint64 = 5

	// InUnits defines the well known subject KID for the <InUnits> predicate,
	// used to declare a unit of measure for a subject.  It is expected that
	// most subjects with literals will specify units.
	InUnits uint64 = 6

	// CompareOperator defines the predicate for the
	// predicate used to define comparisons for a subject+object.
	CompareOperator uint64 = 7

	// EqualTo compare operator sid.
	EqualTo uint64 = 8

	// LessThan compare operator sid.
	LessThan uint64 = 9

	// LessThanOrEqualTo compare operator sid.
	LessThanOrEqualTo uint64 = 10

	// GreaterThan compare operator sid.
	GreaterThan uint64 = 11

	// GreaterThanOrEqualTo compare operator sid.
	GreaterThanOrEqualTo uint64 = 12
)

var (
	// PredicateKGO is a KGObject instance for the Predicate KID
	PredicateKGO = rpc.AKID(Predicate)

	// InstanceOfKGO is a KGOBject instance for the InstanceOf KID
	InstanceOfKGO = rpc.AKID(InstanceOf)

	// CanonicalLabelKGO is a KGObject instance for the CanonicalLabel KID
	CanonicalLabelKGO = rpc.AKID(CanonicalLabel)

	// HasExternalIDKGO is a KGObject instance for the HasExternalID KID
	HasExternalIDKGO = rpc.AKID(HasExternalID)

	// InLanguageKGO is the KGObject instance for the InLanguage KID.
	InLanguageKGO = rpc.AKID(InLanguage)

	// InUnitsKGO is a KObject instance for the InUnits KID
	InUnitsKGO = rpc.AKID(InUnits)

	// CompareOperatorKGO is the object instance for the operator KID.
	CompareOperatorKGO = rpc.AKID(CompareOperator)
)

// BaseFacts returns the set of well-known facts. The caller may modify the
// returned slice.
func BaseFacts() []rpc.Fact {
	id := uint64(500)
	nextID := func() uint64 {
		res := id
		id++
		return res
	}
	base := []rpc.Fact{
		{Id: nextID(), Subject: InstanceOf, Predicate: InstanceOf, Object: PredicateKGO},
		{Id: nextID(), Subject: CanonicalLabel, Predicate: InstanceOf, Object: PredicateKGO},
		{Id: nextID(), Subject: Predicate, Predicate: CanonicalLabel, Object: rpc.AString("Predicate", 0)},
		{Id: nextID(), Subject: InstanceOf, Predicate: CanonicalLabel, Object: rpc.AString("InstanceOf", 0)},
		{Id: nextID(), Subject: CanonicalLabel, Predicate: CanonicalLabel, Object: rpc.AString("CanonicalLabel", 0)},
		{Id: nextID(), Subject: HasExternalID, Predicate: InstanceOf, Object: PredicateKGO},
		{Id: nextID(), Subject: HasExternalID, Predicate: CanonicalLabel, Object: rpc.AString("HasExternalID", 0)},

		{Id: nextID(), Subject: Predicate, Predicate: HasExternalID, Object: rpc.AString("Predicate", 0)},
		{Id: nextID(), Subject: InstanceOf, Predicate: HasExternalID, Object: rpc.AString("InstanceOf", 0)},
		{Id: nextID(), Subject: CanonicalLabel, Predicate: HasExternalID, Object: rpc.AString("CanonicalLabel", 0)},
		{Id: nextID(), Subject: HasExternalID, Predicate: HasExternalID, Object: rpc.AString("HasExternalID", 0)},

		{Id: nextID(), Subject: InLanguage, Predicate: InstanceOf, Object: PredicateKGO},
		{Id: nextID(), Subject: InLanguage, Predicate: CanonicalLabel, Object: rpc.AString("InLanguage", 0)},
		{Id: nextID(), Subject: InLanguage, Predicate: HasExternalID, Object: rpc.AString("InLanguage", 0)},

		{Id: nextID(), Subject: InUnits, Predicate: InstanceOf, Object: PredicateKGO},
		{Id: nextID(), Subject: InUnits, Predicate: CanonicalLabel, Object: rpc.AString("InUnits", 0)},
		{Id: nextID(), Subject: InUnits, Predicate: HasExternalID, Object: rpc.AString("InUnits", 0)},
	}
	addCompare := func(subject uint64, label, externalID string) {
		base = append(base, rpc.Fact{Id: nextID(), Subject: subject, Predicate: InstanceOf, Object: CompareOperatorKGO})
		base = append(base, rpc.Fact{Id: nextID(), Subject: subject, Predicate: CanonicalLabel, Object: rpc.AString(label, 0)})
		base = append(base, rpc.Fact{Id: nextID(), Subject: subject, Predicate: HasExternalID, Object: rpc.AString(externalID, 0)})
	}
	addCompare(EqualTo, "EqualTo", "eq")
	addCompare(LessThan, "LessThan", "lt")
	addCompare(LessThanOrEqualTo, "LessThanOrEqualTo", "lte")
	addCompare(GreaterThan, "GreaterThan", "gt")
	addCompare(GreaterThanOrEqualTo, "GreaterThanOrEqualTo", "gte")
	return base
}

// ResolveExternalID returns a non-zero KID if there exists a well-known fact
// with that KID as the subject, <HasExternalID> as the predicate, and the given
// externalID as the object. Otherwise, it returns 0.
func ResolveExternalID(externalID string) uint64 {
	return xidToKID[externalID]
}

var xidToKID = make(map[string]uint64)

func init() {
	for i, fact := range BaseFacts() {
		if fact.Predicate == HasExternalID {
			xid := fact.Object.ValString()
			if xid == "" {
				panic(fmt.Sprintf("Found empty external ID string in base fact %d", i+1))
			}
			if fact.Subject == 0 {
				panic(fmt.Sprintf("Base facts can't assign external ID %q to KID 0", xid))
			}
			if _, found := xidToKID[xid]; found {
				panic(fmt.Sprintf("Duplicate external ID assignment in base facts: %s", xid))
			}
			xidToKID[xid] = fact.Subject
		}
	}
}
