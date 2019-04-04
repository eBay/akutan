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
	"fmt"
	"regexp"
	"testing"

	wellknown "github.com/ebay/beam/msg/facts"
	"github.com/ebay/beam/rpc"
	"github.com/stretchr/testify/assert"
)

func Test_Snapshot_basic(t *testing.T) {
	snap := NewSnapshot([]rpc.Fact{
		{Index: 1, Subject: 1001},
		{Index: 2, Subject: 2001},
	})
	assert.Len(t, snap.StoredFacts(), 2)
	assert.Len(t, snap.AllFacts(), 2+len(wellknown.BaseFacts()))
	assert.Len(t, snap.Filtered(0).StoredFacts(), 0)
	assert.Len(t, snap.Filtered(1).StoredFacts(), 1)
	assert.Len(t, snap.Filtered(2).StoredFacts(), 2)
}

func Test_Snapshot_CheckInvariants(t *testing.T) {
	t.Run("empty snapshot", func(t *testing.T) {
		snap := NewSnapshot(nil)
		mockT := new(mockAssert)
		snap.CheckInvariants(mockT)
		mockT.Regexp(t) // empty
	})

	t.Run("checkKIDsNotZero", func(t *testing.T) {
		snap := NewSnapshot([]rpc.Fact{
			{Index: 0, Id: 0, Subject: 0, Predicate: 0, Object: rpc.AKID(0)},
		})
		mockT := new(mockAssert)
		snap.CheckInvariants(mockT)
		mockT.Regexp(t, "subject can't be zero",
			"predicate can't be zero",
			"object can't be KID zero")
	})

	t.Run("checkFactUniqueness", func(t *testing.T) {
		snap := NewSnapshot(wellknown.BaseFacts()[:1])
		mockT := new(mockAssert)
		snap.CheckInvariants(mockT)
		mockT.Regexp(t, "duplicate.*fact: subject:2 predicate:2 object:#1")
	})

	t.Run("checkExternalIDUniqueness", func(t *testing.T) {
		snap := NewSnapshot([]rpc.Fact{
			{Subject: 1001, Predicate: wellknown.HasExternalID, Object: rpc.AString("foo", 0)},
			{Subject: 1002, Predicate: wellknown.HasExternalID, Object: rpc.AString("foo", 0)},
			{Subject: 1003, Predicate: wellknown.HasExternalID, Object: rpc.AString("foo", 0)},
		})
		mockT := new(mockAssert)
		snap.CheckInvariants(mockT)
		mockT.Regexp(t, `duplicate ExternalID found.*external ID: "foo" -> 1001 and 1002`,
			`duplicate ExternalID found.*external ID: "foo" -> 1001 and 1003`)
	})
}

func Test_Snapshot_AssertFacts(t *testing.T) {
	snap := NewSnapshot([]rpc.Fact{
		{Subject: 1001, Predicate: wellknown.HasExternalID, Object: rpc.AString("foo", 0)},
	})
	mockT := new(mockAssert)
	assert.False(t, snap.AssertFacts(mockT, "qwerty"))
	mockT.Regexp(t, `parser.*error`)

	mockT = new(mockAssert)
	assert.True(t, snap.AssertFacts(mockT, `
		<foo> <HasExternalID> "foo"
		#1001 <HasExternalID> "foo"
		<HasExternalID> <HasExternalID> "HasExternalID"
	`))
	mockT.Regexp(t) // empty

	mockT = new(mockAssert)
	assert.False(t, snap.AssertFacts(mockT, `
		<foo> <HasExternalID> "joe"
	`))
	mockT.Regexp(t, `fact not found: <foo> <HasExternalID> "joe"`)
}

func Test_Snapshot_AssertFacts_metafacts(t *testing.T) {
	snap := NewSnapshot([]rpc.Fact{
		{Id: 2001, Subject: 1001, Predicate: wellknown.HasExternalID, Object: rpc.AString("foo", 0)},
		{Id: 2002, Subject: 2001, Predicate: 1005, Object: rpc.AString("metabar", 0)},
	})
	mockT := new(mockAssert)
	assert.True(t, snap.AssertFacts(mockT, `
		?fact <foo> <HasExternalID> "foo"
		?fact #1005 "metabar"
	`))
	mockT.Regexp(t) // empty

	mockT = new(mockAssert)
	assert.False(t, snap.AssertFacts(mockT, `
		?fact <foo> <HasExternalID> "foo"
		?fact #1005 "metabolize"
	`))
	mockT.Regexp(t, `fact not found: \?fact #1005 "metabolize"`)

	mockT = new(mockAssert)
	assert.False(t, snap.AssertFacts(mockT, `
		?fact #1005 "metabar"
	`))
	mockT.Regexp(t, `variable \?fact used but not \(yet\) captured`)
}

func Test_Snapshot_ResolveXID(t *testing.T) {
	snap := NewSnapshot(nil)
	assert.Equal(t, wellknown.HasExternalID, snap.ResolveXID("HasExternalID"))
	assert.Equal(t, wellknown.HasExternalID, snap.MustResolveXID("HasExternalID"))
	assert.Equal(t, uint64(0), snap.ResolveXID("agile"))
	assert.Panics(t, func() { snap.MustResolveXID("agile") })
}

func Test_Snapshot_ExternalID(t *testing.T) {
	snap := NewSnapshot([]rpc.Fact{
		{Subject: 1001, Predicate: wellknown.HasExternalID, Object: rpc.AString("foo", 0)},
		{Subject: 1001, Predicate: wellknown.HasExternalID, Object: rpc.AString("bar", 0)},
	})
	assert.Equal(t, "foo", snap.ExternalID(1001))
	assert.Equal(t, "foo", snap.MustExternalID(1001))
	assert.Equal(t, "", snap.ExternalID(1002))
	assert.Panics(t, func() { snap.MustExternalID(1002) })
}

// mockAssert implements assert.TestingT.
type mockAssert struct {
	log []string
}

func (mock *mockAssert) Errorf(format string, args ...interface{}) {
	mock.log = append(mock.log, fmt.Sprintf(format, args...))
}

func (mock *mockAssert) Regexp(t *testing.T, patterns ...string) {
	linesMatched := make([]bool, len(mock.log))
	for _, pattern := range patterns {
		regex := regexp.MustCompile("(?s)" + pattern)
		match := false
		for i := range mock.log {
			if linesMatched[i] {
				continue
			}
			match = (regex.FindStringIndex(mock.log[i]) != nil)
			if match {
				linesMatched[i] = true
				break
			}
		}
		if !match {
			t.Errorf("mockAssert: no unmatched log line was matched by pattern: %v", pattern)
		}
	}
	for i := range mock.log {
		if !linesMatched[i] {
			t.Errorf("mockAssert: no unmatched pattern matched log line: %v", mock.log[i])
		}
	}
}
