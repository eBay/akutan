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
	"sort"
	"testing"

	"github.com/ebay/akutan/api"
	wellknown "github.com/ebay/akutan/msg/facts"
	"github.com/ebay/akutan/query/parser"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/viewclient/lookups/mocklookups"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_resolveExternalIDs(t *testing.T) {
	assert := assert.New(t)
	xidToKID, err := ResolveExternalIDs(context.Background(), nil, 30, nil)
	assert.NoError(err)
	assert.Len(xidToKID, 0)

	store, assertDone := mocklookups.New(t, mocklookups.OK(
		&rpc.LookupPORequest{
			Index: 30,
			Lookups: []rpc.LookupPORequest_Item{
				{
					Predicate: wellknown.HasExternalID,
					Object:    rpc.AString("hello", 0),
				},
			},
		}))
	xidToKID, err = ResolveExternalIDs(context.Background(), store, 30,
		[]string{"hello"})
	assert.NoError(err)
	assert.Equal(map[string]uint64{"hello": 0}, xidToKID)
	assertDone()

	store, assertDone = mocklookups.New(t, mocklookups.OK(
		&rpc.LookupPORequest{
			Index: 30,
			Lookups: []rpc.LookupPORequest_Item{
				{
					Predicate: wellknown.HasExternalID,
					Object:    rpc.AString("hello", 0),
				},
				{
					Predicate: wellknown.HasExternalID,
					Object:    rpc.AString("world", 0),
				},
			},
		},
		rpc.LookupChunk{
			Facts: []rpc.LookupChunk_Fact{
				{
					Lookup: 1,
					Fact: rpc.Fact{
						Index:     32,
						Id:        32001,
						Subject:   33,
						Predicate: wellknown.HasExternalID,
						Object:    rpc.AString("world", 0),
					},
				},
			},
		}))
	xidToKID, err = ResolveExternalIDs(context.Background(), store, 30,
		[]string{"hello", "world", "hello"})
	assert.NoError(err)
	assert.Equal(map[string]uint64{"hello": 0, "world": 33}, xidToKID)
	assertDone()
}

// This tests newInsertRequest (with respect to referenced external IDs) and the
// function referencedExternalIDs.
func Test_referencedXIDs(t *testing.T) {
	facts := []struct {
		quad string
		ref  []string
	}{
		{`?var #1 #2 #3`,
			[]string{}},
		{`<a> <b> <c>`,
			[]string{"a", "b", "c"}},
		{`test:d test:e test:f`,
			[]string{"test:d", "test:e", "test:f"}},
		{`?var <b> true`,
			[]string{"b"}},
		{`<a> <b> 12`,
			[]string{"a", "b"}},
		{`<a> <b> 12.23`,
			[]string{"a", "b"}},
		{`<a> <b> "banana"`,
			[]string{"a", "b"}},
		{`<a> <b> '2017-01-03'`,
			[]string{"a", "b"}},
		{`<a> <b> true^^<isEmpty>`,
			[]string{"a", "b", "isEmpty"}},
		{`<a> <b> 12^^<inch>`,
			[]string{"a", "b", "inch"}},
		{`<a> <b> 12.23^^<inch>`,
			[]string{"a", "b", "inch"}},
		{`<a> <b> "banana"@en_US`,
			[]string{"a", "b", "en_US"}},
		{`<a> <b> '2017-01-03'^^<dateOfBirth>`,
			[]string{"a", "b", "dateOfBirth"}},
	}
	req := &api.InsertRequest{Format: "tsv"}
	for _, fact := range facts {
		req.Facts += fact.quad + "\n"
	}
	update, res := newInsertRequest(req)
	require.Nil(t, res)
	require.Equal(t, len(facts), len(update.facts))
	for i := range facts {
		assert.Equal(t, facts[i].ref, update.facts[i].referencesXIDs,
			"referenced XIDs in fact: %v", facts[i].quad)
	}

	assert.Equal(t, []string{
		"a", "b", "c",
		"dateOfBirth", "en_US", "inch", "isEmpty",
		"test:d", "test:e", "test:f",
	}, sorted(update.allXIDs))
}

func sorted(set map[string]struct{}) []string {
	slice := make([]string, 0, len(set))
	for item := range set {
		slice = append(slice, item)
	}
	sort.Strings(slice)
	return slice
}

func Test_assignsExternalID(t *testing.T) {
	tests := []struct {
		name           string
		facts          string
		expFactsAssign []string // one per fact
		expAllXIDs     []string // in sorted order
		expRes         *api.InsertResult
	}{
		{
			name: "no assignment",
			facts: `
				<a> <b> <c>`,
			expAllXIDs: []string{"a", "b", "c"},
		},
		{
			name: "use after assignment",
			facts: `
				<c> <HasExternalID> "a"
				<a> <b> <c>`,
			expFactsAssign: []string{"a", ""},
			expAllXIDs:     []string{"HasExternalID", "a", "b", "c"},
		},
		{
			name:  "alias <HasExternalID>",
			facts: `<HasExternalID> <HasExternalID> "foo"`,
			expRes: &api.InsertResult{
				Status: api.InsertStatus_SchemaViolation,
				Error: `the subject HasExternalID may not be assigned ` +
					`additional external IDs (fact 1)`,
			},
		},
		{
			name:  "alias #4",
			facts: `#4 #4 "foo"`,
			expRes: &api.InsertResult{
				Status: api.InsertStatus_SchemaViolation,
				Error: `the subject HasExternalID may not be assigned ` +
					`additional external IDs (fact 1)`,
			},
		},
		{
			name:           "HasExternalID HasExternalID HasExternalID",
			facts:          `<HasExternalID> <HasExternalID> "HasExternalID"`,
			expFactsAssign: []string{""},
			expAllXIDs:     []string{"HasExternalID"},
		},
		{
			name:  "assign with float",
			facts: `<foo> <HasExternalID> 25.8`,
			expRes: &api.InsertResult{
				Status: api.InsertStatus_SchemaViolation,
				Error: `HasExternalID needs string object, got ` +
					`*parser.LiteralFloat (fact 1)`,
			},
		},
		{
			name:  "assign with empty string",
			facts: `<foo> <HasExternalID> ""`,
			expRes: &api.InsertResult{
				Status: api.InsertStatus_SchemaViolation,
				Error:  `HasExternalID strings must be non-empty (fact 1)`,
			},
		},
		{
			name:  "assign with language",
			facts: `<foo> <HasExternalID> "few"@en_GB`,
			expRes: &api.InsertResult{
				Status: api.InsertStatus_SchemaViolation,
				Error: `HasExternalID strings can't have language, ` +
					`got en_GB (fact 1)`,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := &api.InsertRequest{
				Format: "tsv",
				Facts:  test.facts,
			}
			update, res := newInsertRequest(req)
			if test.expRes != nil {
				if assert.NotNil(t, res) {
					assert.Equal(t, test.expRes, res)
				}
				return
			}
			assert.Nil(t, res)
			require.NotNil(t, update)
			assert.Equal(t, test.expAllXIDs, sorted(update.allXIDs),
				"insert.allXIDs")
			if test.expFactsAssign != nil {
				require.Equal(t, len(test.expFactsAssign), len(update.facts))
				for i := range test.expFactsAssign {
					assert.Equal(t, test.expFactsAssign[i],
						update.facts[i].assignsXID)
				}
			}
		})
	}
}

func Test_isHasExternalID(t *testing.T) {
	assert.True(t, isHasExternalID(&parser.LiteralID{
		Value: wellknown.HasExternalID}))
	assert.True(t, isHasExternalID(&parser.Entity{Value: "HasExternalID"}))
	assert.True(t, isHasExternalID(&parser.QName{Value: "HasExternalID"}))
	assert.False(t, isHasExternalID(&parser.Entity{Value: "xid"}))
	assert.False(t, isHasExternalID(&parser.Nil{}))
}
