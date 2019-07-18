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
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/ebay/akutan/api"
	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/blog/mockblog"
	"github.com/ebay/akutan/logentry"
	"github.com/ebay/akutan/logentry/logencoder"
	"github.com/ebay/akutan/logentry/logread"
	wellknown "github.com/ebay/akutan/msg/facts"
	"github.com/ebay/akutan/query/parser"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/viewclient/lookups/mocklookups"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Insert(t *testing.T) {
	assert := assert.New(t)
	res, err := Insert(context.Background(), &api.InsertRequest{
		Format: "pdf",
		Facts:  "the:sky akutan:is colors:blue",
	}, 13, nil, nil)
	if assert.NoError(err) {
		assert.Equal(api.InsertStatus_ParseError.String(), res.Status.String())
		assert.Zero(res.Index)
		assert.Regexp("unsupported input format: pdf", res.Error)
	}

	res, err = Insert(context.Background(), &api.InsertRequest{
		Format: "tsv",
		Facts:  "Is the sky really blue?",
	}, 13, nil, nil)
	if assert.NoError(err) {
		assert.Equal(api.InsertStatus_ParseError.String(), res.Status.String())
		assert.Zero(res.Index)
		assert.Regexp("pars", res.Error)
	}

	res, err = Insert(context.Background(), &api.InsertRequest{
		Format: "tsv",
		Facts:  "# empty",
	}, 13, nil, nil)
	if assert.NoError(err) {
		assert.Equal(api.InsertStatus_OK.String(), res.Status.String())
		assert.Equal(uint64(13), res.Index)
	}
}

func Test_updateRequest_attempt_alreadyDone(t *testing.T) {
	update := &updateRequest{}
	reads := &updateReadSet{
		index: 50,
	}
	res, newReads, err := update.attempt(context.Background(), 1, reads, nil, nil)
	assert.NoError(t, err)
	if assert.NotNil(t, res) {
		assert.Equal(t, api.InsertStatus_OK.String(), res.Status.String())
		assert.Equal(t, reads.index, res.Index)
		assert.Nil(t, newReads)
	}
}

func Test_updateRequest_attempt(t *testing.T) {
	makeUpdateRequest := func() *updateRequest {
		return &updateRequest{
			facts: []insertFact{{
				quad: &parser.Quad{
					Subject:   &parser.LiteralID{Value: 1},
					Predicate: &parser.LiteralID{Value: 2},
					Object:    &parser.LiteralID{Value: 3},
				},
			}},
		}
	}
	lookupSPO := func(index blog.Index) *rpc.LookupSPORequest {
		return &rpc.LookupSPORequest{
			Index: index,
			Lookups: []rpc.LookupSPORequest_Item{{
				Subject:   1,
				Predicate: 2,
				Object:    rpc.AKID(3),
			}},
		}
	}
	lookupSPOReply := func(index blog.Index) rpc.LookupChunk {
		return rpc.LookupChunk{
			Facts: []rpc.LookupChunk_Fact{{
				Lookup: 0,
				Fact: rpc.Fact{
					Id:        index*logread.MaxOffset + 1,
					Subject:   1,
					Predicate: 2,
					Object:    rpc.AKID(3),
				},
			}},
		}
	}
	insertTxCmd := func() *logentry.InsertTxCommand {
		return &logentry.InsertTxCommand{
			Facts: []logentry.InsertFact{{
				FactIDOffset: 1,
				Subject:      logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 1}},
				Predicate:    logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 2}},
				Object:       logentry.KGObject{Value: &logentry.KGObject_AKID{AKID: 3}},
			}},
		}
	}

	t.Run("no intervening writes", func(t *testing.T) {
		update := makeUpdateRequest()
		reads := &updateReadSet{
			index:           50,
			existingFactIDs: []uint64{0},
		}
		store, assertDone := mocklookups.New(t,
			mocklookups.OK(lookupSPO(51), lookupSPOReply(51)),
		)
		ctx := context.Background()
		blog := mockblog.New(ctx)
		blog.Discard(ctx, 51)
		res, newReads, err := update.attempt(ctx, 1, reads, store, blog)
		assert.NoError(t, err)
		if assert.NotNil(t, res) {
			assert.Equal(t, api.InsertStatus_OK.String(), res.Status.String())
			assert.Equal(t, uint64(51), res.Index)
			assert.Nil(t, newReads)
		}
		blog.AssertCommands(t, rpc.LogPosition{Index: 51, Version: logencoder.DefaultLogVersion},
			[]logencoder.ProtobufCommand{
				insertTxCmd(),
				&logentry.TxDecisionCommand{Tx: 51, Commit: true},
			})
		assertDone()
	})

	t.Run("normal", func(t *testing.T) {
		update := makeUpdateRequest()
		reads := &updateReadSet{
			index:           50,
			existingFactIDs: []uint64{0},
		}
		store, assertDone := mocklookups.New(t,
			mocklookups.OK(lookupSPO(59) /* with empty result */),
			mocklookups.OK(lookupSPO(60), lookupSPOReply(60)),
		)
		ctx := context.Background()
		aLog := mockblog.New(ctx)
		aLog.Discard(ctx, 60)
		res, newReads, err := update.attempt(ctx, 1, reads, store, aLog)
		assert.NoError(t, err)
		if assert.NotNil(t, res) {
			assert.Equal(t, api.InsertStatus_OK.String(), res.Status.String())
			assert.Equal(t, uint64(60), res.Index)
			assert.Nil(t, newReads)
		}
		aLog.AssertCommands(t, rpc.LogPosition{Index: 60, Version: logencoder.DefaultLogVersion},
			[]logencoder.ProtobufCommand{
				insertTxCmd(),
				&logentry.TxDecisionCommand{Tx: 60, Commit: true},
			})
		assertDone()
	})

	t.Run("externally aborted", func(t *testing.T) {
		update := makeUpdateRequest()
		reads := &updateReadSet{
			index:           50,
			existingFactIDs: []uint64{0},
		}
		store, assertDone := mocklookups.New(t,
			mocklookups.OK(lookupSPO(59) /* with empty result */),
			mocklookups.OK(lookupSPO(60) /* with empty result */),
		)
		ctx := context.Background()
		aLog := mockblog.New(ctx)
		aLog.Discard(ctx, 60)
		res, newReads, err := update.attempt(ctx, 1, reads, store, aLog)
		assert.NoError(t, err)
		assert.Nil(t, res)
		if assert.NotNil(t, newReads) {
			assert.Equal(t, uint64(59), newReads.index)
			assert.Equal(t, uint64(0), newReads.existingFactIDs[0])
		}
		aLog.AssertCommands(t, rpc.LogPosition{Index: 60, Version: logencoder.DefaultLogVersion},
			[]logencoder.ProtobufCommand{
				insertTxCmd(),
				&logentry.TxDecisionCommand{Tx: 60, Commit: true},
			})
		assertDone()
	})

	t.Run("read set changed", func(t *testing.T) {
		update := makeUpdateRequest()
		reads := &updateReadSet{
			index:           50,
			existingFactIDs: []uint64{0},
		}
		store, assertDone := mocklookups.New(t,
			mocklookups.OK(lookupSPO(59), lookupSPOReply(55)),
		)
		ctx := context.Background()
		aLog := mockblog.New(ctx)
		aLog.Discard(ctx, 60)
		res, newReads, err := update.attempt(ctx, 1, reads, store, aLog)
		assert.NoError(t, err)
		assert.Nil(t, res)
		if assert.NotNil(t, newReads) {
			assert.Equal(t, uint64(59), newReads.index)
			assert.Equal(t, uint64(55001), newReads.existingFactIDs[0])
		}
		aLog.AssertCommands(t, rpc.LogPosition{Index: 60, Version: logencoder.DefaultLogVersion},
			[]logencoder.ProtobufCommand{
				insertTxCmd(),
				&logentry.TxDecisionCommand{Tx: 60, Commit: false},
			})
		assertDone()
	})
}

func Test_newInsertRequest_parserError(t *testing.T) {
	update, res := newInsertRequest(&api.InsertRequest{
		Format: "pdf",
		Facts:  "the:sky akutan:is colors:blue",
	})
	assert := assert.New(t)
	assert.Nil(update)
	if assert.NotNil(res) {
		assert.Equal(api.InsertStatus_ParseError.String(), res.Status.String())
		assert.Zero(res.Index)
		assert.Regexp("unsupported input format: pdf", res.Error)
	}
}

func Test_newInsertRequest(t *testing.T) {
	tests := []struct {
		name           string
		facts          string
		expRefXIDs     []string // one space-delimited list per fact
		expFactsAssign []string // one per fact
		expAllXIDs     []string // in sorted order
		expRes         *api.InsertResult
	}{
		{
			name: "no assignment",
			facts: `
				<a> <b> <c>`,
			expRefXIDs:     []string{"a b c"},
			expFactsAssign: []string{""},
			expAllXIDs:     []string{"a", "b", "c"},
		},
		{
			name: "assignment",
			facts: `
				<c> <HasExternalID> "a"`,
			expRefXIDs:     []string{"c HasExternalID"},
			expFactsAssign: []string{"a"},
			expAllXIDs:     []string{"HasExternalID", "a", "c"},
		},
		{
			name: "use after assignment",
			facts: `
				<c> <HasExternalID> "a"
				<a> <b> <c>`,
			expRefXIDs:     []string{"c HasExternalID", "a b c"},
			expFactsAssign: []string{"a", ""},
			expAllXIDs:     []string{"HasExternalID", "a", "b", "c"},
		},
		{
			name: "assignment after use",
			facts: `
				<a> <b> <c>
				<c> <HasExternalID> "a"`,
			expRes: &api.InsertResult{
				Status: api.InsertStatus_SchemaViolation,
				Error: `can't use and then manually assign external ID "a" ` +
					`(fact 2)`,
			},
		},
		{
			name: "assignment after assignment",
			facts: `
				<c> <HasExternalID> "a"
				<c> <HasExternalID> "a"`,
			expRes: &api.InsertResult{
				Status: api.InsertStatus_SchemaViolation,
				Error: `can't use and then manually assign external ID "a" ` +
					`(fact 2)`,
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
				"update.allXIDs")
			if assert.Equal(t, len(test.expRefXIDs), len(update.facts)) {
				for i := range test.expRefXIDs {
					assert.Equal(t, strings.Split(test.expRefXIDs[i], " "),
						update.facts[i].referencesXIDs)
				}
			}
			if assert.Equal(t, len(test.expFactsAssign), len(update.facts)) {
				for i := range test.expFactsAssign {
					assert.Equal(t, test.expFactsAssign[i],
						update.facts[i].assignsXID)
				}
			}
		})
	}
}

// There's not much in here to test, since resolveExternalIDs and lookupFacts
// are tested separately.
func Test_updateRequest_read(t *testing.T) {
	update := &updateRequest{}
	reads, err := update.read(context.Background(), nil, 50)
	assert.NoError(t, err)
	assert.Equal(t, uint64(50), reads.index)
	assert.Len(t, reads.xidToKID, 0)
	assert.Len(t, reads.existingFactIDs, 0)
}

func Test_updateRequest_lookupFacts_empty(t *testing.T) {
	update := &updateRequest{}
	existingFactIDs, err := update.lookupFacts(context.Background(), nil, 13, nil)
	assert.NoError(t, err)
	assert.Len(t, existingFactIDs, 0)
}

func Test_updateRequest_lookupFacts_basic(t *testing.T) {
	update, apiRes := newInsertRequest(&api.InsertRequest{
		Format: "tsv",
		Facts: `
			# This fact will not exist.
			the:sky akutan:is colors:blue
			# This fact will exist.
			the:grass akutan:is colors:green
			# This fact will not be ready for a LookupSPO
			# because <undefined> is a new external ID.
			<undefined> akutan:is colors:blue
	`})
	require.Nil(t, apiRes)
	xids := map[string]uint64{
		"the:sky":      1001,
		"akutan:is":    1002,
		"colors:blue":  1003,
		"the:grass":    1004,
		"colors:green": 1005,
		"undefined":    0,
	}

	store, assertDone := mocklookups.New(t,
		mocklookups.OK(
			&rpc.LookupSPORequest{
				Index: 93,
				Lookups: []rpc.LookupSPORequest_Item{
					{
						Subject:   xids["the:sky"],
						Predicate: xids["akutan:is"],
						Object:    rpc.AKID(xids["colors:blue"]),
					},
					{
						Subject:   xids["the:grass"],
						Predicate: xids["akutan:is"],
						Object:    rpc.AKID(xids["colors:green"]),
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
							Subject:   xids["the:sky"],
							Predicate: xids["akutan:is"],
							Object:    rpc.AKID(xids["colors:blue"]),
						},
					},
				},
			},
		))
	existingFactIDs, err := update.lookupFacts(context.Background(), store, 93, xids)
	assert.NoError(t, err)
	assert.Equal(t, []uint64{0, 32001, 0}, existingFactIDs)
	assertDone()
}

func Test_updateRequest_lookupFacts_meta(t *testing.T) {
	update, apiRes := newInsertRequest(&api.InsertRequest{
		Format: "tsv",
		Facts: `
			# All of these facts will exist.
			?zero   the:sky  akutan:is       colors:blue
			?one    ?zero    akutan:source   place:san_francisco
			?two    ?one     akutan:source   web:weather_com
			?three  ?two     akutan:channel  comms:web
			# For fun, this next noise fact won't exist.
			        ?three   data:noise    0.65
			# This one will introduce a new external ID.
			        ?three   akutan:like     <aloha_net>
			# Finally, this signal fact will exist.
			        ?three   data:signal   0.8
	`})
	require.Nil(t, apiRes)
	xids := map[string]uint64{
		"the:sky":             1001,
		"akutan:is":           1002,
		"colors:blue":         1003,
		"akutan:source":       1004,
		"place:san_francisco": 1005,
		"web:weather_com":     1006,
		"akutan:channel":      1007,
		"comms:web":           1008,
		"data:noise":          1009,
		"akutan:like":         1010,
		"data:signal":         1011,
		"aloha_net":           0,
	}
	vars := map[string]uint64{
		"zero":  2001,
		"one":   2002,
		"two":   2003,
		"three": 2004,
	}
	// The index, subject, predicate, and object are omitted from the replies
	// since they're not used.
	store, assertDone := mocklookups.New(t,
		mocklookups.OK(
			&rpc.LookupSPORequest{
				Index: 93,
				Lookups: []rpc.LookupSPORequest_Item{
					{
						Subject:   xids["the:sky"],
						Predicate: xids["akutan:is"],
						Object:    rpc.AKID(xids["colors:blue"]),
					},
				},
			},
			rpc.LookupChunk{Facts: []rpc.LookupChunk_Fact{
				{Lookup: 0, Fact: rpc.Fact{Id: vars["zero"]}},
			}},
		),
		mocklookups.OK(
			&rpc.LookupSPORequest{
				Index: 93,
				Lookups: []rpc.LookupSPORequest_Item{
					{
						Subject:   vars["zero"],
						Predicate: xids["akutan:source"],
						Object:    rpc.AKID(xids["place:san_francisco"]),
					},
				},
			},
			rpc.LookupChunk{Facts: []rpc.LookupChunk_Fact{
				{Lookup: 0, Fact: rpc.Fact{Id: vars["one"]}},
			}},
		),
		mocklookups.OK(
			&rpc.LookupSPORequest{
				Index: 93,
				Lookups: []rpc.LookupSPORequest_Item{
					{
						Subject:   vars["one"],
						Predicate: xids["akutan:source"],
						Object:    rpc.AKID(xids["web:weather_com"]),
					},
				},
			},
			rpc.LookupChunk{Facts: []rpc.LookupChunk_Fact{
				{Lookup: 0, Fact: rpc.Fact{Id: vars["two"]}},
			}},
		),
		mocklookups.OK(
			&rpc.LookupSPORequest{
				Index: 93,
				Lookups: []rpc.LookupSPORequest_Item{
					{
						Subject:   vars["two"],
						Predicate: xids["akutan:channel"],
						Object:    rpc.AKID(xids["comms:web"]),
					},
				},
			},
			rpc.LookupChunk{Facts: []rpc.LookupChunk_Fact{
				{Lookup: 0, Fact: rpc.Fact{Id: vars["three"]}},
			}},
		),
		mocklookups.OK(
			&rpc.LookupSPORequest{
				Index: 93,
				Lookups: []rpc.LookupSPORequest_Item{
					{
						Subject:   vars["three"],
						Predicate: xids["data:noise"],
						Object:    rpc.AFloat64(0.65, 0),
					},
					{
						Subject:   vars["three"],
						Predicate: xids["data:signal"],
						Object:    rpc.AFloat64(0.8, 0),
					},
				},
			},
			rpc.LookupChunk{
				Facts: []rpc.LookupChunk_Fact{
					{Lookup: 1, Fact: rpc.Fact{Id: 9025}},
				}},
		),
	)
	existingFactIDs, err := update.lookupFacts(context.Background(), store, 93, xids)
	assert.NoError(t, err)
	assert.Equal(t, []uint64{
		vars["zero"], vars["one"], vars["two"], vars["three"], 0, 0, 9025,
	}, existingFactIDs)
	assertDone()
}

// This test focuses on the handling of external IDs in buildStartTxCommand.
func Test_updateRequest_buildStartTxCommand_xids(t *testing.T) {
	update, apiRes := newInsertRequest(&api.InsertRequest{
		Format: "tsv",
		Facts: `
			the:sky akutan:is the:sky
			the:cloud akutan:in the:sky
	`})
	require.Nil(t, apiRes)
	reads := &updateReadSet{
		xidToKID: map[string]uint64{
			"the:sky":   0,
			"akutan:is": 1001,
			"the:cloud": 0,
			"akutan:in": 0,
		},
		existingFactIDs: make([]uint64, len(update.facts)),
	}
	expected := &logentry.InsertTxCommand{
		Facts: []logentry.InsertFact{
			{
				FactIDOffset: 1,
				Subject:      logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Offset{Offset: 2}},
				Predicate:    logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: wellknown.HasExternalID}},
				Object:       logentry.KGObject{Value: &logentry.KGObject_AString{AString: "the:sky"}},
			}, {
				FactIDOffset: 3,
				Subject:      logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Offset{Offset: 2}},
				Predicate:    logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 1001}},
				Object:       logentry.KGObject{Value: &logentry.KGObject_AKIDOffset{AKIDOffset: 2}},
			}, {
				FactIDOffset: 4,
				Subject:      logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Offset{Offset: 5}},
				Predicate:    logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: wellknown.HasExternalID}},
				Object:       logentry.KGObject{Value: &logentry.KGObject_AString{AString: "the:cloud"}},
			}, {
				FactIDOffset: 6,
				Subject:      logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Offset{Offset: 7}},
				Predicate:    logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: wellknown.HasExternalID}},
				Object:       logentry.KGObject{Value: &logentry.KGObject_AString{AString: "akutan:in"}},
			}, {
				FactIDOffset: 8,
				Subject:      logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Offset{Offset: 5}},
				Predicate:    logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Offset{Offset: 7}},
				Object:       logentry.KGObject{Value: &logentry.KGObject_AKIDOffset{AKIDOffset: 2}},
			},
		},
	}
	txCmd, apiRes := update.buildStartTxCommand(reads)
	assert.Nil(t, apiRes)
	assert.Equal(t, expected.GoString(), txCmd.GoString())
}

// This test focuses on the handling of fact IDs, metafacts, and existing facts
// in buildStartTxCommand.
func Test_updateRequest_buildStartTxCommand_metafacts(t *testing.T) {
	update, apiRes := newInsertRequest(&api.InsertRequest{
		Format: "tsv",
		Facts: `
			?one    the:sky  akutan:is       colors:blue
			?two    ?one     akutan:source   place:san_francisco
			        ?two     akutan:source   web:weather_com
	`})
	require.Nil(t, apiRes)
	reads := &updateReadSet{
		index: 6,
		xidToKID: map[string]uint64{
			"the:sky":             1001,
			"akutan:is":           1002,
			"colors:blue":         1003,
			"akutan:source":       1004,
			"place:san_francisco": 1005,
			"web:weather_com":     1006,
		},
		existingFactIDs: make([]uint64, len(update.facts)),
	}
	expected := &logentry.InsertTxCommand{
		Facts: []logentry.InsertFact{
			{
				FactIDOffset: 1,
				Subject:      logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 1001}},
				Predicate:    logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 1002}},
				Object:       logentry.KGObject{Value: &logentry.KGObject_AKID{AKID: 1003}},
			}, {
				FactIDOffset: 2,
				Subject:      logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Offset{Offset: 1}},
				Predicate:    logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 1004}},
				Object:       logentry.KGObject{Value: &logentry.KGObject_AKID{AKID: 1005}},
			}, {
				FactIDOffset: 3,
				Subject:      logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Offset{Offset: 2}},
				Predicate:    logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 1004}},
				Object:       logentry.KGObject{Value: &logentry.KGObject_AKID{AKID: 1006}},
			},
		},
	}
	txCmd, apiRes := update.buildStartTxCommand(reads)
	assert.Nil(t, apiRes)
	assert.Equal(t, expected.GoString(), txCmd.GoString())

	reads.existingFactIDs = []uint64{5003, 0, 0}
	expected = &logentry.InsertTxCommand{
		Facts: []logentry.InsertFact{
			{
				FactIDOffset: 1,
				Subject:      logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 5003}},
				Predicate:    logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 1004}},
				Object:       logentry.KGObject{Value: &logentry.KGObject_AKID{AKID: 1005}},
			}, {
				FactIDOffset: 2,
				Subject:      logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Offset{Offset: 1}},
				Predicate:    logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 1004}},
				Object:       logentry.KGObject{Value: &logentry.KGObject_AKID{AKID: 1006}},
			},
		},
	}
	txCmd, apiRes = update.buildStartTxCommand(reads)
	assert.Nil(t, apiRes)
	assert.Equal(t, expected.GoString(), txCmd.GoString())

	reads.existingFactIDs = []uint64{5003, 5004, 0}
	expected = &logentry.InsertTxCommand{
		Facts: []logentry.InsertFact{
			{
				FactIDOffset: 1,
				Subject:      logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 5004}},
				Predicate:    logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 1004}},
				Object:       logentry.KGObject{Value: &logentry.KGObject_AKID{AKID: 1006}},
			},
		},
	}
	txCmd, apiRes = update.buildStartTxCommand(reads)
	assert.Nil(t, apiRes)
	assert.Equal(t, expected.GoString(), txCmd.GoString())

	reads.existingFactIDs = []uint64{5003, 5004, 5005}
	txCmd, apiRes = update.buildStartTxCommand(reads)
	assert.Nil(t, txCmd)
	if assert.NotNil(t, apiRes) {
		assert.Equal(t, api.InsertStatus_OK.String(),
			apiRes.Status.String())
		assert.Equal(t, reads.index, apiRes.Index)
	}
}

// This test focuses on the handling of requests that need too many new KIDs in
// buildStartTxCommand.
func Test_updateRequest_buildStartTxCommand_tooBig(t *testing.T) {
	build := func(numFacts int) (*logentry.InsertTxCommand, *api.InsertResult) {
		facts := make([]insertFact, numFacts)
		for i := range facts {
			facts[i] = insertFact{
				quad: &parser.Quad{
					ID:        &parser.Nil{},
					Subject:   &parser.LiteralID{Value: uint64(1000*i + 1)},
					Predicate: &parser.LiteralID{Value: uint64(1000*i + 2)},
					Object:    &parser.LiteralID{Value: uint64(1000*i + 3)},
				},
			}
		}
		update := &updateRequest{
			facts: facts,
		}
		reads := &updateReadSet{
			existingFactIDs: make([]uint64, len(facts)),
		}
		return update.buildStartTxCommand(reads)
	}

	t.Run("just under limit", func(t *testing.T) {
		txCmd, res := build(logread.MaxOffset - 1)
		assert.Nil(t, res)
		if assert.NotNil(t, txCmd) {
			assert.Equal(t, logread.MaxOffset-1, len(txCmd.Facts),
				"num facts")
			for i := range txCmd.Facts {
				assert.Equal(t, int32(i+1), txCmd.Facts[i].FactIDOffset,
					"fact ID offset at fact %v", i)
			}
		}
	})

	t.Run("just over limit", func(t *testing.T) {
		txCmd, res := build(logread.MaxOffset)
		assert.Nil(t, txCmd)
		if assert.NotNil(t, res) {
			assert.Equal(t, api.InsertStatus_AtomicRequestTooBig.String(),
				res.Status.String())
			assert.Zero(t, res.Index)
			assert.Equal(t, "insert request too big: "+
				"need 1000 KIDs but hard limit is 999",
				res.Error)
		}
	})
}

func Test_wasExternallyAborted(t *testing.T) {
	ctx := context.Background()
	txCmd := &logentry.InsertTxCommand{
		Facts: []logentry.InsertFact{
			{
				FactIDOffset: 1,
				Subject:      logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 1}},
				Predicate:    logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 2}},
				Object:       logentry.KGObject{Value: &logentry.KGObject_AKIDOffset{AKIDOffset: 2}},
			},
		},
	}
	expReq := &rpc.LookupSPORequest{
		Index: 50,
		Lookups: []rpc.LookupSPORequest_Item{{
			Subject:   1,
			Predicate: 2,
			Object:    rpc.AKID(50*logread.MaxOffset + 2),
		}},
	}

	t.Run("OK with empty reply", func(t *testing.T) {
		store, assertDone := mocklookups.New(t, mocklookups.OK(expReq))
		aborted, err := wasExternallyAborted(ctx, store, 50, txCmd)
		assert.NoError(t, err)
		assert.True(t, aborted)
		assertDone()
	})

	t.Run("OK with reply", func(t *testing.T) {
		store, assertDone := mocklookups.New(t,
			mocklookups.OK(expReq, rpc.LookupChunk{
				Facts: []rpc.LookupChunk_Fact{{
					Lookup: 1,
					Fact: rpc.Fact{
						Index:     18,
						Id:        18009,
						Subject:   1,
						Predicate: 2,
						Object:    rpc.AKID(50*logread.MaxOffset + 2),
					},
				}},
			}))
		aborted, err := wasExternallyAborted(ctx, store, 50, txCmd)
		assert.NoError(t, err)
		assert.False(t, aborted)
		assertDone()
	})

	t.Run("error from LookupSPO", func(t *testing.T) {
		store, assertDone := mocklookups.New(t,
			mocklookups.Err(expReq, errors.New("ants in pants")))
		aborted, err := wasExternallyAborted(ctx, store, 50, txCmd)
		assert.EqualError(t, err, "ants in pants")
		assert.True(t, aborted)
		assertDone()
	})
}

func Test_updateReadSet_equalValues(t *testing.T) {
	// empty case
	empty1 := &updateReadSet{index: 1}
	empty2 := &updateReadSet{index: 2}
	assert.True(t, empty1.equalValues(empty2))

	// all of these must have the same length (3)
	xidToKIDMaps := []map[string]uint64{
		{"foo": 3, "bar": 2, "baz": 0},
		{"foo": 2, "bar": 3, "baz": 0},
		{"tic": 2, "tac": 3, "toe": 0},
	}
	for _, m1 := range xidToKIDMaps {
		for _, m2 := range xidToKIDMaps {
			expected := reflect.DeepEqual(m1, m2)
			reads1 := &updateReadSet{index: 1, xidToKID: m1}
			reads2 := &updateReadSet{index: 2, xidToKID: m2}
			actual := reads1.equalValues(reads2)
			assert.Equal(t, expected, actual, "m1: %+v\nm2: %+v", m1, m2)
		}
	}

	// all of these must have the same length (3)
	existingFactIDSlices := [][]uint64{
		{3, 2, 0},
		{2, 3, 0},
		{0, 0, 0},
	}
	for _, s1 := range existingFactIDSlices {
		for _, s2 := range existingFactIDSlices {
			expected := reflect.DeepEqual(s1, s2)
			reads1 := &updateReadSet{index: 1, existingFactIDs: s1}
			reads2 := &updateReadSet{index: 2, existingFactIDs: s2}
			actual := reads1.equalValues(reads2)
			assert.Equal(t, expected, actual, "s1: %+v\ns2: %+v", s1, s2)
		}
	}
}
