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

package impl

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ebay/beam/api"
	"github.com/ebay/beam/blog"
	"github.com/ebay/beam/blog/mockblog"
	"github.com/ebay/beam/logentry"
	"github.com/ebay/beam/logentry/logencoder"
	"github.com/ebay/beam/logentry/logwrite"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/viewclient/lookups/mocklookups"
	"github.com/stretchr/testify/assert"
)

func Test_EqualKeys(t *testing.T) {
	assert.True(t, equalKeys(map[int]uint64{}, map[int]uint64{}))
	assert.True(t, equalKeys(map[int]uint64{1: 1}, map[int]uint64{1: 2}))
	assert.True(t, equalKeys(map[int]uint64{1: 1, 50: 2}, map[int]uint64{1: 2, 50: 3}))
	assert.False(t, equalKeys(map[int]uint64{1: 1, 51: 2}, map[int]uint64{1: 1, 50: 2}))
	assert.False(t, equalKeys(map[int]uint64{1: 1, 51: 2}, map[int]uint64{51: 2}))
}

func Test_ExcludeExisting(t *testing.T) {
	// msg is a helper to generate a InsertTxCommand with the list of FactIDOffsets
	msg := func(offsets ...int32) logentry.InsertTxCommand {
		r := logentry.InsertTxCommand{
			Facts: make([]logentry.InsertFact, len(offsets)),
		}
		for i, offset := range offsets {
			r.Facts[i].FactIDOffset = offset
		}
		return r
	}
	type tc struct {
		in       logentry.InsertTxCommand
		existing map[int]uint64 // offset of Fact in Insert -> existing FactID
		exp      logentry.InsertTxCommand
	}
	tests := []tc{
		{msg(3, 1, 2), map[int]uint64{}, msg(3, 1, 2)},
		{msg(3, 1, 2), map[int]uint64{0: 11}, msg(1, 2)},
		{msg(3, 1, 2), map[int]uint64{1: 11}, msg(3, 2)},
		{msg(3, 1, 2), map[int]uint64{2: 12}, msg(3, 1)},
		{msg(3, 1, 2), map[int]uint64{0: 11, 2: 12}, msg(1)},
		{msg(3, 1, 2), map[int]uint64{0: 11, 1: 11, 2: 12}, msg()},
		{msg(3, 1, 2), map[int]uint64{0: 11, 4: 11}, msg(1, 2)},
	}
	for idx, test := range tests {
		t.Run(fmt.Sprintf("tc[%d]", idx), func(t *testing.T) {
			before := test.in
			filtered := excludeExisting(test.in, test.existing)
			assert.Equal(t, test.exp, filtered)
			assert.Equal(t, before, test.in, "The input InsertTxCommand to excludeExisting should not be mutated")
		})
	}
}

func Test_InsertResults(t *testing.T) {
	apiReq := api.InsertFactsRequest{
		NewSubjectVars: []string{"bob"},
		Facts: []api.InsertFact{
			{
				Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Var{Var: "bob"}},
				Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 4}},
				Object: api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{
					Object: &api.KGObject{Value: &api.KGObject_AString{AString: "Bob"}}}}},
			{
				Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 6}},
				Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 41}},
				Object: api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{
					Object: &api.KGObject{Value: &api.KGObject_AKID{AKID: 7}}}},
			},
			{
				Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 0x5}},
				Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 0x41}},
				Object:    api.KGObjectOrVar{Value: &api.KGObjectOrVar_Var{Var: "bob"}},
			},
		},
	}
	logReq, varOffsets := convertAPIInsertToLogCommand(&apiReq)
	existing := map[int]uint64{1: 1001}
	result := insertResult(&apiReq, &logReq, existing, 2, varOffsets)
	assert.Equal(t, []uint64{2001}, result.VarResults)
	assert.Equal(t, []uint64{2002, 1001, 2004}, result.FactIds)
	assert.Equal(t, int64(2), result.Index)
}

func Test_appendInsertTxBegin(t *testing.T) {
	beamLog := mockblog.New(context.Background())
	cmd := &logentry.InsertTxCommand{
		Facts: []logentry.InsertFact{{
			FactIDOffset: 3,
		}},
	}
	idx, err := appendInsertTxBegin(context.Background(), beamLog, cmd)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), idx)
	beamLog.AssertCommands(t, rpc.LogPosition{
		Index:   1,
		Version: logencoder.DefaultLogVersion,
	}, []logencoder.ProtobufCommand{cmd})
}

func Test_appendInsertTxBegin_reportsError(t *testing.T) {
	beamLog := mockblog.New(context.Background())
	beamLog.SetNextAppendError(errors.New("Unable to append"))
	idx, err := appendInsertTxBegin(context.Background(), beamLog,
		&logentry.InsertTxCommand{})
	assert.EqualError(t, err, "Unable to append")
	assert.Equal(t, uint64(0), idx)
}

func Test_appendTxDecide(t *testing.T) {
	beamLog := mockblog.New(context.Background())
	idx, err := appendTxDecide(context.Background(), beamLog, 5, true)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), idx)
	beamLog.AssertCommands(t, rpc.LogPosition{
		Index:   1,
		Version: logencoder.DefaultLogVersion,
	}, []logencoder.ProtobufCommand{
		&logentry.TxDecisionCommand{Tx: 5, Commit: true},
	})
}

func Test_appendTxDecide_reportsError(t *testing.T) {
	beamLog := mockblog.New(context.Background())
	beamLog.SetNextAppendError(errors.New("Unable to append"))
	idx, err := appendTxDecide(context.Background(), beamLog, 5, true)
	assert.EqualError(t, err, "Unable to append")
	assert.Equal(t, uint64(0), idx)
}

func makeInsertFact(s, p, o uint64) api.InsertFact {
	return api.InsertFact{
		Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: s}},
		Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: p}},
		Object: api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{
			Object: &api.KGObject{Value: &api.KGObject_AKID{AKID: o}}}},
	}
}

func Test_Insert(t *testing.T) {
	lfb := new(logwrite.InsertFactBuilder).SetAutoFactID(false)

	// tc defines a single insert test case. It takes the inbound API request
	// and runs it though InsertFacts using a mock log and a mock LookupSPO
	// endpoint. The test case can control what lookupSPO returns. It will
	// verify both the expected result of InsertFacts, as well as the what
	// LookupSPO requests were made, and what Commands were written to the log
	type tc struct {
		name string
		req  api.InsertFactsRequest
		// if not empty will assert that the insertFacts call returned this error
		expectedError string
		// the expected results of the insertFacts call if there was no error
		expectedResult api.InsertFactsResult
		// the first lookupSPO request returns mockSPO.results[0], the seconds returns [1] etc.
		lookupSPO []mocklookups.Expected
		// the list of commands expected to be written to the log
		expectedLog []logencoder.ProtobufCommand
	}
	// startingLogIndex is the log index the first log entry created by the test will
	// be placed at.
	const startingLogIndex = 5
	tests := []tc{
		{
			name: "Insert should validate the request",
			req: api.InsertFactsRequest{
				NewSubjectVars: []string{"bob"},
				Facts: []api.InsertFact{
					makeInsertFact(1, 2, 3),
				},
			},
			expectedError: "the following variables were declared but not used, all variables must get used: [bob]",
		},
		{
			name: "Insert with no existing facts, no variables",
			req: api.InsertFactsRequest{
				Facts: []api.InsertFact{
					makeInsertFact(1001, 1002, 1003),
					{
						Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 2001}},
						Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 2002}},
						Object: api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{
							Object: &api.KGObject{
								Value:  &api.KGObject_AInt64{AInt64: 42},
								UnitID: 1,
							},
						}},
					},
				},
			},
			expectedResult: api.InsertFactsResult{
				Index:   startingLogIndex,
				FactIds: []uint64{5001, 5002},
			},
			lookupSPO: []mocklookups.Expected{
				mocklookups.OK(&rpc.LookupSPORequest{
					Index: startingLogIndex - 1,
					Lookups: []rpc.LookupSPORequest_Item{
						{Subject: 1001, Predicate: 1002, Object: rpc.AKID(1003)},
						{Subject: 2001, Predicate: 2002, Object: rpc.AInt64(42, 1)},
					},
				},
					// this is a slice with one item in it that's empty, i.e. no facts found
					rpc.LookupChunk{}),
			},
			expectedLog: []logencoder.ProtobufCommand{
				&logentry.InsertTxCommand{
					Facts: []logentry.InsertFact{
						lfb.SID(1001).PID(1002).OKID(1003).FactID(1).Fact(),
						lfb.SID(2001).PID(2002).OInt64(42, 1).FactID(2).Fact(),
					},
				},
				&logentry.TxDecisionCommand{Tx: 5, Commit: true},
			},
		},
		{
			name: "Insert with Variables",
			req: api.InsertFactsRequest{
				NewSubjectVars: []string{"?bob"},
				Facts: []api.InsertFact{
					{
						Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Var{Var: "?bob"}},
						Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 4}},
						Object: api.KGObjectOrVar{Value: &api.KGObjectOrVar_Object{
							Object: &api.KGObject{Value: &api.KGObject_AKID{AKID: 1}},
						}},
					},
					{
						Subject:   api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 123}},
						Predicate: api.KIDOrVar{Value: &api.KIDOrVar_Kid{Kid: 4}},
						Object:    api.KGObjectOrVar{Value: &api.KGObjectOrVar_Var{Var: "?bob"}},
					},
				},
			},
			expectedResult: api.InsertFactsResult{
				Index:      startingLogIndex,
				VarResults: []uint64{5001},
				FactIds:    []uint64{5002, 5003},
			},
			// as all the facts reference a newSubjectVar, they can't
			// possibly exist, so there should be no lookupSPO call made
			lookupSPO: nil,
			expectedLog: []logencoder.ProtobufCommand{
				&logentry.InsertTxCommand{
					Facts: []logentry.InsertFact{
						lfb.SOffset(1).PID(4).OKID(1).FactID(2).Fact(),
						lfb.SID(123).PID(4).OOffset(1).FactID(3).Fact(),
					},
				},
				&logentry.TxDecisionCommand{Tx: 5, Commit: true},
			},
		},
		{
			name: "Insert multiple facts with one existing",
			req: api.InsertFactsRequest{
				Facts: []api.InsertFact{
					makeInsertFact(1001, 1002, 1003),
					makeInsertFact(2001, 2002, 2003), // this one exists and has factID 2004
				},
			},
			expectedResult: api.InsertFactsResult{
				Index:   startingLogIndex + 2,
				FactIds: []uint64{7001, 2004},
			},
			lookupSPO: []mocklookups.Expected{
				mocklookups.OK(
					&rpc.LookupSPORequest{
						Index: startingLogIndex - 1,
						Lookups: []rpc.LookupSPORequest_Item{
							{Subject: 1001, Predicate: 1002, Object: rpc.AKID(1003)},
							{Subject: 2001, Predicate: 2002, Object: rpc.AKID(2003)},
						},
					},
					rpc.LookupChunk{
						Facts: []rpc.LookupChunk_Fact{{
							Lookup: 1,
							Fact:   rpc.Fact{Subject: 2001, Predicate: 2002, Object: rpc.AKID(2003), Index: 2, Id: 2004},
						}},
					},
				),
				mocklookups.OK(
					&rpc.LookupSPORequest{
						Index: startingLogIndex + 1,
						Lookups: []rpc.LookupSPORequest_Item{
							{Subject: 1001, Predicate: 1002, Object: rpc.AKID(1003)},
							{Subject: 2001, Predicate: 2002, Object: rpc.AKID(2003)},
						},
					},
					rpc.LookupChunk{
						Facts: []rpc.LookupChunk_Fact{{
							Lookup: 1,
							Fact:   rpc.Fact{Subject: 2001, Predicate: 2002, Object: rpc.AKID(2003), Index: 2, Id: 2004},
						}},
					},
				),
			},
			expectedLog: []logencoder.ProtobufCommand{
				&logentry.InsertTxCommand{
					Facts: []logentry.InsertFact{
						lfb.SID(1001).PID(1002).OKID(1003).FactID(1).Fact(),
						lfb.SID(2001).PID(2002).OKID(2003).FactID(2).Fact(),
					},
				},
				&logentry.TxDecisionCommand{Tx: startingLogIndex, Commit: false},
				&logentry.InsertTxCommand{
					Facts: []logentry.InsertFact{
						lfb.SID(1001).PID(1002).OKID(1003).FactID(1).Fact(),
					},
				},
				&logentry.TxDecisionCommand{Tx: startingLogIndex + 2, Commit: true},
			},
		},
		{
			name: "Insert where all facts already exist",
			req: api.InsertFactsRequest{
				Facts: []api.InsertFact{
					makeInsertFact(1001, 1002, 1003),
					makeInsertFact(2001, 2002, 2003),
				},
			},
			expectedResult: api.InsertFactsResult{
				Index:   startingLogIndex,
				FactIds: []uint64{1004, 2004},
			},
			lookupSPO: []mocklookups.Expected{
				mocklookups.OK(
					&rpc.LookupSPORequest{
						Index: startingLogIndex - 1,
						Lookups: []rpc.LookupSPORequest_Item{
							{Subject: 1001, Predicate: 1002, Object: rpc.AKID(1003)},
							{Subject: 2001, Predicate: 2002, Object: rpc.AKID(2003)},
						},
					},
					// one result with the 2 existing facts
					rpc.LookupChunk{
						Facts: []rpc.LookupChunk_Fact{{
							Lookup: 0,
							Fact:   rpc.Fact{Subject: 1001, Predicate: 1002, Object: rpc.AKID(1003), Id: 1004, Index: 1},
						}, {
							Lookup: 1,
							Fact:   rpc.Fact{Subject: 2001, Predicate: 2002, Object: rpc.AKID(2003), Id: 2004, Index: 2},
						}},
					},
				),
			},
			expectedLog: []logencoder.ProtobufCommand{
				&logentry.InsertTxCommand{
					Facts: []logentry.InsertFact{
						lfb.SID(1001).PID(1002).OKID(1003).FactID(1).Fact(),
						lfb.SID(2001).PID(2002).OKID(2003).FactID(2).Fact(),
					},
				},
				&logentry.TxDecisionCommand{Tx: startingLogIndex, Commit: false},
			},
		},
		{
			name: "insert that requires more than 1 attempt to get stable existing state",
			req: api.InsertFactsRequest{
				Facts: []api.InsertFact{
					makeInsertFact(1001, 1002, 1003),
					makeInsertFact(2001, 2002, 2003),
				},
			},
			expectedResult: api.InsertFactsResult{
				Index:   startingLogIndex + 4,
				FactIds: []uint64{1004, 9002},
			},
			lookupSPO: []mocklookups.Expected{
				mocklookups.OK(
					&rpc.LookupSPORequest{
						Index: startingLogIndex - 1,
						Lookups: []rpc.LookupSPORequest_Item{
							{Subject: 1001, Predicate: 1002, Object: rpc.AKID(1003)},
							{Subject: 2001, Predicate: 2002, Object: rpc.AKID(2003)},
						},
					},
					rpc.LookupChunk{
						Facts: []rpc.LookupChunk_Fact{{
							Lookup: 1,
							Fact:   rpc.Fact{Subject: 2001, Predicate: 2002, Object: rpc.AKID(2003), Id: 2004, Index: 2},
						}},
					},
				),
				// after finding that this fact exists, it'll loop around
				// and write another insertTx with it removed, and then
				// check again that its still the only existing fact, but in
				// that time other writers could have created and/or deleted
				// facts from the original request. The eventual state should
				// be that Id:1004 exists, and Id:2004 does not.
				mocklookups.OK(
					&rpc.LookupSPORequest{
						Index: startingLogIndex + 1,
						Lookups: []rpc.LookupSPORequest_Item{
							{Subject: 1001, Predicate: 1002, Object: rpc.AKID(1003)},
							{Subject: 2001, Predicate: 2002, Object: rpc.AKID(2003)},
						},
					},
					rpc.LookupChunk{
						Facts: []rpc.LookupChunk_Fact{{
							Lookup: 0,
							Fact:   rpc.Fact{Subject: 1001, Predicate: 1002, Object: rpc.AKID(1003), Id: 1004, Index: 1},
						}},
					},
				),
				mocklookups.OK(
					&rpc.LookupSPORequest{
						Index: startingLogIndex + 3,
						Lookups: []rpc.LookupSPORequest_Item{
							{Subject: 1001, Predicate: 1002, Object: rpc.AKID(1003)},
							{Subject: 2001, Predicate: 2002, Object: rpc.AKID(2003)},
						},
					},
					rpc.LookupChunk{
						Facts: []rpc.LookupChunk_Fact{{
							Lookup: 0,
							Fact:   rpc.Fact{Subject: 1001, Predicate: 1002, Object: rpc.AKID(1003), Id: 1004, Index: 1},
						}},
					},
				),
			},
			expectedLog: []logencoder.ProtobufCommand{
				&logentry.InsertTxCommand{
					Facts: []logentry.InsertFact{
						lfb.SID(1001).PID(1002).OKID(1003).FactID(1).Fact(),
						lfb.SID(2001).PID(2002).OKID(2003).FactID(2).Fact(),
					},
				},
				&logentry.TxDecisionCommand{Tx: startingLogIndex, Commit: false},
				&logentry.InsertTxCommand{
					Facts: []logentry.InsertFact{
						lfb.SID(1001).PID(1002).OKID(1003).FactID(1).Fact(),
					},
				},
				&logentry.TxDecisionCommand{Tx: startingLogIndex + 2, Commit: false},
				&logentry.InsertTxCommand{
					Facts: []logentry.InsertFact{
						lfb.SID(2001).PID(2002).OKID(2003).FactID(2).Fact(),
					},
				},
				&logentry.TxDecisionCommand{Tx: startingLogIndex + 4, Commit: true},
			},
		},
		{
			name: "Abort log entry should be created when lookup fails",
			req: api.InsertFactsRequest{
				Facts: []api.InsertFact{
					makeInsertFact(1001, 1002, 1003),
				},
			},
			expectedError: "unable to lookup existing facts: LookupSPO Failed",
			lookupSPO: []mocklookups.Expected{
				mocklookups.Err(
					&rpc.LookupSPORequest{
						Index: startingLogIndex - 1,
						Lookups: []rpc.LookupSPORequest_Item{
							{Subject: 1001, Predicate: 1002, Object: rpc.AKID(1003)},
						},
					},
					errors.New("LookupSPO Failed"),
				),
			},
			expectedLog: []logencoder.ProtobufCommand{
				&logentry.InsertTxCommand{
					Facts: []logentry.InsertFact{
						lfb.SID(1001).PID(1002).OKID(1003).FactID(1).Fact(),
					},
				},
				&logentry.TxDecisionCommand{Tx: startingLogIndex, Commit: false},
			},
		},
	}
	// this builds a test case where the lookups return different results each time
	// (e.g. if there were high levels of concurrent writes/deletes of the same or
	// overlapping sets of facts). Eventually this insert gives up and returns an error.
	maxAttemptsTestCase := tc{
		name: "Exhaust lookups attempts",
		req: api.InsertFactsRequest{
			Facts: []api.InsertFact{
				makeInsertFact(1001, 1002, 1003),
				makeInsertFact(2001, 2002, 2003),
			},
		},
		expectedError: "insert exhausted all retries (32) while attempting to resolve which facts do/don't exist",
	}
	for i := 0; i < maxInsertRetry; i++ {
		// in reality we'd expect the index of the results to change for each request, but that
		// doesn't affect the processing, so we don't set that up.
		var result rpc.LookupChunk
		if i%2 == 0 {
			result = rpc.LookupChunk{
				Facts: []rpc.LookupChunk_Fact{{
					Lookup: 0,
					Fact:   rpc.Fact{Subject: 1001, Predicate: 1002, Object: rpc.AKID(1003), Id: 1004, Index: 1},
				}},
			}
		} else {
			result = rpc.LookupChunk{
				Facts: []rpc.LookupChunk_Fact{{
					Lookup: 1,
					Fact:   rpc.Fact{Subject: 2001, Predicate: 2002, Object: rpc.AKID(2003), Id: 2004, Index: 2},
				}},
			}
		}
		// every insert attempt consists of a insertTx followed by an decideTx
		// so the read's will be done at intervals of 2. (at least in the test
		// where nothing else is writing).
		maxAttemptsTestCase.lookupSPO = append(maxAttemptsTestCase.lookupSPO,
			mocklookups.OK(
				&rpc.LookupSPORequest{
					Index: startingLogIndex - 1 + 2*uint64(i),
					Lookups: []rpc.LookupSPORequest_Item{
						{Subject: 1001, Predicate: 1002, Object: rpc.AKID(1003)},
						{Subject: 2001, Predicate: 2002, Object: rpc.AKID(2003)},
					},
				},
				result))
	}
	expLog := make([]logencoder.ProtobufCommand, 0, maxInsertRetry)
	firstInsert := logentry.InsertTxCommand{
		Facts: []logentry.InsertFact{
			lfb.SID(1001).PID(1002).OKID(1003).FactID(1).Fact(),
			lfb.SID(2001).PID(2002).OKID(2003).FactID(2).Fact(),
		},
	}
	expLog = append(expLog, &firstInsert)
	expLog = append(expLog, &logentry.TxDecisionCommand{Tx: startingLogIndex, Commit: false})
	// after the first attempt, the log entries will flip between 2 different attempts
	// based on the fliping lookup results
	insertA := logentry.InsertTxCommand{Facts: firstInsert.Facts[1:]}
	insertB := logentry.InsertTxCommand{Facts: firstInsert.Facts[:1]}
	var tx uint64 = startingLogIndex + 2
	for i := 0; i < maxInsertRetry-1; i++ { // -1 because firstInsert will be the first attempt
		if i%2 == 0 {
			expLog = append(expLog, &insertA, &logentry.TxDecisionCommand{Tx: tx})
		} else {
			expLog = append(expLog, &insertB, &logentry.TxDecisionCommand{Tx: tx})
		}
		tx += 2
	}
	maxAttemptsTestCase.expectedLog = expLog
	tests = append(tests, maxAttemptsTestCase)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			beamLog := mockblog.New(ctx)
			assert.NoError(t, beamLog.Discard(ctx, startingLogIndex))
			mockSPO, assertDone := mocklookups.New(t, test.lookupSPO...)
			res, err := insertFacts(ctx, mockSPO, beamLog, &test.req)
			if test.expectedError != "" {
				assert.EqualError(t, err, test.expectedError)
			} else {
				assert.NoError(t, err)
				// assert.Equal doesn't consider []foo{} & nil equal, even though nil
				// is treated by an empty slice by everything. so normalize these up
				// to what the is generated for the output
				if test.expectedResult.VarResults == nil {
					test.expectedResult.VarResults = []uint64{}
				}
				assert.Equal(t, &test.expectedResult, res)
			}
			assertDone()
			beamLog.AssertCommands(t, rpc.LogPosition{
				Index:   startingLogIndex,
				Version: logencoder.DefaultLogVersion,
			}, test.expectedLog)
		})
	}
}

func Test_ResolveIndexConstraint(t *testing.T) {
	type test struct {
		recent        blog.Index
		infoResult    blog.Index
		infoResultErr error
		input         api.LogIndex
		expRecent     blog.Index
		expIndex      blog.Index
		expError      string
		expInfoCalls  int
	}
	infoErr := fmt.Errorf("There was an error fetching Info")
	tests := []test{{
		input:    api.LogIndex{Index: 500, Constraint: api.LogIndexExact},
		expIndex: 500,
	}, {
		recent:       0,
		infoResult:   100,
		input:        api.LogIndex{Constraint: api.LogIndexAtLeast, Index: 50},
		expIndex:     100,
		expRecent:    100,
		expInfoCalls: 1,
	}, {
		recent:       55,
		infoResult:   100,
		input:        api.LogIndex{Constraint: api.LogIndexAtLeast, Index: 50},
		expIndex:     55,
		expRecent:    55,
		expInfoCalls: 0,
	}, {
		recent:       55,
		infoResult:   100,
		input:        api.LogIndex{Constraint: api.LogIndexAtLeast, Index: 60},
		expIndex:     60,
		expRecent:    55,
		expInfoCalls: 0,
	}, {
		recent:       55,
		infoResult:   100,
		input:        api.LogIndex{Constraint: api.LogIndexAtLeast, Index: 0},
		expIndex:     55,
		expRecent:    55,
		expInfoCalls: 0,
	}, {
		recent:       500,
		infoResult:   600,
		input:        api.LogIndex{Constraint: api.LogIndexRecent},
		expIndex:     500,
		expRecent:    500,
		expInfoCalls: 0,
	}, {
		recent:       0,
		infoResult:   600,
		input:        api.LogIndex{Constraint: api.LogIndexRecent},
		expIndex:     600,
		expRecent:    600,
		expInfoCalls: 1,
	}, {
		recent:       999,
		infoResult:   1000,
		input:        api.LogIndex{Constraint: api.LogIndexLatest},
		expIndex:     1000,
		expRecent:    1000,
		expInfoCalls: 1,
	}, {
		input:    api.LogIndex{Constraint: api.LogIndexLatest, Index: 10},
		expError: "a log index can't be specified when using the Latest constraint",
	}, {
		input:    api.LogIndex{Constraint: api.LogIndexRecent, Index: 10},
		expError: "a log index can't be specified when using the Recent constraint",
	}, {
		// a ExactIndex of 0 is valid if the log is actually empty.
		recent:       0,
		infoResult:   0,
		input:        api.LogIndex{Constraint: api.LogIndexExact, Index: 0},
		expIndex:     0,
		expInfoCalls: 1,
	}, {
		recent:       0,
		infoResult:   10,
		input:        api.LogIndex{Constraint: api.LogIndexExact, Index: 0},
		expError:     "a log index constraint of Exact requires a log index to be set",
		expInfoCalls: 1,
	}, {
		input:         api.LogIndex{Constraint: api.LogIndexExact, Index: 11},
		infoResultErr: infoErr,
		expIndex:      11,
	}, {
		recent:        50,
		input:         api.LogIndex{Constraint: api.LogIndexAtLeast, Index: 12},
		infoResultErr: infoErr,
		expIndex:      50,
	}, {
		recent:        5,
		input:         api.LogIndex{Constraint: api.LogIndexAtLeast, Index: 13},
		infoResultErr: infoErr,
		expIndex:      13,
	}, {
		recent:        0,
		input:         api.LogIndex{Constraint: api.LogIndexAtLeast, Index: 14},
		infoResultErr: infoErr,
		expError:      infoErr.Error(),
		expInfoCalls:  1,
	}, {
		recent:        5,
		input:         api.LogIndex{Constraint: api.LogIndexLatest},
		infoResultErr: infoErr,
		expError:      infoErr.Error(),
		expInfoCalls:  1,
	}, {
		input:    api.LogIndex{Constraint: api.LogIndexConstraint(42)},
		expError: "LogIndex constraint of 42 is not supported",
	}}
	for _, test := range tests {
		t.Run(test.input.String(), func(t *testing.T) {
			s := Server{}
			s.locked.recentIndex = test.recent
			log := &mockInfo{latestIndex: test.infoResult, err: test.infoResultErr}
			resIndex, resErr := s.resolveIndexConstraint(context.Background(), log, test.input)
			if test.expError != "" {
				assert.EqualError(t, resErr, test.expError)
			} else {
				assert.NoError(t, resErr)
				assert.Equal(t, test.expIndex, resIndex, "returned Index not correct")
			}
			if test.expRecent != 0 {
				assert.Equal(t, s.locked.recentIndex, test.expRecent, "Resulting value for Recent unexpected")
			}
			assert.Equal(t, test.expInfoCalls, log.infoCalls, "Unexpected number of calls to log.Info()")
		})
	}
}

func Test_FetchLatestLogIndex(t *testing.T) {
	type test struct {
		recent        blog.Index
		infoResult    blog.Index
		infoResultErr error
		expResult     blog.Index
		expErr        string
		expRecent     blog.Index
	}
	tests := []test{{
		recent:     10,
		infoResult: 20,
		expResult:  20,
		expRecent:  20,
	}, {
		recent:     11,
		infoResult: 10,
		expResult:  10,
		expRecent:  11,
	}, {
		recent:        10,
		infoResultErr: fmt.Errorf("Failed to reach log server"),
		expErr:        "Failed to reach log server",
		expRecent:     10,
	}}
	for _, test := range tests {
		s := Server{}
		s.locked.recentIndex = test.recent
		log := &mockInfo{latestIndex: test.infoResult, err: test.infoResultErr}
		res, err := s.fetchLatestLogIndex(context.Background(), log)
		if test.expErr != "" {
			assert.EqualError(t, err, test.expErr)
			continue
		}
		assert.Equal(t, test.expResult, res, "Unexpected result value returned")
		assert.Equal(t, test.expRecent, s.locked.recentIndex, "recent value has unexpected value")
	}
}

type mockInfo struct {
	latestIndex blog.Index
	err         error
	infoCalls   int
}

func (m *mockInfo) Info(context.Context) (*blog.Info, error) {
	m.infoCalls++
	if m.err != nil {
		return nil, m.err
	}
	return &blog.Info{
		LastIndex: m.latestIndex,
	}, nil
}
