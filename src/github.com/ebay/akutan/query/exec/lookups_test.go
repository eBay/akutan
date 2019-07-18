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

package exec

import (
	"errors"
	"testing"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/facts/cache"
	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/viewclient/lookups/mocklookups"
	"github.com/stretchr/testify/assert"
)

// lookupTest defines a single test case for a Lookup or Infer
// operation, run() will execute the test, it handles creating
// a stream of rpc.LookupChunks that are the results of the
// viewclient call, executing the op, and validating the results
type lookupTest struct {
	name string
	// Requests the queryOp is expected to make, and what replies to send it.
	lookups []mocklookups.Expected

	// the plan & binder to build and execute the queryOp from
	plan   plandef.Plan
	binder valueBinder // if nil, will use new(defaultBinder)

	// ResultChunks are aggregated into a single result and compared to this,
	// unless expError is set.
	expResults ResultChunk
	expError   error
}

// all queryOps built by lookupTest are executed at this index.
const lookupTestIndex = uint64(42)

// defaultLookupTestData creates a list of facts, and constructs a set of
// rpc.LookupChunks containing them. Each LookupChunk gets a subset of the
// facts. A []FactSet that also contains the facts is constructed. These
// are used to help construct lookup & infer test cases.
func defaultLookupTestData() ([]rpc.LookupChunk, []FactSet, []rpc.Fact) {
	facts := makeFacts(lookupTestIndex, 100, 40)
	lookupChunks := make([]rpc.LookupChunk, 4)
	factSets := make([]FactSet, len(facts))
	// for the lookup/infer operators the operator itself is not doing the
	// actual selection of facts, thats done downstream by the underlying RPCs so
	// we don't need to test that results are correct based on the operator,
	// just that given a set of inputs, the corresponding set of outputs appear
	for i, f := range facts {
		chunkIdx := i / (len(facts) / len(lookupChunks))
		lookupChunks[chunkIdx].Facts = append(lookupChunks[chunkIdx].Facts, rpc.LookupChunk_Fact{Fact: f})
		factSets[i] = FactSet{Facts: facts[i : i+1]}
	}
	return lookupChunks, factSets, facts
}

func (tc *lookupTest) run(t *testing.T) {
	if len(tc.lookups) == 0 {
		t.Fatal("lookupTest should have the lookups field set")
	}
	if tc.binder == nil {
		tc.binder = new(defaultBinder)
	}
	lookups, assertDone := mocklookups.New(t, tc.lookups...)
	queryOp := buildOperator(ignoreEvents{}, lookupTestIndex, cache.New(), lookups, &tc.plan)
	op := queryOp.(*decoratedOp).op
	res, err := executeOp(t, op, tc.binder)
	assert.Equal(t, tc.expError, err)
	if tc.expError == nil {
		assertResultChunkEqual(t, tc.expResults.sorted(t), res.sorted(t))
	}
	assertDone()
}

func runTestCases(t *testing.T, tests []lookupTest) {
	// create the error test case as well
	tests = append(tests, createErrorTest(tests[0]))

	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

// createErrorTest will create a new lookupTest by taking the base
// test and creating a version that generates an error on the input
// and checks that was returned from the queryOp
func createErrorTest(baseTest lookupTest) lookupTest {
	// baseTest is a shallow copy
	anError := errors.New("A Lookup Call failed")
	baseTest.expError = anError
	baseTest.name += "_Error"
	baseTest.lookups = []mocklookups.Expected{{
		Request:  baseTest.lookups[0].Request,
		Replies:  baseTest.lookups[0].Replies,
		ReplyErr: anError,
	}}
	return baseTest
}

func Test_LookupS(t *testing.T) {
	inputChunks, factsets, facts := defaultLookupTestData()
	tests := []lookupTest{{
		name: "NoBinding",
		plan: plandef.Plan{
			Operator: &plandef.LookupS{
				ID:        &plandef.DontCare{},
				Subject:   &plandef.Literal{Value: rpc.AKID(4321)},
				Predicate: &plandef.DontCare{},
				Object:    varO,
			}},
		expResults: ResultChunk{
			Columns: Columns{varO},
			Values:  objectValuesOf(facts),
			offsets: make([]uint32, len(facts)),
			Facts:   factsets,
		},
		lookups: []mocklookups.Expected{
			mocklookups.OK(&rpc.LookupSRequest{
				Index:    lookupTestIndex,
				Subjects: []uint64{4321},
			}, inputChunks...),
		},
	}, {
		name: "Bindings",
		plan: plandef.Plan{
			Operator: &plandef.LookupS{
				ID:        &plandef.DontCare{},
				Subject:   &plandef.Binding{Var: varS},
				Predicate: &plandef.DontCare{},
				Object:    &plandef.DontCare{},
			},
		},
		binder: newTestBinder(varS, 1234, 1236),
		expResults: ResultChunk{
			Columns: Columns{varS},
			Values:  subjectValuesOf(facts),
			offsets: make([]uint32, len(facts)),
			Facts:   factsets,
		},
		lookups: []mocklookups.Expected{
			mocklookups.OK(&rpc.LookupSRequest{
				Index:    lookupTestIndex,
				Subjects: []uint64{1234, 1236},
			}, inputChunks...),
		},
	}}
	runTestCases(t, tests)
}

func Test_LookupSP(t *testing.T) {
	inputChunks, factsets, facts := defaultLookupTestData()
	tests := []lookupTest{{
		name: "NoBinding",
		plan: plandef.Plan{
			Operator: &plandef.LookupSP{
				ID:        &plandef.DontCare{},
				Subject:   &plandef.OID{Value: 1234},
				Predicate: &plandef.OID{Value: 5678},
				Object:    varO,
			},
		},
		expResults: ResultChunk{
			Columns: Columns{varO},
			Values:  objectValuesOf(facts),
			offsets: make([]uint32, len(facts)),
			Facts:   factsets,
		},
		lookups: []mocklookups.Expected{
			mocklookups.OK(&rpc.LookupSPRequest{
				Index: lookupTestIndex,
				Lookups: []rpc.LookupSPRequest_Item{
					{Subject: 1234, Predicate: 5678},
				},
			}, inputChunks...),
		},
	}, {
		name: "Bindings",
		plan: plandef.Plan{
			Operator: &plandef.LookupSP{
				ID:        &plandef.DontCare{},
				Subject:   &plandef.Binding{Var: varS},
				Predicate: &plandef.Binding{Var: varP},
				Object:    &plandef.DontCare{},
			},
		},
		binder: mergeBinders(t,
			newTestBinder(varS, 1234, 4321),
			newTestBinder(varP, 444, 555),
		),
		expResults: ResultChunk{
			Columns: Columns{varS, varP},
			Values:  zipValues(subjectValuesOf(facts), predicateValuesOf(facts)),
			offsets: make([]uint32, len(facts)),
			Facts:   factsets,
		},
		lookups: []mocklookups.Expected{
			mocklookups.OK(&rpc.LookupSPRequest{
				Index: lookupTestIndex,
				Lookups: []rpc.LookupSPRequest_Item{
					{Subject: 1234, Predicate: 444},
					{Subject: 4321, Predicate: 555},
				},
			}, inputChunks...),
		},
	}}
	runTestCases(t, tests)
}

func Test_LookupSPO(t *testing.T) {
	inputChunks, factsets, facts := defaultLookupTestData()
	tests := []lookupTest{{
		name: "NoBinding",
		plan: plandef.Plan{
			Operator: &plandef.LookupSPO{
				ID:        varI,
				Subject:   &plandef.OID{Value: 33},
				Predicate: &plandef.OID{Value: 44},
				Object:    &plandef.Literal{Value: rpc.AString("Bob", 1)},
			},
		},
		expResults: ResultChunk{
			Columns: Columns{varI},
			Values:  factIDValuesOf(facts),
			offsets: make([]uint32, len(facts)),
			Facts:   factsets,
		},
		lookups: []mocklookups.Expected{
			mocklookups.OK(&rpc.LookupSPORequest{
				Index: lookupTestIndex,
				Lookups: []rpc.LookupSPORequest_Item{{
					Subject:   33,
					Predicate: 44,
					Object:    rpc.AString("Bob", 1),
				}},
			}, inputChunks...),
		},
	}, {
		name: "Binding",
		plan: plandef.Plan{
			Operator: &plandef.LookupSPO{
				ID:        &plandef.DontCare{},
				Subject:   &plandef.Binding{Var: varS},
				Predicate: &plandef.Binding{Var: varP},
				Object:    &plandef.Binding{Var: varO},
			}},
		binder: mergeBinders(t,
			newTestBinder(varS, 123, 999),
			newTestBinder(varP, 456, 888),
			newTestObjBinder(varO, rpc.AString("Bob", 1), rpc.AInt64(777, 2)),
		),
		expResults: ResultChunk{
			Columns: Columns{varS, varP, varO},
			Values: zipValues(
				subjectValuesOf(facts),
				predicateValuesOf(facts),
				objectValuesOf(facts),
			),
			offsets: make([]uint32, len(facts)),
			Facts:   factsets,
		},
		lookups: []mocklookups.Expected{
			mocklookups.OK(&rpc.LookupSPORequest{
				Index: lookupTestIndex,
				Lookups: []rpc.LookupSPORequest_Item{{
					Subject:   123,
					Predicate: 456,
					Object:    rpc.AString("Bob", 1),
				}, {
					Subject:   999,
					Predicate: 888,
					Object:    rpc.AInt64(777, 2),
				}},
			}, inputChunks...),
		},
	}}

	runTestCases(t, tests)
}

func Test_LookupPO(t *testing.T) {
	inputChunks, factsets, facts := defaultLookupTestData()
	tests := []lookupTest{{
		name: "NoBinding",
		plan: plandef.Plan{
			Operator: &plandef.LookupPO{
				ID:        &plandef.DontCare{},
				Subject:   varS,
				Predicate: &plandef.Literal{Value: rpc.AKID(4321)},
				Object:    &plandef.Literal{Value: rpc.AKID(4322)},
			}},
		expResults: ResultChunk{
			Columns: Columns{varS},
			Values:  subjectValuesOf(facts),
			offsets: make([]uint32, len(facts)),
			Facts:   factsets,
		},
		lookups: []mocklookups.Expected{
			mocklookups.OK(&rpc.LookupPORequest{
				Index: lookupTestIndex,
				Lookups: []rpc.LookupPORequest_Item{
					{Predicate: 4321, Object: rpc.AKID(4322)},
				},
			}, inputChunks...),
		},
	}, {
		name: "Bindings",
		plan: plandef.Plan{
			Operator: &plandef.LookupPO{
				ID:        &plandef.DontCare{},
				Subject:   &plandef.DontCare{},
				Predicate: &plandef.Binding{Var: varP},
				Object:    &plandef.Literal{Value: rpc.AKID(4322)},
			},
		},
		binder: newTestBinder(varP, 1234, 1236),
		expResults: ResultChunk{
			Columns: Columns{varP},
			Values:  predicateValuesOf(facts),
			offsets: make([]uint32, len(facts)),
			Facts:   factsets,
		},
		lookups: []mocklookups.Expected{
			mocklookups.OK(&rpc.LookupPORequest{
				Index: lookupTestIndex,
				Lookups: []rpc.LookupPORequest_Item{
					{Predicate: 1234, Object: rpc.AKID(4322)},
					{Predicate: 1236, Object: rpc.AKID(4322)},
				},
			}, inputChunks...),
		},
	}}
	runTestCases(t, tests)
}

func Test_LookupPOCmp(t *testing.T) {
	inputChunks, factsets, facts := defaultLookupTestData()
	tests := []lookupTest{{
		name: "NoBinding",
		plan: plandef.Plan{
			Operator: &plandef.LookupPOCmp{
				ID:        &plandef.DontCare{},
				Subject:   varS,
				Predicate: &plandef.OID{Value: 321},
				Object:    nil,
				Cmp: plandef.SelectClause{
					Comparison: rpc.OpRangeIncInc,
					Literal1:   &plandef.Literal{Value: rpc.AInt64(900, 1)},
					Literal2:   &plandef.Literal{Value: rpc.AInt64(1200, 1)},
				}}},
		expResults: ResultChunk{
			Columns: Columns{varS},
			Values:  subjectValuesOf(facts),
			offsets: make([]uint32, len(facts)),
			Facts:   factsets,
		},
		lookups: []mocklookups.Expected{
			mocklookups.OK(&rpc.LookupPOCmpRequest{
				Index: lookupTestIndex,
				Lookups: []rpc.LookupPOCmpRequest_Item{{
					Predicate: 321,
					Operator:  rpc.OpRangeIncInc,
					Object:    rpc.AInt64(900, 1),
					EndObject: rpc.AInt64(1200, 1),
				}},
			}, inputChunks...),
		},
	}, {
		name: "Bindings",
		plan: plandef.Plan{
			Operator: &plandef.LookupPOCmp{
				ID:        &plandef.DontCare{},
				Subject:   varS,
				Predicate: &plandef.Binding{Var: varP},
				Object:    nil,
				Cmp: plandef.SelectClause{
					Comparison: rpc.OpLess,
					Literal1:   &plandef.Literal{Value: rpc.ATimestampY(2018, 0)},
				},
			},
		},
		binder: newTestBinder(varP, 222, 333),
		expResults: ResultChunk{
			Columns: Columns{varS, varP},
			Values:  zipValues(subjectValuesOf(facts), predicateValuesOf(facts)),
			offsets: make([]uint32, len(facts)),
			Facts:   factsets,
		},
		lookups: []mocklookups.Expected{
			mocklookups.OK(&rpc.LookupPOCmpRequest{
				Index: lookupTestIndex,
				Lookups: []rpc.LookupPOCmpRequest_Item{{
					Predicate: 222,
					Operator:  rpc.OpLess,
					Object:    rpc.ATimestampY(2018, 0),
				}, {
					Predicate: 333,
					Operator:  rpc.OpLess,
					Object:    rpc.ATimestampY(2018, 0),
				}},
			}, inputChunks...),
		},
	}}

	runTestCases(t, tests)
}

// inputOutputForInferTests creates default lookup test data for an infer test.
func inputOutputForInferTests() ([]rpc.LookupChunk, []FactSet, []rpc.Fact) {
	// The InferXX call is going create a 2nd round of requests based on first
	// set of results. We shrink the input to a more manageable size for
	// defining the expected lookup requests.
	inferInput, factSets, facts := defaultLookupTestData()
	inferInput = inferInput[:1]
	inferInput[0].Facts = inferInput[0].Facts[:3]
	return inferInput, factSets[:3], facts
}

func Test_InferSP(t *testing.T) {
	// inferInputs are
	// Subject:101, Predicate:102, Object:111
	// Subject:111, Predicate:112, Object:121
	// Subject:121, Predicate:122, Object:131
	inferInput, factSets, defaultFacts := inputOutputForInferTests()

	tests := []lookupTest{{
		name: "NoBinding",
		plan: plandef.Plan{
			Operator: &plandef.InferSP{
				ID:        &plandef.DontCare{},
				Subject:   &plandef.OID{Value: 222},
				Predicate: &plandef.OID{Value: 333},
				Object:    varO,
			},
		},
		expResults: ResultChunk{
			Columns: Columns{varO},
			Values: []Value{
				kidVal(111), kidVal(121), kidVal(131),
			},
			offsets: []uint32{0, 0, 0},
			Facts:   factSets,
		},
		lookups: []mocklookups.Expected{
			mocklookups.OK(&rpc.LookupSPRequest{
				Index: lookupTestIndex,
				Lookups: []rpc.LookupSPRequest_Item{
					{Subject: 222, Predicate: 333},
				},
			}, inferInput...),
			mocklookups.OK(&rpc.LookupSPRequest{
				Index: lookupTestIndex,
				Lookups: []rpc.LookupSPRequest_Item{
					// InferSP will take the Object field from the result
					// and build the next lookupSP on it
					{Subject: defaultFacts[0].Object.ValKID(), Predicate: 333},
					{Subject: defaultFacts[1].Object.ValKID(), Predicate: 333},
					{Subject: defaultFacts[2].Object.ValKID(), Predicate: 333},
				},
			}, inferInput...),
		},
	}, {
		name: "Bindings",
		plan: plandef.Plan{
			Operator: &plandef.InferSP{
				ID:        &plandef.DontCare{},
				Subject:   &plandef.Binding{Var: varS},
				Predicate: &plandef.Binding{Var: varP},
				Object:    varO,
			},
		},
		expResults: ResultChunk{
			Columns: Columns{varS, varP, varO},
			Values: []Value{
				kidVal(101), kidVal(102), kidVal(111),
				kidVal(111), kidVal(112), kidVal(121),
				kidVal(121), kidVal(122), kidVal(131),
			},
			offsets: []uint32{0, 0, 0},
			Facts:   factSets,
		},
		binder: mergeBinders(t,
			newTestBinder(varS, 111, 222),
			newTestBinder(varP, 333, 444),
		),
		lookups: []mocklookups.Expected{
			mocklookups.OK(&rpc.LookupSPRequest{
				Index: lookupTestIndex,
				Lookups: []rpc.LookupSPRequest_Item{
					{Subject: 111, Predicate: 333},
					{Subject: 222, Predicate: 444},
				},
			}, inferInput...),
			mocklookups.OK(&rpc.LookupSPRequest{
				Index: lookupTestIndex,
				Lookups: []rpc.LookupSPRequest_Item{
					{Subject: defaultFacts[0].Object.ValKID(), Predicate: 333},
					{Subject: defaultFacts[1].Object.ValKID(), Predicate: 333},
					{Subject: defaultFacts[2].Object.ValKID(), Predicate: 333},
				},
			}, inferInput...),
		},
	}}

	runTestCases(t, tests)
}

func Test_InferSPO(t *testing.T) {
	inferInput, _, defaultFacts := inputOutputForInferTests()

	tests := []lookupTest{{
		name: "NoBinding",
		plan: plandef.Plan{
			Operator: &plandef.InferSPO{
				ID:        varI,
				Subject:   &plandef.OID{Value: 3},
				Predicate: &plandef.OID{Value: 4},
				// the inferInput will indicate this search is complete
				Object: &plandef.Literal{Value: defaultFacts[2].Object},
			}},
		expResults: ResultChunk{
			Columns: Columns{varI},
			Values:  []Value{kidVal(defaultFacts[2].Id)},
			offsets: []uint32{0},
			Facts: []FactSet{{
				Facts: []rpc.Fact{{
					Index:     lookupTestIndex,
					Id:        defaultFacts[2].Id,
					Subject:   3,
					Predicate: 4,
					Object:    defaultFacts[2].Object,
				}},
			}},
		},
		lookups: []mocklookups.Expected{
			mocklookups.OK(&rpc.LookupSPRequest{
				Index: lookupTestIndex,
				Lookups: []rpc.LookupSPRequest_Item{
					{Subject: 3, Predicate: 4},
				},
			}, inferInput...),
		},
	}, {
		name: "Binding",
		plan: plandef.Plan{
			Operator: &plandef.InferSPO{
				ID:        &plandef.DontCare{},
				Subject:   &plandef.Binding{Var: varS},
				Predicate: &plandef.Binding{Var: varP},
				Object:    &plandef.Binding{Var: varO},
			}},
		binder: mergeBinders(t,
			newTestBinder(varS, 3, 5),
			newTestBinder(varP, 4, 6),
			newTestBinder(varO, 131, 141),
		),
		expResults: ResultChunk{
			Columns: Columns{varS, varP, varO},
			Values: []Value{
				{KGObject: rpc.AKID(3)}, {KGObject: rpc.AKID(4)}, {KGObject: defaultFacts[2].Object},
			},
			offsets: []uint32{0},
			Facts: []FactSet{{
				Facts: []rpc.Fact{{
					Index:     lookupTestIndex,
					Id:        defaultFacts[2].Id,
					Subject:   3,
					Predicate: 4,
					Object:    defaultFacts[2].Object,
				}},
			}},
		},
		lookups: []mocklookups.Expected{
			mocklookups.OK(&rpc.LookupSPRequest{
				Index: lookupTestIndex,
				Lookups: []rpc.LookupSPRequest_Item{
					{Subject: 3, Predicate: 4},
					{Subject: 5, Predicate: 6},
				},
			}, inferInput...),
		},
	}}

	runTestCases(t, tests)
}
func Test_InferPO(t *testing.T) {
	// inferInputs are
	// Subject:101, Predicate:102, Object:111
	// Subject:111, Predicate:112, Object:121
	// Subject:121, Predicate:122, Object:131
	inferInput, factSets, defaultFacts := inputOutputForInferTests()

	tests := []lookupTest{{
		name: "NoBinding",
		plan: plandef.Plan{
			Operator: &plandef.InferPO{
				ID:      &plandef.DontCare{},
				Subject: varS,
				// These values don't mater, as they're not evaluated, the
				// fixed input is replayed into the op.
				Predicate: &plandef.Literal{Value: rpc.AKID(4321)},
				Object:    &plandef.Literal{Value: rpc.AKID(4322)},
			}},
		expResults: ResultChunk{
			Columns: Columns{varS},
			Values: []Value{
				{KGObject: rpc.AKID(101)},
				{KGObject: rpc.AKID(111)},
				{KGObject: rpc.AKID(121)},
			},
			offsets: []uint32{0, 0, 0},
			Facts:   factSets,
		},
		lookups: []mocklookups.Expected{
			mocklookups.OK(&rpc.LookupPORequest{
				Index: lookupTestIndex,
				Lookups: []rpc.LookupPORequest_Item{
					{Predicate: 4321, Object: rpc.AKID(4322)},
				},
			}, inferInput...),
			mocklookups.OK(&rpc.LookupPORequest{
				Index: lookupTestIndex,
				// InferPO will take the subjects from the result of the above
				// call and use them to make another LookupPO call
				Lookups: []rpc.LookupPORequest_Item{
					{Predicate: 4321, Object: rpc.AKID(defaultFacts[0].Subject)},
					{Predicate: 4321, Object: rpc.AKID(defaultFacts[1].Subject)},
					{Predicate: 4321, Object: rpc.AKID(defaultFacts[2].Subject)},
				},
			}, inferInput...),
		},
	}, {
		name: "Bindings",
		plan: plandef.Plan{
			Operator: &plandef.InferPO{
				ID:        &plandef.DontCare{},
				Subject:   varS,
				Predicate: &plandef.Binding{Var: varP},
				Object:    &plandef.Literal{Value: rpc.AKID(4322)},
			},
		},
		expResults: ResultChunk{
			Columns: Columns{varS, varP},
			Values: []Value{
				kidVal(101), kidVal(102),
				kidVal(111), kidVal(112),
				kidVal(121), kidVal(122),
			},
			offsets: []uint32{0, 0, 0},
			Facts:   factSets,
		},
		binder: newTestBinder(varP, 1234, 1236),
		lookups: []mocklookups.Expected{
			mocklookups.OK(&rpc.LookupPORequest{
				Index: lookupTestIndex,
				Lookups: []rpc.LookupPORequest_Item{
					{Predicate: 1234, Object: rpc.AKID(4322)},
					{Predicate: 1236, Object: rpc.AKID(4322)},
				},
			}, inferInput...),
			mocklookups.OK(&rpc.LookupPORequest{
				Index: lookupTestIndex,
				Lookups: []rpc.LookupPORequest_Item{
					{Predicate: 1234, Object: rpc.AKID(defaultFacts[0].Subject)},
					{Predicate: 1234, Object: rpc.AKID(defaultFacts[1].Subject)},
					{Predicate: 1234, Object: rpc.AKID(defaultFacts[2].Subject)},
				},
			}, inferInput...),
		},
	}}
	runTestCases(t, tests)
}

func makeFacts(index blog.Index, firstID uint64, count int) []rpc.Fact {
	res := make([]rpc.Fact, count)
	for i := range res {
		res[i] = rpc.Fact{Index: index, Id: firstID, Subject: firstID + 1, Predicate: firstID + 2, Object: rpc.AKID(firstID + 11)}
		firstID += 10
	}
	return res
}

func Test_ConvertAndFwdChunks(t *testing.T) {
	type testCase struct {
		name string
		in   []*rpc.LookupChunk
		vars variableSet
		exp  ResultChunk
	}
	firstID := uint64(100)
	facts := makeFacts(1, firstID, 51)
	cases := []testCase{{
		name: "ManyChunksIntoOneResult",
		in: []*rpc.LookupChunk{{
			Facts: []rpc.LookupChunk_Fact{
				{Lookup: 0, Fact: facts[0]},
				{Lookup: 0, Fact: facts[1]},
				{Lookup: 0, Fact: facts[2]},
			}}, {
			Facts: []rpc.LookupChunk_Fact{
				{Lookup: 0, Fact: facts[3]},
			}},
		},
		vars: variableSet{varI, nil, nil, nil},
		exp: ResultChunk{
			Columns: Columns{varI},
			Values: []Value{
				{KGObject: rpc.AKID(facts[0].Id)},
				{KGObject: rpc.AKID(facts[1].Id)},
				{KGObject: rpc.AKID(facts[2].Id)},
				{KGObject: rpc.AKID(facts[3].Id)},
			},
			offsets: []uint32{0, 0, 0, 0},
			Facts: []FactSet{
				{Facts: facts[:1]},
				{Facts: facts[1:2]},
				{Facts: facts[2:3]},
				{Facts: facts[3:4]},
			},
		}}, {
		name: "NothingToDo",
		in:   nil,
		vars: variableSet{nil, nil, nil, nil},
		exp:  ResultChunk{},
	}, {
		name: "ManyLookups",
		in: []*rpc.LookupChunk{{
			Facts: []rpc.LookupChunk_Fact{
				{Lookup: 0, Fact: facts[0]},
				{Lookup: 1, Fact: facts[1]},
				{Lookup: 2, Fact: facts[2]},
				{Lookup: 0, Fact: facts[3]},
				{Lookup: 1, Fact: facts[4]},
				{Lookup: 2, Fact: facts[5]},
				{Lookup: 0, Fact: facts[6]},
				{Lookup: 1, Fact: facts[7]},
				{Lookup: 2, Fact: facts[8]},
			},
		}},
		vars: variableSet{nil, varS, nil, nil},
		exp: ResultChunk{
			Columns: Columns{varS},
			Values: []Value{
				{KGObject: rpc.AKID(facts[0].Subject)},
				{KGObject: rpc.AKID(facts[1].Subject)},
				{KGObject: rpc.AKID(facts[2].Subject)},
				{KGObject: rpc.AKID(facts[3].Subject)},
				{KGObject: rpc.AKID(facts[4].Subject)},
				{KGObject: rpc.AKID(facts[5].Subject)},
				{KGObject: rpc.AKID(facts[6].Subject)},
				{KGObject: rpc.AKID(facts[7].Subject)},
				{KGObject: rpc.AKID(facts[8].Subject)},
			},
			offsets: []uint32{0, 1, 2, 0, 1, 2, 0, 1, 2},
			Facts: []FactSet{
				{Facts: facts[0:1]},
				{Facts: facts[1:2]},
				{Facts: facts[2:3]},
				{Facts: facts[3:4]},
				{Facts: facts[4:5]},
				{Facts: facts[5:6]},
				{Facts: facts[6:7]},
				{Facts: facts[7:8]},
				{Facts: facts[8:9]},
			}},
	}}
	manyFacts := testCase{
		name: "ManyFacts",
		vars: variableSet{nil, nil, varP, nil},
		exp: ResultChunk{
			Columns: Columns{varP},
		},
	}
	for i := 0; i < 50; i++ {
		manyFacts.in = append(manyFacts.in, &rpc.LookupChunk{Facts: []rpc.LookupChunk_Fact{{Lookup: 1, Fact: facts[i]}}})
		fs := FactSet{
			Facts: facts[i : i+1],
		}
		manyFacts.exp.Facts = append(manyFacts.exp.Facts, fs)
		manyFacts.exp.offsets = append(manyFacts.exp.offsets, 1)
		manyFacts.exp.Values = append(manyFacts.exp.Values, Value{KGObject: rpc.AKID(facts[i].Predicate)})
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			inputCh := make(chan *rpc.LookupChunk, 2)
			results := resultsCollector{}
			go func() {
				for _, chunk := range testCase.in {
					inputCh <- chunk
				}
				close(inputCh)
			}()
			convertAndFwdChunks(ctx, testCase.vars, inputCh, &results)()
			assert.Equal(t, testCase.exp.Facts, results.facts)
			assert.Equal(t, testCase.exp.offsets, results.offsets)
			vals := make([]Value, 0, len(testCase.exp.Values))
			for _, r := range results.rows {
				vals = append(vals, r...)
			}
			assert.Equal(t, len(testCase.exp.Values), len(vals), "unexpected number of result rows")
			// assert.Equal(exp.Values, vals) treats a nil slice and am empty
			// slice as different, but we don't care
			if len(testCase.exp.Values) == 0 {
				assert.Equal(t, 0, len(vals))
			} else {
				assert.Equal(t, testCase.exp.Values, vals)
			}
		})
	}
}
