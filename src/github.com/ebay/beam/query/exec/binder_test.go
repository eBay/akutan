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
	"context"
	"errors"
	"testing"

	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_KidOf(t *testing.T) {
	bob := rpc.AKID(19)
	oid := &plandef.OID{Value: 2}
	lit := &plandef.Literal{Value: bob}
	bind := &plandef.Binding{Var: &plandef.Variable{Name: "b"}}
	binder := newTestBinder(bind.Var, 3)

	assert.Equal(t, uint64(2), kidOf(0, binder, oid))
	assert.Equal(t, uint64(19), kidOf(0, binder, lit))
	assert.Equal(t, uint64(3), kidOf(0, binder, bind))

	binder = newTestBinder(bind.Var, 3, 4)
	assert.Equal(t, uint64(3), kidOf(0, binder, bind))
	assert.Equal(t, uint64(4), kidOf(1, binder, bind))
}

func Test_BindingTypes(t *testing.T) {
	// catch any new FixedTerm types that aren't handled
	binder := new(defaultBinder)
	for _, ft := range plandef.ImplementFixedTerm {
		if _, isBinding := ft.(*plandef.Binding); isBinding {
			continue // already explicitly tested, and using an empty Binding will panic
		}
		if _, isLiteral := ft.(*plandef.Literal); isLiteral {
			continue // already explicitly tested and using an empty Literal will panic
		}
		assert.NotPanics(t, func() { kidOf(0, binder, ft) }, "Got panic for kidOf with FixedTerm type: %T", ft)
		assert.NotPanics(t, func() { kgObjectOf(0, binder, ft) }, "Got panic for kgObjectOf with FixedTerm type: %T", ft)
	}
}

func Test_KGObjectOf(t *testing.T) {
	bob := rpc.AString("Bob", 0)
	oid := &plandef.OID{Value: 2}
	lit := &plandef.Literal{Value: bob}
	bind := &plandef.Binding{Var: &plandef.Variable{Name: "b"}}
	bind2 := &plandef.Binding{Var: &plandef.Variable{Name: "x"}}
	binder := newTestObjBinder(bind.Var, rpc.AString("Alice", 1))

	assert.Equal(t, rpc.AKID(2), kgObjectOf(0, binder, oid))
	assert.Equal(t, bob, kgObjectOf(0, binder, lit))
	assert.Equal(t, rpc.AString("Alice", 1), kgObjectOf(0, binder, bind))

	binder = mergeBinders(t,
		newTestObjBinder(bind.Var, rpc.AString("Eve", 1), rpc.AString("Alice", 1)),
		newTestObjBinder(bind2.Var, rpc.AString("xRow0", 1), rpc.AString("xRow1", 1)),
	)

	assert.Equal(t, rpc.AKID(2), kgObjectOf(0, binder, oid))
	assert.Equal(t, bob, kgObjectOf(0, binder, lit))
	assert.Equal(t, rpc.AKID(2), kgObjectOf(1, binder, oid))
	assert.Equal(t, bob, kgObjectOf(1, binder, lit))
	assert.Equal(t, rpc.AString("Eve", 1), kgObjectOf(0, binder, bind))
	assert.Equal(t, rpc.AString("Alice", 1), kgObjectOf(1, binder, bind))
	assert.Equal(t, rpc.AString("xRow0", 1), kgObjectOf(0, binder, bind2))
	assert.Equal(t, rpc.AString("xRow1", 1), kgObjectOf(1, binder, bind2))
}

func Test_DefaultBinder(t *testing.T) {
	b := new(defaultBinder)
	assert.Equal(t, uint32(1), b.len())
	assert.Panics(t, func() { b.bind(0, &plandef.Binding{Var: &plandef.Variable{Name: "x"}}) })
}

func Test_SingleRowBinder(t *testing.T) {
	b := &plandef.Binding{Var: varP}
	innerBinder := newTestBinder(varP, 1, 2, 3)
	srBinder := singleRowBinder{inner: innerBinder}
	assert.Equal(t, uint32(1), srBinder.len())
	assert.True(t, innerBinder.len() > srBinder.len())
	assert.Equal(t, uint64(1), srBinder.bind(0, b).KGObject.ValKID())

	srBinder.bulkOffset = 1
	assert.Equal(t, uint32(1), srBinder.len())
	assert.Equal(t, uint64(2), srBinder.bind(0, b).KGObject.ValKID())

	srBinder.bulkOffset = 2
	assert.Equal(t, uint32(1), srBinder.len())
	assert.Equal(t, uint64(3), srBinder.bind(0, b).KGObject.ValKID())

	assert.Panics(t, func() { srBinder.bind(1, b) })
}

func Test_BulkWrapper(t *testing.T) {
	binding := &plandef.Binding{Var: varP}
	queryOp := &mockOp{
		exec: func(ctx context.Context, binder valueBinder, rb results) error {
			assert.NotNil(t, ctx)
			assert.NotNil(t, binder)
			assert.NotNil(t, rb)
			assert.Equal(t, uint32(1), binder.len())
			f := rpc.Fact{Subject: kidOf(0, binder, binding)}
			rb.add(ctx, 0, FactSet{Facts: []rpc.Fact{f}}, []Value{{KGObject: rpc.AKID(f.Subject)}})
			return nil
		},
	}
	bulkOp := bulkWrapper{queryOp}
	results := resultsCollector{}
	binder := newTestBinder(binding.Var, 1, 2, 3)
	err := bulkOp.execute(ctx, binder, &results)
	assert.Nil(t, err)
	require.Equal(t, 3, len(results.rows))
	for i, r := range results.rows {
		assert.Equal(t, 1, len(results.facts[i].Facts))
		assert.Equal(t, uint32(i), results.offsets[i])
		assert.Equal(t, uint64(i+1), results.facts[i].Facts[0].Subject)
		assert.Equal(t, uint64(i+1), r[0].KGObject.ValKID())
	}

	results = resultsCollector{}
	binder = newTestBinder(binding.Var, 4)
	err = bulkOp.execute(context.Background(), binder, &results)
	assert.NoError(t, err)
	require.Len(t, results.rows, 1)
	assert.Len(t, results.facts[0].Facts, 1)
	assert.Equal(t, uint32(0), results.offsets[0])
	assert.Equal(t, uint64(4), results.facts[0].Facts[0].Subject)
}

func Test_BulkWrapperError(t *testing.T) {
	opFailedErr := errors.New("it failed")
	opCallCount := 0
	errorOp := &mockOp{
		exec: func(context.Context, valueBinder, results) error {
			opCallCount++
			return opFailedErr
		},
	}
	binder := newTestBinder(varP, 1, 2, 3)
	bulkOp := bulkWrapper{errorOp}
	results := resultsCollector{}
	err := bulkOp.execute(context.Background(), binder, &results)
	assert.Equal(t, opFailedErr, err)
	assert.Empty(t, results.rows)
	assert.Equal(t, 1, opCallCount)
}

func Test_BinderWithParent(t *testing.T) {
	// imagine the case where LoopJoin was passed a binder with multiple rows,
	// and in turn left generates a chunk for different parent binder offsets
	bindO := &plandef.Binding{Var: varO}
	bindP := &plandef.Binding{Var: varP}
	bindS := &plandef.Binding{Var: varS}
	parent := newTestBinder(varP, 10, 20)
	child1 := ResultChunk{
		Columns: Columns{varO},
		Values: []Value{
			{KGObject: rpc.AKID(100)},
			{KGObject: rpc.AKID(200)},
			{KGObject: rpc.AKID(300)},
			{KGObject: rpc.AKID(400)},
		},
		offsets: []uint32{0, 1, 1, 0},
	}
	b := &binderWithParent{parent: parent, child: &child1}
	assert.Equal(t, uint32(4), b.len())
	assert.Equal(t, uint64(100), b.bind(0, bindO).KGObject.ValKID())
	assert.Equal(t, uint64(200), b.bind(1, bindO).KGObject.ValKID())
	assert.Equal(t, uint64(300), b.bind(2, bindO).KGObject.ValKID())
	assert.Equal(t, uint64(400), b.bind(3, bindO).KGObject.ValKID())
	assert.Equal(t, uint64(10), b.bind(0, bindP).KGObject.ValKID())
	assert.Equal(t, uint64(20), b.bind(1, bindP).KGObject.ValKID())
	assert.Equal(t, uint64(20), b.bind(2, bindP).KGObject.ValKID())
	assert.Equal(t, uint64(10), b.bind(3, bindP).KGObject.ValKID())
	assert.Nil(t, b.bind(0, bindS))
}

type testBinder struct {
	vars []*plandef.Variable
	rows [][]Value // [row][varIdx]
}

func (t testBinder) len() uint32 {
	return uint32(len(t.rows))
}

func (t testBinder) bind(o uint32, b *plandef.Binding) *Value {
	for i, v := range t.vars {
		if b.Var == v {
			return &t.rows[o][i]
		}
	}
	return nil
}

// newTestBinder will create a testBinder that has a row for each val
// provided associated with planVar
func newTestBinder(planVar *plandef.Variable, vals ...uint64) testBinder {
	objs := make([]rpc.KGObject, len(vals))
	for i, v := range vals {
		objs[i] = rpc.AKID(v)
	}
	return newTestObjBinder(planVar, objs...)
}

// newTestObjBinder will create a testBinder that has a row for each KGObject val
// provided associated with planVar
func newTestObjBinder(planVar *plandef.Variable, vals ...rpc.KGObject) testBinder {
	b := testBinder{
		vars: []*plandef.Variable{planVar},
		rows: make([][]Value, len(vals)),
	}
	for i, v := range vals {
		b.rows[i] = []Value{{KGObject: v}}
	}
	return b
}

// mergeBinders will merge the supplied binders into a single binder, creating
// combined variables results for each row, for example
// mergeBinders(newTestBinder(varP, 111), newTestBinder(varS, 222))
// will return a new testBinder that has 1 row where the row has both varP & varS as variables.
func mergeBinders(t *testing.T, a, b testBinder, more ...testBinder) testBinder {
	require.Equal(t, len(a.rows), len(b.rows), "testBinders passed to mergeBinders should all be the same length")
	res := testBinder{
		vars: append(a.vars, b.vars...),
		rows: make([][]Value, len(a.rows)),
	}
	for i := range a.rows {
		res.rows[i] = append(a.rows[i], b.rows[i]...)
	}
	if len(more) > 0 {
		res = mergeBinders(t, res, more[0], more[1:]...)
	}
	return res
}
