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
	"fmt"

	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/rpc"
)

// valueBinder allows for a queryOp to resolve the value bound to the supplied
// binding for 1 or more results. In general a queryOperator will pass a
// valueBinder down to any child queryOps it runs. bulkOffset / length is
// managed as a uint32 to match the Lookup offset reported by the Lookup data
// sources. LoopJoin is the primary example of how multi-row bindings are
// generated/used.
type valueBinder interface {
	// len returns number of factSets/results available for binding (i.e. think
	// rows).
	len() uint32

	// bind returns the result of the binding 'b' for the result at 'bulkOffset'.
	bind(bulkOffset uint32, b *plandef.Binding) *Value
}

// kidOf returns a KID from a FixedTerm, either using the value in the
// term itself or by applying the valueBinder.
func kidOf(bulkIndex uint32, b valueBinder, t plandef.FixedTerm) uint64 {
	switch ft := t.(type) {
	case *plandef.OID:
		return ft.Value
	case *plandef.Literal:
		return ft.Value.ValKID()
	case *plandef.Binding:
		return b.bind(bulkIndex, ft).KGObject.ValKID()
	}
	panic(fmt.Sprintf("Unexpected FixedTerm type: %T %v", t, t))
}

// kgObjectOf returns a KGObject from a FixedTerm, either using the
// value in the term itself or by applying the valueBinder.
func kgObjectOf(bulkIndex uint32, b valueBinder, t plandef.FixedTerm) rpc.KGObject {
	switch ft := t.(type) {
	case *plandef.OID:
		return rpc.AKID(ft.Value)
	case *plandef.Literal:
		return ft.Value
	case *plandef.Binding:
		return b.bind(bulkIndex, ft).KGObject
	}
	panic(fmt.Sprintf("Unexpected FixedTerm type: %T %v", t, t))
}

// defaultBinder is an implementation of valueBinder that contains 1 row, and no
// values to bind.
type defaultBinder struct {
}

func (e *defaultBinder) len() uint32 {
	return 1 // defaultBinder has one row, which are the Terms from the plan.Op
}

func (e *defaultBinder) bind(bulkOffset uint32, b *plandef.Binding) *Value {
	panic(fmt.Sprintf("defaultBinder incorrectly had bind called on it (index=%d, binding=%s)", bulkOffset, b.Var.Name))
}

// binderWithParent provides an implementation of valueBinder that will first look
// for bindings in the child binder, and if it doesn't exist, look for it in the
// parent binder. This is useful for cases like LoopJoin where as the execution
// goes down branches with nested LoopJoin's it needs to accumulate variable values
// from the path down the tree.
type binderWithParent struct {
	parent valueBinder
	child  *ResultChunk
}

func (p *binderWithParent) len() uint32 {
	return p.child.len()
}

func (p *binderWithParent) bind(bulkOffset uint32, b *plandef.Binding) *Value {
	res := p.child.bind(bulkOffset, b)
	if res == nil {
		// the offset stored in the child at the requested bulkOffset indicates the
		// offset into the parent binder it was generated for.
		res = p.parent.bind(p.child.offsets[bulkOffset], b)
	}
	return res
}

// singleRowBinder wraps a valueBinder and exposes it as a single item binding.
type singleRowBinder struct {
	inner valueBinder
	// Indicates which row in the inner binding we're currently bound to.
	bulkOffset uint32
}

func (s *singleRowBinder) len() uint32 {
	return 1
}

func (s *singleRowBinder) bind(offset uint32, b *plandef.Binding) *Value {
	if offset != 0 {
		panic(fmt.Sprintf("singleRowBinder.bind was called with an invalid offset of %d", offset))
	}
	return s.inner.bind(s.bulkOffset, b)
}

// bulkWrapper takes an operator that works on a single binder input item at
// a time and becomes one that understands bulk binding.
//
// Ideally all op's should be bulk native, but this is useful during transition,
// or for items that don't make sense to be executed in bulk (like hash join)
type bulkWrapper struct {
	singleRowOp operator
}

func (b *bulkWrapper) operator() plandef.Operator {
	return b.singleRowOp.operator()
}

func (b *bulkWrapper) columns() Columns {
	return b.singleRowOp.columns()
}

func (b *bulkWrapper) execute(ctx context.Context, binder valueBinder, res results) error {
	if binder.len() == 1 {
		// common case, the bulk binding is actually only for one item, so we can just call the
		// single item version directly with nothing extra to do
		return b.singleRowOp.execute(ctx, binder, res)
	}
	srBinder := singleRowBinder{inner: binder}
	adjuster := resultOffsetAdjuster{delegate: res}
	for bulkOffset := uint32(0); bulkOffset < uint32(binder.len()); bulkOffset++ {
		srBinder.bulkOffset = bulkOffset
		adjuster.bulkOffset = bulkOffset
		err := b.singleRowOp.execute(ctx, &srBinder, adjuster)
		if err != nil {
			return err
		}
	}
	return nil
}

// resultOffsetAdjuster is an implementation of the results interface that will
// update the offset on any received results and pass them on to the delegate.
// It is used by bulkWrapper to ensure that the offsets are correct in its
// results.
type resultOffsetAdjuster struct {
	delegate   results
	bulkOffset uint32
}

func (r resultOffsetAdjuster) add(ctx context.Context, offset uint32, f FactSet, rowValues []Value) {
	r.delegate.add(ctx, r.bulkOffset, f, rowValues)
}

func (r resultOffsetAdjuster) setFinalStatistics(stats FinalStatistics) {
	r.delegate.setFinalStatistics(stats)
}
