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

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/facts/cache"
	"github.com/ebay/akutan/infer"
	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/parallel"
	"github.com/ebay/akutan/viewclient/lookups"
)

// all the InferX and LookupX operators are leaf nodes and geneate a FactSet of 1 fact
// for each fact they find. The bulk of this code is dealing with taking the plan/binding
// input and generating a request that can be passed onto viewclient.

const bulkCountTag = "bulk_count"

func newInferPO(index uint64, lookup lookups.PO, op *plandef.InferPO, inputs []queryOperator) operator {
	if len(inputs) > 0 {
		panic(fmt.Sprintf("inferPO operation with unexpected inputs: %v", len(inputs)))
	}
	return &inferPO{
		baseLookup: baseLookup{
			def:   op,
			index: index,
			vars:  variableSetFromTerms(op.Terms()),
		},
		def:    op,
		lookup: lookup,
	}
}

type inferPO struct {
	baseLookup
	def    *plandef.InferPO
	lookup lookups.PO
}

// baseLookup handles implementations of columns() & operator() for all
// lookup/infer operators.
type baseLookup struct {
	def   plandef.Operator
	index blog.Index
	vars  variableSet
}

func (l *baseLookup) columns() Columns {
	return l.vars.columns()
}

func (l *baseLookup) operator() plandef.Operator {
	return l.def
}

func (i *inferPO) execute(ctx context.Context, binder valueBinder, res results) error {
	req := infer.PORequest{
		Index:   i.index,
		Lookups: make([]infer.PORequestItem, binder.len()),
	}
	for bulkOffset := uint32(0); bulkOffset < binder.len(); bulkOffset++ {
		req.Lookups[bulkOffset] = infer.PORequestItem{
			Predicate: kidOf(bulkOffset, binder, i.def.Predicate),
			Object:    kgObjectOf(bulkOffset, binder, i.def.Object),
		}
	}
	inferResCh := make(chan *rpc.LookupChunk, 4)
	wait := parallel.Go(convertAndFwdChunks(ctx, i.vars, inferResCh, res))
	err := infer.PO(ctx, i.lookup, &req, inferResCh)
	wait()
	return err
}

func newInferSP(index uint64, lookup lookups.SP, op *plandef.InferSP, inputs []queryOperator) operator {
	if len(inputs) > 0 {
		panic(fmt.Sprintf("inferSP operation with unexpected inputs: %v", len(inputs)))
	}
	return &inferSP{
		baseLookup: baseLookup{
			def:   op,
			index: index,
			vars:  variableSetFromTerms(op.Terms()),
		},
		def:    op,
		lookup: lookup,
	}
}

type inferSP struct {
	baseLookup
	def    *plandef.InferSP
	lookup lookups.SP
}

func (i *inferSP) execute(ctx context.Context, binder valueBinder, res results) error {
	req := infer.SPRequest{
		Index:   i.index,
		Lookups: make([]infer.SPRequestItem, binder.len()),
	}
	for bulkOffset := uint32(0); bulkOffset < binder.len(); bulkOffset++ {
		req.Lookups[bulkOffset] = infer.SPRequestItem{
			Subject:   kidOf(bulkOffset, binder, i.def.Subject),
			Predicate: kidOf(bulkOffset, binder, i.def.Predicate),
		}
	}
	inferResCh := make(chan *rpc.LookupChunk, 4)
	wait := parallel.Go(convertAndFwdChunks(ctx, i.vars, inferResCh, res))
	err := infer.SP(ctx, i.lookup, &req, inferResCh)
	wait()
	return err
}

func newInferSPO(index blog.Index, lookup lookups.SP, cache cache.FactCache, op *plandef.InferSPO, inputs []queryOperator) operator {
	if len(inputs) > 0 {
		panic(fmt.Sprintf("inferSPO operation with unexpected inputs: %v", len(inputs)))
	}
	return &inferSPO{
		baseLookup: baseLookup{
			def:   op,
			index: index,
			vars:  variableSetFromTerms(op.Terms()),
		},
		cache:  cache,
		def:    op,
		lookup: lookup,
	}
}

type inferSPO struct {
	baseLookup
	cache  cache.FactCache
	def    *plandef.InferSPO
	lookup lookups.SP
}

func (i *inferSPO) execute(ctx context.Context, binder valueBinder, res results) error {
	req := infer.SPORequest{
		Index:   i.index,
		Lookups: make([]infer.SPORequestItem, binder.len()),
	}
	for bulkOffset := uint32(0); bulkOffset < binder.len(); bulkOffset++ {
		req.Lookups[bulkOffset] = infer.SPORequestItem{
			Subject:   kidOf(bulkOffset, binder, i.def.Subject),
			Predicate: kidOf(bulkOffset, binder, i.def.Predicate),
			Object:    kgObjectOf(bulkOffset, binder, i.def.Object),
		}
	}
	inferResCh := make(chan *rpc.LookupChunk, 4)
	wait := parallel.Go(convertAndFwdChunks(ctx, i.vars, inferResCh, res))
	err := infer.SPO(ctx, i.lookup, i.cache, &req, inferResCh)
	wait()
	return err
}

func newLookupPO(index blog.Index, lookup lookups.PO, op *plandef.LookupPO, inputs []queryOperator) operator {
	if len(inputs) > 0 {
		panic(fmt.Sprintf("lookupPO operation with unexpected inputs: %v", len(inputs)))
	}
	return &lookupPO{
		baseLookup: baseLookup{
			def:   op,
			index: index,
			vars:  variableSetFromTerms(op.Terms()),
		},
		def:    op,
		lookup: lookup,
	}
}

type lookupPO struct {
	baseLookup
	def    *plandef.LookupPO
	lookup lookups.PO
}

func (l *lookupPO) execute(ctx context.Context, binder valueBinder, res results) error {
	req := rpc.LookupPORequest{
		Index:   l.index,
		Lookups: make([]rpc.LookupPORequest_Item, binder.len()),
	}
	for bulkOffset := uint32(0); bulkOffset < binder.len(); bulkOffset++ {
		req.Lookups[bulkOffset] = rpc.LookupPORequest_Item{
			Predicate: kidOf(bulkOffset, binder, l.def.Predicate),
			Object:    kgObjectOf(bulkOffset, binder, l.def.Object),
		}
	}
	lookupResCh := make(chan *rpc.LookupChunk, 4)
	wait := parallel.Go(convertAndFwdChunks(ctx, l.vars, lookupResCh, res))
	err := l.lookup.LookupPO(ctx, &req, lookupResCh)
	wait()
	return err
}

func newLookupPOCmp(index blog.Index, lookup lookups.POCmp, op *plandef.LookupPOCmp, inputs []queryOperator) operator {
	if len(inputs) > 0 {
		panic(fmt.Sprintf("lookupPOCmp operation with unexpected inputs: %v", len(inputs)))
	}
	return &lookupPOCmp{
		baseLookup: baseLookup{
			def:   op,
			index: index,
			// given the current defintion of plandef.SelectClause the 2 object values are literals from the query
			// if we want to be able to bind these at some point, we'll have to update op.Terms() & variableSet
			// to have an extra slot for the 2nd object
			vars: variableSetFromTerms(op.Terms()),
		},
		def:    op,
		lookup: lookup,
	}
}

type lookupPOCmp struct {
	baseLookup
	def    *plandef.LookupPOCmp
	lookup lookups.POCmp
}

func (l *lookupPOCmp) execute(ctx context.Context, binder valueBinder, res results) error {
	req := rpc.LookupPOCmpRequest{
		Index:   l.index,
		Lookups: make([]rpc.LookupPOCmpRequest_Item, binder.len()),
	}
	for bulkOffset := uint32(0); bulkOffset < binder.len(); bulkOffset++ {
		req.Lookups[bulkOffset] = rpc.LookupPOCmpRequest_Item{
			Predicate: kidOf(bulkOffset, binder, l.def.Predicate),
			Operator:  l.def.Cmp.Comparison,
			Object:    l.def.Cmp.Literal1.Value,
		}
		if l.def.Cmp.Literal2 != nil {
			req.Lookups[bulkOffset].EndObject = l.def.Cmp.Literal2.Value
		}
	}
	lookupResCh := make(chan *rpc.LookupChunk, 4)
	wait := parallel.Go(convertAndFwdChunks(ctx, l.vars, lookupResCh, res))
	err := l.lookup.LookupPOCmp(ctx, &req, lookupResCh)
	wait()
	return err
}

func newLookupS(index blog.Index, lookup lookups.S, op *plandef.LookupS, inputs []queryOperator) operator {
	if len(inputs) > 0 {
		panic(fmt.Sprintf("lookupS operation with unexpected inputs: %v", len(inputs)))
	}
	return &lookupS{
		baseLookup: baseLookup{
			def:   op,
			index: index,
			vars:  variableSetFromTerms(op.Terms()),
		},
		def:    op,
		lookup: lookup,
	}
}

type lookupS struct {
	baseLookup
	def    *plandef.LookupS
	lookup lookups.S
}

func (l *lookupS) execute(ctx context.Context, binder valueBinder, res results) error {
	req := rpc.LookupSRequest{
		Index:    l.index,
		Subjects: make([]uint64, binder.len()),
	}
	for bulkOffset := uint32(0); bulkOffset < binder.len(); bulkOffset++ {
		req.Subjects[bulkOffset] = kidOf(bulkOffset, binder, l.def.Subject)
	}

	lookupResCh := make(chan *rpc.LookupChunk, 4)
	wait := parallel.Go(convertAndFwdChunks(ctx, l.vars, lookupResCh, res))
	err := l.lookup.LookupS(ctx, &req, lookupResCh)
	wait()
	return err
}

func newLookupSP(index blog.Index, lookup lookups.SP, op *plandef.LookupSP, inputs []queryOperator) operator {
	if len(inputs) > 0 {
		panic(fmt.Sprintf("lookupSP operation with unexpected inputs: %v", len(inputs)))
	}
	return &lookupSP{
		baseLookup: baseLookup{
			def:   op,
			index: index,
			vars:  variableSetFromTerms(op.Terms()),
		},
		def:    op,
		lookup: lookup,
	}
}

type lookupSP struct {
	baseLookup
	def    *plandef.LookupSP
	lookup lookups.SP
}

func (l *lookupSP) execute(ctx context.Context, binder valueBinder, res results) error {
	req := rpc.LookupSPRequest{
		Index:   l.index,
		Lookups: make([]rpc.LookupSPRequest_Item, binder.len()),
	}
	for bulkOffset := uint32(0); bulkOffset < binder.len(); bulkOffset++ {
		req.Lookups[bulkOffset] = rpc.LookupSPRequest_Item{
			Subject:   kidOf(bulkOffset, binder, l.def.Subject),
			Predicate: kidOf(bulkOffset, binder, l.def.Predicate),
		}
	}
	lookupResCh := make(chan *rpc.LookupChunk, 4)
	wait := parallel.Go(convertAndFwdChunks(ctx, l.vars, lookupResCh, res))
	err := l.lookup.LookupSP(ctx, &req, lookupResCh)
	wait()
	return err
}

func newLookupSPO(index blog.Index, lookup lookups.SPO, op *plandef.LookupSPO, inputs []queryOperator) operator {
	if len(inputs) > 0 {
		panic(fmt.Sprintf("lookupSPO operation with unexpected inputs: %v", len(inputs)))
	}
	return &lookupSPO{
		baseLookup: baseLookup{
			def:   op,
			index: index,
			vars:  variableSetFromTerms(op.Terms()),
		},
		def:    op,
		lookup: lookup,
	}
}

type lookupSPO struct {
	baseLookup
	def    *plandef.LookupSPO
	lookup lookups.SPO
}

func (l *lookupSPO) execute(ctx context.Context, binder valueBinder, res results) error {
	req := rpc.LookupSPORequest{
		Index:   l.index,
		Lookups: make([]rpc.LookupSPORequest_Item, binder.len()),
	}
	for bulkOffset := uint32(0); bulkOffset < binder.len(); bulkOffset++ {
		req.Lookups[bulkOffset] = rpc.LookupSPORequest_Item{
			Subject:   kidOf(bulkOffset, binder, l.def.Subject),
			Predicate: kidOf(bulkOffset, binder, l.def.Predicate),
			Object:    kgObjectOf(bulkOffset, binder, l.def.Object),
		}
	}
	lookupResCh := make(chan *rpc.LookupChunk, 4)
	wait := parallel.Go(convertAndFwdChunks(ctx, l.vars, lookupResCh, res))
	err := l.lookup.LookupSPO(ctx, &req, lookupResCh)
	wait()
	return err
}

// convertAndFwdChunks returns a function that reads from lookupCh, converts
// each fact from it into a result and adds it to the result builder. It'll
// continue to do this until lookupCh is closed.
func convertAndFwdChunks(ctx context.Context, variables variableSet,
	lookupCh <-chan *rpc.LookupChunk, res results) func() {

	return func() {
		for chunk := range lookupCh {
			for _, fact := range chunk.Facts {
				fs := FactSet{Facts: []rpc.Fact{fact.Fact}}
				values := variables.resultsOf(&fact.Fact)
				res.add(ctx, fact.Lookup, fs, values)
			}
		}
	}
}
