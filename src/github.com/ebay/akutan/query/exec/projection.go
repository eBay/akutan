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
	"github.com/ebay/akutan/msg/facts"
	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/parallel"
	"github.com/ebay/akutan/viewclient/lookups"
)

// newProjection returns a projection operator (aka SELECT). When executed it
// evaluates each row of input using the expressions defined in the projection
// Operator, and sends the results as output.
func newProjection(op *plandef.Projection, inputs []queryOperator) operator {
	if len(inputs) != 1 {
		panic(fmt.Sprintf("projection operation with unexpected number of inputs: %v", len(inputs)))
	}
	cols := make(Columns, len(op.Select))
	for i := range op.Select {
		cols[i] = op.Select[i].Out
	}
	return &projection{
		cols:  cols,
		def:   op,
		input: inputs[0],
	}
}

type projection struct {
	cols  Columns
	def   *plandef.Projection
	input queryOperator
}

func (p *projection) operator() plandef.Operator {
	return p.def
}

func (p *projection) columns() Columns {
	return p.cols
}

func (p *projection) execute(ctx context.Context, binder valueBinder, res results) error {
	if binder.len() != 1 {
		panic(fmt.Sprintf("projection operator %v unexpectedly bulk bound to %d rows",
			p.def, binder.len()))
	}
	inputResCh := make(chan ResultChunk, 4)
	wait := parallel.Go(func() {
		evals := buildExprEvaluators(p.def, p.input.columns())
		for chunk := range inputResCh {
			res.setFinalStatistics(chunk.FinalStatistics)
			for i := range chunk.offsets {
				rowIn := chunk.Row(i)
				rowOut := make([]Value, len(p.def.Select))
				hasValue := false
				for colIdx := range p.def.Select {
					out := evals[colIdx].consume(rowIn)
					if out != nil {
						rowOut[colIdx] = *out
						hasValue = true
					}
				}
				if hasValue {
					res.add(ctx, 0, FactSet{}, rowOut)
				}
			}
		}
		rowOut := make([]Value, len(p.def.Select))
		hasValue := false
		for colIdx := range p.def.Select {
			out := evals[colIdx].completed()
			if out != nil {
				rowOut[colIdx] = *out
				hasValue = true
			}
		}
		if hasValue {
			res.add(ctx, 0, FactSet{}, rowOut)
		}
	})
	err := p.input.run(ctx, binder, inputResCh)
	wait()
	return err
}

// newAsk returns a new Ask operator, when executed it generates a single result
// of true if the input returned any rows, false otherwise.
func newAsk(op *plandef.Ask, inputs []queryOperator) operator {
	if len(inputs) != 1 {
		panic(fmt.Sprintf("ask operation with unexpected number of inputs: %d", len(inputs)))
	}
	return &ask{
		def:   op,
		input: inputs[0],
	}
}

type ask struct {
	def   *plandef.Ask
	input queryOperator
}

func (a *ask) columns() Columns {
	return Columns{a.def.Out}
}

func (a *ask) operator() plandef.Operator {
	return a.def
}

func (a *ask) execute(ctx context.Context, binder valueBinder, res results) error {
	if binder.len() != 1 {
		panic(fmt.Sprintf("ask operator %v unexpectedly bulk bound to %d rows",
			a.def, binder.len()))
	}
	inputResCh := make(chan ResultChunk)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	weCancelled := false
	wait := parallel.Go(func() {
		for chunk := range inputResCh {
			if len(chunk.offsets) > 0 {
				result := Value{KGObject: rpc.ABool(true, 0)}
				res.add(ctx, 0, FactSet{}, []Value{result})
				weCancelled = true
				cancel()
				return
			}
		}
		result := Value{KGObject: rpc.ABool(false, 0)}
		res.add(ctx, 0, FactSet{}, []Value{result})
	})
	err := a.input.run(ctx, binder, inputResCh)
	wait()
	if weCancelled && err == context.Canceled {
		return nil
	}
	return err
}

// newExternalIDs returns an operator that updates the input Values with
// ExternalID values.
func newExternalIDs(index blog.Index, lookup lookups.SP, op *plandef.ExternalIDs, inputs []queryOperator) operator {
	if len(inputs) != 1 {
		panic(fmt.Sprintf("externalIDs operation with unexpected inputs: %v", len(inputs)))
	}
	return &externalIDs{
		index:  index,
		lookup: lookup,
		def:    op,
		input:  inputs[0],
	}
}

type externalIDs struct {
	index  blog.Index
	lookup lookups.SP
	def    *plandef.ExternalIDs
	input  queryOperator
}

func (e *externalIDs) columns() Columns {
	return e.input.columns()
}

func (e *externalIDs) operator() plandef.Operator {
	return e.def
}

func (e *externalIDs) execute(ctx context.Context, binder valueBinder, res results) error {
	inputResCh := make(chan ResultChunk, 4)

	// processInput reads chunks from the input operator, collects up the
	// set of externalIDs that need fetching, fetches them, and updates the
	// chunk, and then publishes it as this operators output. Each chunk
	// processed results in at most one LookupSP call. ExternalIDs are
	// cached for the duration of the operation.
	processInput := func(ctx context.Context) error {
		resolver := newExternalIDResolver(e.index)
		for c := range inputResCh {
			res.setFinalStatistics(c.FinalStatistics)
			for i := range c.Values {
				v := &c.Values[i]
				if v.KGObject.IsType(rpc.KtKID) {
					resolver.resolveID(v.KGObject.ValKID(), v.SetExtID)

				} else if v.KGObject.UnitID() != 0 {
					resolver.resolveID(v.KGObject.UnitID(), v.SetUnitExtID)

				} else if v.KGObject.LangID() != 0 {
					resolver.resolveID(v.KGObject.LangID(), v.SetLangExtID)
				}
			}
			// resolve any newly found KIDs and call the callback functions.
			if err := resolver.fetchPending(ctx, e.lookup); err != nil {
				return err
			}
			// we're done with this chunk now, send it as output
			for i := range c.offsets {
				res.add(ctx, c.offsets[i], c.Facts[i], c.Row(i))
			}
		}
		return nil
	}
	err := parallel.Invoke(ctx,
		processInput,
		func(ctx context.Context) error {
			return e.input.run(ctx, binder, inputResCh)
		})
	return err
}

// newExternalIDResolver returns a new externalIDResolver, which resolves KIDs
// to ExternalIDs as of the supplied log index. The returned resolver is not
// concurrent safe.
func newExternalIDResolver(index blog.Index) *externalIDResolver {
	return &externalIDResolver{
		ids: make(map[uint64]extIDlookupItem),
		lookup: rpc.LookupSPRequest{
			Index: index,
		},
	}
}

// externalIDResolver is used to bulk resolve internal KIDs to their
// externalIDs.
type externalIDResolver struct {
	ids    map[uint64]extIDlookupItem
	lookup rpc.LookupSPRequest
}

// resolveID arranges for 'callback' to be called with the externalID value of
// the supplied 'kid'. It might be called straight away if we already have it or
// it might be later after a call to fetchPending(). The Lookup item will be
// added to lookupReq if needed.
func (r *externalIDResolver) resolveID(kid uint64, callback func(extID string)) {
	item, exists := r.ids[kid]
	if exists && item.extID != "" {
		callback(item.extID)
		return
	}
	item.callbacks = append(item.callbacks, callback)
	r.ids[kid] = item
	if !exists {
		// this is the first request for this ID, add the lookup rpc.
		lk := rpc.LookupSPRequest_Item{
			Subject:   kid,
			Predicate: facts.HasExternalID,
		}
		r.lookup.Lookups = append(r.lookup.Lookups, lk)
	}
}

// fetchPending executes a LookupSP RPC to fetch the ExternalIDs that still need
// resolving, and executes the callbacks once the results are received. There
// can be multiple cycles of resolveID & fetchPending on a single
// externalIDResolver. An error is returned if the LookupSP request fails, as
// that is a streaming response its possible to get partial results in error
// conditions.
func (r *externalIDResolver) fetchPending(ctx context.Context, lookup lookups.SP) error {
	if len(r.lookup.Lookups) == 0 {
		return nil
	}
	resCh := make(chan *rpc.LookupChunk, 4)
	wait := parallel.Go(func() {
		for lookupChunk := range resCh {
			for _, f := range lookupChunk.Facts {
				extID := f.Fact.Object.ValString()
				for _, cb := range r.ids[f.Fact.Subject].callbacks {
					cb(extID)
				}
				r.ids[f.Fact.Subject] = extIDlookupItem{extID: extID}
			}
		}
	})
	err := lookup.LookupSP(ctx, &r.lookup, resCh)
	wait()
	if err != nil {
		return err
	}
	r.lookup.Lookups = r.lookup.Lookups[:0]
	return nil
}

type extIDlookupItem struct {
	extID     string
	callbacks []func(string)
}

// emptyResultOp is a queryOperator that generates a ResultChunk with the output
// columns in the event the input operator generates no results.
type emptyResultOp struct {
	input queryOperator
}

func (e *emptyResultOp) columns() Columns {
	return e.input.columns()
}

func (e *emptyResultOp) run(ctx context.Context, binder valueBinder, resCh chan<- ResultChunk) error {
	if binder.len() != 1 {
		panic(fmt.Sprintf("emptyResultOp unexpectedly bulk bound to %d rows", binder.len()))
	}
	childCh := make(chan ResultChunk, 4)
	wait := parallel.Go(func() {
		hadResult := false
		defer close(resCh)
		for chunk := range childCh {
			hadResult = true
			select {
			case resCh <- chunk:
			case <-ctx.Done():
				return
			}
		}
		if !hadResult {
			empty := ResultChunk{
				Columns: e.input.columns(),
			}
			select {
			case resCh <- empty:
			case <-ctx.Done():
			}
		}
	})
	err := e.input.run(ctx, binder, childCh)
	wait()
	return err
}
