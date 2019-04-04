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
	"context"
	"fmt"
	"strings"

	"github.com/ebay/beam/blog"
	"github.com/ebay/beam/rpc"
	"github.com/sirupsen/logrus"
)

type storeLookups struct {
	snapshot func(context.Context, blog.Index) (*Snapshot, error)
}

func (lookups storeLookups) LookupPO(ctx context.Context, req *rpc.LookupPORequest, resCh chan *rpc.LookupChunk) error {
	snap, err := lookups.snapshot(ctx, req.Index)
	if err != nil {
		close(resCh)
		return err
	}
	return snap.Lookups().LookupPO(ctx, req, resCh)
}

func (lookups storeLookups) LookupPOCmp(ctx context.Context, req *rpc.LookupPOCmpRequest, resCh chan *rpc.LookupChunk) error {
	snap, err := lookups.snapshot(ctx, req.Index)
	if err != nil {
		close(resCh)
		return err
	}
	return snap.Lookups().LookupPOCmp(ctx, req, resCh)
}

func (lookups storeLookups) LookupS(ctx context.Context, req *rpc.LookupSRequest, resCh chan *rpc.LookupChunk) error {
	snap, err := lookups.snapshot(ctx, req.Index)
	if err != nil {
		close(resCh)
		return err
	}
	return snap.Lookups().LookupS(ctx, req, resCh)
}

func (lookups storeLookups) LookupSP(ctx context.Context, req *rpc.LookupSPRequest, resCh chan *rpc.LookupChunk) error {
	snap, err := lookups.snapshot(ctx, req.Index)
	if err != nil {
		close(resCh)
		return err
	}
	return snap.Lookups().LookupSP(ctx, req, resCh)
}

func (lookups storeLookups) LookupSPO(ctx context.Context, req *rpc.LookupSPORequest, resCh chan *rpc.LookupChunk) error {
	snap, err := lookups.snapshot(ctx, req.Index)
	if err != nil {
		close(resCh)
		return err
	}
	return snap.Lookups().LookupSPO(ctx, req, resCh)
}

// snapLookups is a wrapper around Snapshot to avoid cluttering the Snapshot
// type with a bunch of lookup methods.
type snapLookups struct {
	snap *Snapshot
}

func (s snapLookups) LookupS(ctx context.Context, req *rpc.LookupSRequest, resCh chan *rpc.LookupChunk) error {
	dest := rpc.NewFactSink(sender(resCh), lookupFlushSize)
	for offset, subject := range req.Subjects {
		s.filter(req.Index, offset, dest, hasSubject(subject))
	}
	dest.Flush()
	close(resCh)
	return nil
}

const lookupFlushSize = 128

func sender(resCh chan *rpc.LookupChunk) rpc.ChunkReadyCallback {
	return func(c *rpc.LookupChunk) error {
		resCh <- c
		return nil
	}
}

type factPredicate func(rpc.Fact) bool

func (s snapLookups) filter(index blog.Index, offset int, dest *rpc.FactSink, pred factPredicate) {
	for _, fact := range s.snap.AllFacts() {
		if fact.Index <= index && pred(fact) {
			err := dest.Write(offset, fact)
			if err != nil {
				logrus.WithError(err).Warn("Error writing to rpc.FactSink")
			}
		}
	}
}

func hasSubject(s uint64) factPredicate {
	return func(f rpc.Fact) bool {
		return f.Subject == s
	}
}

func (s snapLookups) LookupSP(ctx context.Context, req *rpc.LookupSPRequest, resCh chan *rpc.LookupChunk) error {
	dest := rpc.NewFactSink(sender(resCh), lookupFlushSize)
	for offset, lk := range req.Lookups {
		s.filter(req.Index, offset, dest,
			hasSubjectPredicate(lk.Subject, lk.Predicate))
	}
	dest.Flush()
	close(resCh)
	return nil
}

func hasSubjectPredicate(subject, predicate uint64) factPredicate {
	return func(f rpc.Fact) bool {
		return f.Subject == subject && f.Predicate == predicate
	}
}

func (s snapLookups) LookupSPO(ctx context.Context, req *rpc.LookupSPORequest, resCh chan *rpc.LookupChunk) error {
	dest := rpc.NewFactSink(sender(resCh), lookupFlushSize)
	for offset, lk := range req.Lookups {
		s.filter(req.Index, offset, dest,
			hasSubjectPredicateObject(lk.Subject, lk.Predicate, lk.Object))
	}
	dest.Flush()
	close(resCh)
	return nil
}

func hasSubjectPredicateObject(subject, predicate uint64, obj rpc.KGObject) factPredicate {
	return func(f rpc.Fact) bool {
		return f.Subject == subject && f.Predicate == predicate && f.Object.Equal(obj)
	}
}

func (s snapLookups) LookupPO(ctx context.Context, req *rpc.LookupPORequest, resCh chan *rpc.LookupChunk) error {
	dest := rpc.NewFactSink(sender(resCh), lookupFlushSize)
	for offset, lk := range req.Lookups {
		s.filter(req.Index, offset, dest,
			hasPredicateObject(lk.Predicate, lk.Object))
	}
	dest.Flush()
	close(resCh)
	return nil
}

func hasPredicateObject(predicate uint64, obj rpc.KGObject) factPredicate {
	return func(f rpc.Fact) bool {
		return f.Predicate == predicate && f.Object.Equal(obj)
	}
}

func (s snapLookups) LookupPOCmp(ctx context.Context, req *rpc.LookupPOCmpRequest, resCh chan *rpc.LookupChunk) error {
	dest := rpc.NewFactSink(sender(resCh), lookupFlushSize)
	for offset, lk := range req.Lookups {
		s.filter(req.Index, offset, dest, func(f rpc.Fact) bool {
			if f.Predicate != lk.Predicate || f.Object.ValueType() != lk.Object.ValueType() {
				return false
			}
			switch lk.Operator {
			case rpc.OpLess:
				return f.Object.Less(lk.Object)
			case rpc.OpLessOrEqual:
				return objLessOrEq(f.Object, lk.Object)
			case rpc.OpGreater:
				return !objLessOrEq(f.Object, lk.Object)
			case rpc.OpGreaterOrEqual:
				return !f.Object.Less(lk.Object)
			case rpc.OpPrefix:
				return strings.HasPrefix(f.Object.ValString(), lk.Object.ValString())
			case rpc.OpEqual:
				return f.Object.Equal(lk.Object)
			case rpc.OpNotEqual:
				return !f.Object.Equal(lk.Object)
			case rpc.OpRangeIncInc:
				return !objLessOrEq(lk.Object, f.Object) && objLessOrEq(f.Object, lk.EndObject)
			case rpc.OpRangeIncExc:
				return !objLessOrEq(lk.Object, f.Object) && f.Object.Less(lk.EndObject)
			case rpc.OpRangeExcInc:
				return !lk.Object.Less(f.Object) && objLessOrEq(f.Object, lk.EndObject)
			case rpc.OpRangeExcExc:
				return !lk.Object.Less(f.Object) && f.Object.Less(lk.EndObject)
			default:
				panic(fmt.Sprintf("Unexpected operator %v", lk.Operator))
			}
		})
	}
	dest.Flush()
	close(resCh)
	return nil
}

func objLessOrEq(a, b rpc.KGObject) bool {
	return a.Less(b) || a.Equal(b)
}
