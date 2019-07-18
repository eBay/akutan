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
	"fmt"

	"github.com/ebay/akutan/api"
	"github.com/ebay/akutan/query"
	"github.com/ebay/akutan/query/exec"
	"github.com/ebay/akutan/query/parser"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/errors"
	"github.com/ebay/akutan/util/parallel"
	log "github.com/sirupsen/logrus"
)

// QueryFacts implements a streaming api for executing queries against the view store.
func (s *Server) QueryFacts(req *api.QueryFactsRequest, res api.FactStore_QueryFactsServer) error {
	return s.queryFactsImpl(res.Context(), req, res.Send)
}

// Implementation of querying facts, but callable from other parts of the gRPC server.
func (s *Server) queryFactsImpl(ctx context.Context, req *api.QueryFactsRequest, send func(*api.QueryFactsResult) error) error {
	log.Debugf("QueryFacts: %v", req)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	index, err := s.resolveIndex(ctx, uint64(req.Index))
	if err != nil {
		return err
	}
	resCh := make(chan query.ResultChunk, 4)
	wait := parallel.GoCaptureError(func() error {
		apiChunk := &api.QueryFactsResult{
			Index: int64(index),
		}
		for chunk := range resCh {
			apiChunk.Results = make([]api.QueryFactsResult_Result, len(chunk.Facts))
			for i, row := range chunk.Facts {
				apiChunk.Results[i] = api.QueryFactsResult_Result{
					Facts: make([]api.ResolvedFact, len(row.Facts)),
				}
				for j := range row.Facts {
					apiChunk.Results[i].Facts[j] = row.Facts[j].ToAPIFact()
				}
			}
			if err := send(apiChunk); err != nil {
				cancel()
				return err
			}
		}
		return nil
	})
	opts := query.Options{
		Debug:  s.cfg.API.DebugQuery,
		Format: parser.QueryFactPattern,
	}
	err = s.queryEngine.Query(ctx, index, req.Query, opts, resCh)
	return errors.Any(wait(), err)
}

// Query implements the query method in the FactStore gRPC interface
func (s *Server) Query(req *api.QueryRequest, res api.FactStore_QueryServer) error {
	return s.query(res.Context(), req, res.Send)
}

func (s *Server) query(ctx context.Context, req *api.QueryRequest, send func(*api.QueryResult) error) error {
	log.Debugf("Query: %v", req)
	index, err := s.resolveIndexConstraint(ctx, s.aLog, req.Index)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	resCh := make(chan query.ResultChunk, 4)
	wait := parallel.GoCaptureError(func() error {
		apiChunk := &api.QueryResult{
			Index: index,
		}
		for chunk := range resCh {
			rows := chunk.NumRows()
			apiChunk.Columns = make([]api.QueryResult_Column, len(chunk.Columns))
			for colIdx, col := range chunk.Columns {
				apiChunk.Columns[colIdx].Name = col.Name
				apiChunk.Columns[colIdx].Cells = make([]api.KGValue, rows)
			}
			for rowIdx := 0; rowIdx < rows; rowIdx++ {
				for colIdx, val := range chunk.Row(rowIdx) {
					apiChunk.Columns[colIdx].Cells[rowIdx] = toKGValue(val)
				}
			}
			apiChunk.TotalResultSize = chunk.FinalStatistics.TotalResultSize
			if err := send(apiChunk); err != nil {
				cancel()
				return err
			}
		}
		return nil
	})
	opts := query.Options{
		Debug:  s.cfg.API.DebugQuery,
		Format: parser.QuerySparql,
	}
	err = s.queryEngine.Query(ctx, index, req.Query, opts, resCh)
	return errors.Any(wait(), err)
}

// toKGValue will construct a representation of the exec.Value as an api.KGValue
// structure.
func toKGValue(o exec.Value) api.KGValue {
	r := api.KGValue{}
	var unit *api.KGID
	if o.KGObject.UnitID() != 0 {
		unit = &api.KGID{QName: o.UnitExtID, SysId: o.KGObject.UnitID()}
	}
	switch o.KGObject.ValueType() {
	case rpc.KtBool:
		r.Value = &api.KGValue_Bool{Bool: &api.KGBool{Value: o.KGObject.ValBool(), Unit: unit}}
	case rpc.KtFloat64:
		r.Value = &api.KGValue_Float64{Float64: &api.KGFloat64{Value: o.KGObject.ValFloat64(), Unit: unit}}
	case rpc.KtInt64:
		r.Value = &api.KGValue_Int64{Int64: &api.KGInt64{Value: o.KGObject.ValInt64(), Unit: unit}}
	case rpc.KtKID:
		r.Value = &api.KGValue_Node{Node: &api.KGID{QName: o.ExtID, SysId: o.KGObject.ValKID()}}
	case rpc.KtString:
		var lang *api.KGID
		if o.KGObject.LangID() != 0 {
			lang = &api.KGID{QName: o.LangExtID, SysId: o.KGObject.LangID()}
		}
		r.Value = &api.KGValue_Str{Str: &api.KGString{Value: o.KGObject.ValString(), Lang: lang}}
	case rpc.KtTimestamp:
		ts := o.KGObject.ValTimestamp()
		apiTS := api.KGTimestamp{
			Precision: api.Precision(ts.Precision),
			Value:     ts.Value,
		}
		r.Value = &api.KGValue_Timestamp{Timestamp: &apiTS}
	case rpc.KtNil:
		// nothing more to do
	default:
		panic(fmt.Sprintf("toKGValue with unexpected type %v", o.KGObject.ValueType()))
	}
	return r
}
