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

package main

import (
	"context"

	"github.com/ebay/beam/facts/cache"
	"github.com/ebay/beam/infer"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/viewclient"
	log "github.com/sirupsen/logrus"
)

func inferSP(ctx context.Context, client *viewclient.Client, options *options) error {
	req := infer.SPRequest{
		Index: options.Index,
		Lookups: []infer.SPRequestItem{{
			Subject:   options.Subject.Value,
			Predicate: options.Predicate.Value,
		}},
	}
	resCh := make(chan *rpc.LookupChunk, 4)
	wait := goDumpFacts(ctx, client, resCh, options)
	log.Infof("Invoking InferSP: %+v", req)
	err := infer.SP(ctx, client, &req, resCh)
	wait()
	return err
}

func inferSPO(ctx context.Context, client *viewclient.Client, options *options) error {
	req := infer.SPORequest{
		Index: options.Index,
		Lookups: []infer.SPORequestItem{{
			Subject:   options.Subject.Value,
			Predicate: options.Predicate.Value,
			Object:    options.Object,
		}},
	}
	resCh := make(chan *rpc.LookupChunk, 4)
	wait := goDumpFacts(ctx, client, resCh, options)
	log.Infof("Invoking InferSPO: %+v", req)
	err := infer.SPO(ctx, client, cache.New(), &req, resCh)
	wait()
	return err
}

func inferPO(ctx context.Context, client *viewclient.Client, options *options) error {
	req := infer.PORequest{
		Index: options.Index,
		Lookups: []infer.PORequestItem{{
			Predicate: options.Predicate.Value,
			Object:    options.Object,
		}},
	}
	resCh := make(chan *rpc.LookupChunk, 4)
	wait := goDumpFacts(ctx, client, resCh, options)
	log.Infof("Invoking InferPO: %+v", req)
	err := infer.PO(ctx, client, &req, resCh)
	wait()
	return err
}
