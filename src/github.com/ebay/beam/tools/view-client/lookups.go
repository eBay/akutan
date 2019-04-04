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

	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/viewclient"
	log "github.com/sirupsen/logrus"
)

func lookupS(ctx context.Context, client *viewclient.Client, options *options) error {
	req := &rpc.LookupSRequest{
		Index:    options.Index,
		Subjects: []uint64{options.Subject.Value},
	}
	resCh := make(chan *rpc.LookupChunk, 4)
	wait := goDumpFacts(ctx, client, resCh, options)
	log.Infof("Invoking LookupS: %+v", req)
	err := client.LookupS(ctx, req, resCh)
	wait()
	return err
}

func lookupSP(ctx context.Context, client *viewclient.Client, options *options) error {
	req := &rpc.LookupSPRequest{
		Index: options.Index,
		Lookups: []rpc.LookupSPRequest_Item{{
			Subject:   options.Subject.Value,
			Predicate: options.Predicate.Value,
		}},
	}
	resCh := make(chan *rpc.LookupChunk, 4)
	wait := goDumpFacts(ctx, client, resCh, options)
	log.Infof("Invoking LookupSP: %+v", req)
	err := client.LookupSP(ctx, req, resCh)
	wait()
	return err
}

func lookupSPO(ctx context.Context, client *viewclient.Client, options *options) error {
	req := &rpc.LookupSPORequest{
		Index: options.Index,
		Lookups: []rpc.LookupSPORequest_Item{{
			Subject:   options.Subject.Value,
			Predicate: options.Predicate.Value,
			Object:    options.Object,
		}},
	}
	resCh := make(chan *rpc.LookupChunk, 4)
	wait := goDumpFacts(ctx, client, resCh, options)
	log.Infof("Invoking LookupSPO: %#v", req)
	err := client.LookupSPO(ctx, req, resCh)
	wait()
	return err
}

func lookupPO(ctx context.Context, client *viewclient.Client, options *options) error {
	req := &rpc.LookupPORequest{
		Index: options.Index,
		Lookups: []rpc.LookupPORequest_Item{{
			Predicate: options.Predicate.Value,
			Object:    options.Object,
		}},
	}
	resCh := make(chan *rpc.LookupChunk, 4)
	wait := goDumpFacts(ctx, client, resCh, options)
	log.Infof("Invoking LookupPO: %+v", req)
	err := client.LookupPO(ctx, req, resCh)
	wait()
	return err
}

func lookupPOCmp(ctx context.Context, client *viewclient.Client, options *options) error {
	req := &rpc.LookupPOCmpRequest{
		Index: options.Index,
		Lookups: []rpc.LookupPOCmpRequest_Item{{
			Predicate: options.Predicate.Value,
			Operator:  options.Operator.Value,
			Object:    options.Object,
			EndObject: options.EndObject,
		}},
	}
	resCh := make(chan *rpc.LookupChunk, 4)
	wait := goDumpFacts(ctx, client, resCh, options)
	log.Infof("Invoking LookupPOCmp: %+v", req)
	err := client.LookupPOCmp(ctx, req, resCh)
	wait()
	return err
}
