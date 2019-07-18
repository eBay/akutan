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
	"time"

	"github.com/ebay/akutan/api"
	log "github.com/sirupsen/logrus"
)

func lookupPO(ctx context.Context, store api.FactStoreClient, options *options) error {
	req := &api.LookupPORequest{
		Index:     options.Index,
		Predicate: options.Predicate.Value,
		Object:    []api.KGObject{options.Object},
	}
	log.Infof("Invoking LookupPO: %+v", req)
	start := time.Now()
	res, err := store.LookupPo(ctx, req)
	if err != nil {
		return err
	}
	log.Infof("LookupPO returned: %+v", res)
	log.Infof("LookupPO took %s", time.Since(start))
	return nil
}

func lookupSP(ctx context.Context, store api.FactStoreClient, options *options) error {
	req := &api.LookupSPRequest{
		Index:   options.Index,
		Lookups: []api.LookupSPRequest_Item{{Subject: options.Subject.Value, Predicate: options.Predicate.Value}},
	}
	log.Infof("Invoking LookupSP: %+v", req)
	start := time.Now()
	res, err := store.LookupSp(ctx, req)
	if err != nil {
		return err
	}
	log.Infof("LookupSP returned: %+v", res)
	log.Infof("LookupSP took %s", time.Since(start))
	return nil
}
