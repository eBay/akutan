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

// Package lookups defines go interfaces that the various LookupXX rpc wrappers
// expose, this can be useful in decoupling the actual Loopup implementation from
// its usage, allowing for easier testing
package lookups

import (
	"context"

	"github.com/ebay/akutan/rpc"
)

// S defines the LookupS request, i.e. find all finds with a given subject
type S interface {
	LookupS(ctx context.Context, req *rpc.LookupSRequest, resCh chan *rpc.LookupChunk) error
}

// SP defines the LookupSP request, ie. final all facts with a given Subject, Predicate
type SP interface {
	LookupSP(ctx context.Context, req *rpc.LookupSPRequest, resCh chan *rpc.LookupChunk) error
}

// SPO defines the LookupSPO request, i.e. does the fact with these Subject, Predicate, Object values exist
type SPO interface {
	LookupSPO(ctx context.Context, req *rpc.LookupSPORequest, resCh chan *rpc.LookupChunk) error
}

// PO defines the LookupPO request, i.e. final all facts that have a matching Predicate & Object
type PO interface {
	LookupPO(ctx context.Context, req *rpc.LookupPORequest, resCh chan *rpc.LookupChunk) error
}

// POCmp defines the LookupPoCmp request, i.e. find all facts that have a matching Predicate and Object passes a defined comparison,
// comparisons include < <= > >= and within range
type POCmp interface {
	LookupPOCmp(ctx context.Context, req *rpc.LookupPOCmpRequest, resCh chan *rpc.LookupChunk) error
}

// All contains the collection of all Lookup types
type All interface {
	PO
	POCmp
	S
	SP
	SPO
}
