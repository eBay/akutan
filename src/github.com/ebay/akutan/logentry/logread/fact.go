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

// Package logread deals with mapping from logentry types into rpc types.
// Services that have to read the log and process it will find this helpful.
package logread

import (
	"fmt"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/logentry"
	"github.com/ebay/akutan/rpc"
)

// ToRPCFact converts a log representation of a InsertFact into the internal
// representation. This will resolve the final KIDs of any fields that were set
// with KID Offsets, and convert the KGObject from the logentry format to the
// rpc format.
func ToRPCFact(idx blog.Index, f *logentry.InsertFact) rpc.Fact {
	return rpc.Fact{
		Index:     idx,
		Id:        KID(idx, f.FactIDOffset),
		Subject:   KIDof(idx, &f.Subject),
		Predicate: KIDof(idx, &f.Predicate),
		Object:    ToRPCKGObject(idx, f.Object),
	}
}

// ToRPCKGObject returns a new rpc.KGObject instance that is equivalent to the
// supplied log representation of a KGObject. The rpc.KGObject encapsulates a
// binary encoding of the KGObject
func ToRPCKGObject(idx blog.Index, from logentry.KGObject) rpc.KGObject {
	if from.Value == nil {
		return rpc.KGObject{}
	}
	unitKID := KIDof(idx, &from.UnitID)
	switch t := from.Value.(type) {
	case *logentry.KGObject_AString:
		return rpc.AString(t.AString, KIDof(idx, &from.LangID))
	case *logentry.KGObject_AFloat64:
		return rpc.AFloat64(t.AFloat64, unitKID)
	case *logentry.KGObject_AInt64:
		return rpc.AInt64(t.AInt64, unitKID)
	case *logentry.KGObject_ATimestamp:
		return rpc.ATimestamp(t.ATimestamp.Value, t.ATimestamp.Precision, unitKID)
	case *logentry.KGObject_ABool:
		return rpc.ABool(t.ABool, unitKID)
	case *logentry.KGObject_AKID:
		return rpc.AKID(t.AKID)
	case *logentry.KGObject_AKIDOffset:
		return rpc.AKID(KID(idx, t.AKIDOffset))
	default:
		panic(fmt.Sprintf("ToRPCKGObject encountered a KGObject with an unexpected type %T/%v", from.Value, from.Value))
	}
}
