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

// Package conv helps convert between related types as an update request is
// processed.
package conv

import (
	"fmt"

	"github.com/ebay/beam/logentry"
	"github.com/ebay/beam/logentry/logwrite"
	"github.com/ebay/beam/query/parser"
	"github.com/ebay/beam/rpc"
)

// ParserToRPC converts fields from parser.Insert into RPC values.
type ParserToRPC struct {
	// Maps from external IDs to KIDs. Values of 0 are equivalent to missing
	// keys.
	ExternalIDs map[string]uint64
	// Maps from variable names to KIDs. Values of 0 are equivalent to missing
	// keys.
	Variables map[string]uint64
}

// KID returns a non-zero value and true if the term is convertible to a KID.
// Otherwise, it returns 0 and false.
func (converter *ParserToRPC) KID(term parser.Term) (uint64, bool) {
	switch term := term.(type) {
	case *parser.LiteralID:
		return term.Value, true
	case *parser.QName:
		kid := converter.ExternalIDs[term.Value]
		return kid, kid != 0
	case *parser.Entity:
		kid := converter.ExternalIDs[term.Value]
		return kid, kid != 0
	case *parser.Variable:
		kid := converter.Variables[term.Name]
		return kid, kid != 0
	default:
		panic(fmt.Sprintf("update.conv: unexpected type %T", term))
	}
}

// KGObject returns a non-empty KGObject and true if the term is convertible to
// a KGObject. Otherwise, it returns a zero KGObject and false.
func (converter *ParserToRPC) KGObject(term parser.Term) (rpc.KGObject, bool) {
	var object rpc.KGObject
	allOk := true
	required := func(xid string) uint64 {
		kid := converter.ExternalIDs[xid]
		if kid == 0 {
			allOk = false
		}
		return kid
	}
	optional := func(xid string) uint64 {
		if xid == "" {
			return 0
		}
		return required(xid)
	}
	switch term := term.(type) {
	case *parser.LiteralID:
		object = rpc.AKID(term.Value)
	case *parser.QName:
		object = rpc.AKID(required(term.Value))
	case *parser.Entity:
		object = rpc.AKID(required(term.Value))
	case *parser.Variable:
		kid := converter.Variables[term.Name]
		if kid == 0 {
			allOk = false
		}
		object = rpc.AKID(kid)
	case *parser.LiteralBool:
		object = rpc.ABool(term.Value, optional(term.Unit.Value))
	case *parser.LiteralFloat:
		object = rpc.AFloat64(term.Value, optional(term.Unit.Value))
	case *parser.LiteralInt:
		object = rpc.AInt64(term.Value, optional(term.Unit.Value))
	case *parser.LiteralString:
		object = rpc.AString(term.Value, optional(term.Language.Value))
	case *parser.LiteralTime:
		// Precision is currently defined identically in both api/parser &
		// logentry/rpc. This function will need to change if that changes. The
		// unit test Test_PrecisionSame verifies that the values are
		// currently equal.
		object = rpc.ATimestamp(term.Value,
			logentry.TimestampPrecision(term.Precision), optional(term.Unit.Value))
	default:
		panic(fmt.Sprintf("update.conv: unexpected type %T", term))
	}
	if !allOk {
		return rpc.KGObject{}, false
	}
	return object, true
}

// ParserToLog converts fields from parser.Insert into logentry values.
type ParserToLog struct {
	// Maps from external IDs to KID or offset.
	ExternalIDs map[string]logentry.KIDOrOffset
	// Maps from variable names to KID or offset.
	Variables map[string]logentry.KIDOrOffset
}

// MustKIDOrOffset returns a non-zero value or panics. It panics if a necessary
// external ID or variable doesn't have a KIDorOffset value.
func (converter *ParserToLog) MustKIDOrOffset(term parser.Term) logentry.KIDOrOffset {
	resolveExternalIDOrPanic := func(xid string) logentry.KIDOrOffset {
		value, ok := converter.ExternalIDs[xid]
		if !ok {
			panic(fmt.Sprintf("No KID found for external ID: %v", xid))
		}
		return value
	}
	switch term := term.(type) {
	case *parser.QName:
		return resolveExternalIDOrPanic(term.Value)
	case *parser.Entity:
		return resolveExternalIDOrPanic(term.Value)
	case *parser.LiteralID:
		return logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{
			Kid: term.Value,
		}}
	case *parser.Variable:
		value, ok := converter.Variables[term.Name]
		if !ok {
			panic(fmt.Sprintf("Failed to resolve variable %v", term.Name))
		}
		return value
	default:
		panic(fmt.Sprintf("update.conv: unexpected type %T", term))
	}
}

// MustKGObject returns a non-zero value or panics. It panics if a necessary
// external ID or variable doesn't have a KIDorOffset value.
func (converter *ParserToLog) MustKGObject(term parser.Term) logentry.KGObject {
	fromKIDOrOffset := func(v logentry.KIDOrOffset) logentry.KGObject {
		switch v := v.Value.(type) {
		case nil:
			return logentry.KGObject{}
		case *logentry.KIDOrOffset_Kid:
			return logwrite.AKID(v.Kid)
		case *logentry.KIDOrOffset_Offset:
			return logwrite.AKIDOffset(v.Offset)
		}
		panic(fmt.Sprintf("update.conv: unexpected type %T", v.Value))
	}
	requiredExternalID := func(xid string) logentry.KIDOrOffset {
		value := converter.ExternalIDs[xid]
		if value == (logentry.KIDOrOffset{}) {
			panic(fmt.Sprintf("No KID or offset found for external ID: %v", xid))
		}
		return value
	}
	optionalExternalID := func(xid string) logentry.KIDOrOffset {
		if xid == "" {
			return logentry.KIDOrOffset{}
		}
		return requiredExternalID(xid)
	}
	switch term := term.(type) {
	case *parser.LiteralID:
		return logwrite.AKID(term.Value)
	case *parser.QName:
		return fromKIDOrOffset(requiredExternalID(term.Value))
	case *parser.Entity:
		return fromKIDOrOffset(requiredExternalID(term.Value))
	case *parser.Variable:
		value := converter.Variables[term.Name]
		if value == (logentry.KIDOrOffset{}) {
			panic(fmt.Sprintf("Failed to resolve variable %v", term.Name))
		}
		return fromKIDOrOffset(value)
	case *parser.LiteralBool:
		return logentry.KGObject{
			Value:  &logentry.KGObject_ABool{ABool: term.Value},
			UnitID: optionalExternalID(term.Unit.Value),
		}
	case *parser.LiteralFloat:
		return logentry.KGObject{
			Value:  &logentry.KGObject_AFloat64{AFloat64: term.Value},
			UnitID: optionalExternalID(term.Unit.Value),
		}
	case *parser.LiteralInt:
		return logentry.KGObject{
			Value:  &logentry.KGObject_AInt64{AInt64: term.Value},
			UnitID: optionalExternalID(term.Unit.Value),
		}
	case *parser.LiteralString:
		return logentry.KGObject{
			Value:  &logentry.KGObject_AString{AString: term.Value},
			LangID: optionalExternalID(term.Language.Value),
		}
	case *parser.LiteralTime:
		// Precision is currently defined identically in both api &
		// logentry. This function will need to change if that changes. The
		// unit test Test_PrecisionSame verifies that the values are
		// currently equal.
		return logentry.KGObject{
			Value: &logentry.KGObject_ATimestamp{
				ATimestamp: &logentry.KGTimestamp{
					Precision: logentry.TimestampPrecision(term.Precision),
					Value:     term.Value,
				},
			},
			UnitID: optionalExternalID(term.Unit.Value),
		}
	default:
		panic(fmt.Sprintf("update.conv: unexpected type %T", term))
	}
}
