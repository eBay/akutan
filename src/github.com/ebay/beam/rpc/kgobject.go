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

package rpc

import (
	"bytes"
	"fmt"

	"github.com/ebay/beam/api"
)

// KGObject represents a value of the Object field of a fact, this can be a KID
// i.e. another node in the graph, or it can be a typed literal value. String literals
// can also have a Language ID set (LangID) other literal types can have a Units ID
// set (UnitID). KGObject is safe to use as a key in a map. The zero value for KGObject
// maps to the KtNil ObjectType
type KGObject struct {
	// The KGObject encoding will encode the KGObject in a manner that results in keys that
	// preserves the lexicographic sort order e.g. int -1 will appear before int 5 when ordered
	// by the resulting bytes. Note that this encoding doesn't encode how big it is, so when packed with
	// other data, the container will need to be able to determine where the encoded object ends in
	// order to be able to correctly decode it.
	//
	// All types except for nil are encoded with a leading byte indicating the type, and then a per type specific encoding.
	// All sub value are encoded in big endian format
	//
	// 	Type: 		1 byte
	//	UnitID:		19 bytes [only for float64, int64, timestamp, bool]
	//  TypeBytes: 	N bytes depends on type
	//  LangID:		19 bytes [only for string]
	//
	// TypeBytes
	//  String:     The UTF8 bytes of the string, a terminating 0x00. length is not encoded anywhere.
	//              The nil is to ensure correct ordering, its not used to determine the end of the
	//              string. The string may not contain nil's. KGObject doesn't require the string to
	//              valid UTF8, however other parts of the system may.
	//  Float64:  	take the raw bits, invert them all if the value is negative, invert just the sign bit if its >= 0
	//  Int64:  	8 bytes, the sign bit is flipped, which results in -MaxInt64 == 0(x8) & MaxInt64 = FF(x8) and 0 = 0x80 00 00 00 00 00 00 00
	//  Timestamp: 	[year 2 bytes][month 1][day 1][hour 1][minutes 1][seconds 1][nanoseconds 4 bytes][precision 1 byte] normalized to GMT
	//  Bool:   	1 byte, 1 for true, 0 for false
	//  KID:    	8 bytes uint64
	//
	// KGObject's that are nil (either the KGObject is nil, or its value pointer is nil) are encoding
	// as an empty slice of bytes. KGObjects that go over the wire however will send a 1 byte nil marker
	// for the nil value. Marshal & Unmarshal handle this mapping between the go & wire differences.
	value string
}

// KGObjectType is used to describethe contained type within an KGObject.
// Instances of KGObjectType are encoded into the serialization format and so
// you should not change the values for existing types.
type KGObjectType uint8

const (
	// KtNil indicates the KGObject has no type.
	KtNil KGObjectType = 0

	// KtString indicates the KGObject contains an arbitary unicode string.
	KtString KGObjectType = 1

	// KtFloat64 indicates the KGObject contains an 8 byte double precision float.
	KtFloat64 KGObjectType = 2

	// KtInt64 indicates the KGObject contains an 8 byte signed integer.
	KtInt64 KGObjectType = 3

	// KtTimestamp indicates the KGObject contains a KGTimestamp, which constists
	// of a point in time and a precision indication that says which fields to
	// pay attention to
	KtTimestamp KGObjectType = 4

	// KtBool indicates the KGObject contains a boolean
	KtBool KGObjectType = 5

	// KtKID indicates the KGObject contains a KID (a Knowledge Graph ID), aka a node in the graph
	// rather than a literal value
	KtKID KGObjectType = 6
)

// Equal returns true if 'other' & 'o' contain the same value
func (o KGObject) Equal(other KGObject) bool {
	return o.value == other.value
}

// Less returns true if 'o' is lexigraphically smaller than 'right'
// When 'o' & 'right' are of different types, the types are compared
// in a consistent order
func (o KGObject) Less(right KGObject) bool {
	return o.value < right.value
}

// ToAPIObject will take this KGObject and return an KGObject in the external
// API format
func (o KGObject) ToAPIObject() api.KGObject {
	r := api.KGObject{
		LangID: o.LangID(),
		UnitID: o.UnitID(),
	}
	switch o.ValueType() {
	case KtBool:
		r.Value = &api.KGObject_ABool{ABool: o.ValBool()}
	case KtFloat64:
		r.Value = &api.KGObject_AFloat64{AFloat64: o.ValFloat64()}
	case KtInt64:
		r.Value = &api.KGObject_AInt64{AInt64: o.ValInt64()}
	case KtKID:
		r.Value = &api.KGObject_AKID{AKID: o.ValKID()}
	case KtString:
		r.Value = &api.KGObject_AString{AString: o.ValString()}
	case KtTimestamp:
		ts := o.ValTimestamp()
		apiTS := api.KGTimestamp{
			Precision: api.Precision(ts.Precision),
			Value:     ts.Value,
		}
		r.Value = &api.KGObject_ATimestamp{ATimestamp: &apiTS}
	case KtNil:
	default:
		panic(fmt.Sprintf("KGObject.ToAPIObject with unexpected type %v", o.ValueType()))
	}
	return r
}

// KGObjectFromBytes constructs a new KGObject instance from the provided serialized state
// it may return an error if it doesn't contain a valid encoded KGObject state.
func KGObjectFromBytes(data []byte) (KGObject, error) {
	res := KGObject{}
	if err := isKGObject(data); err != nil {
		return res, err
	}
	err := res.Unmarshal(data)
	return res, err
}

// AsBytes returns a serialized state of this KGObject. You can take these bytes
// and pass them to KGObjectFrom to re-hydrate a KGObject instance
func (o *KGObject) AsBytes() []byte {
	b, _ := o.Marshal()
	return b
}

// AsString returns a serialized state of this KGObject as a non-printable string.
func (o KGObject) AsString() string {
	if o.IsType(KtNil) {
		return "\u0000"
	}
	return o.value
}

// WriteOpts contains options that control the data written out by the
// KGOBject.WriteTo method.
type WriteOpts struct {
	// NoLangID Will skip writing the null separator and LanguageID for
	// KGObjects of type string.
	NoLangID bool
}

// WriteTo will write all or some prefix of the encoded data that describes this
// KGObject, the 'opts' can be used to control exactly what is written
func (o KGObject) WriteTo(buff *bytes.Buffer, opts WriteOpts) {
	if opts.NoLangID && o.IsType(KtString) {
		// 1 for the null separator, 19 for the langID
		buff.WriteString(o.value[0 : len(o.value)-20])
		return
	}
	buff.WriteString(o.AsString())
}
