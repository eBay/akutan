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
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/ebay/beam/logentry"
)

// TypePrefix returns a byte slice that contain a prefix of the encoding that contains
// the type. This will contain the type indicator, and for types that have units, will
// also contain the units ID.
func (o KGObject) TypePrefix() []byte {
	vt := o.ValueType()
	if vt == KtNil {
		return []byte{byte(KtNil)}
	}
	if vt != KtString && vt != KtKID {
		return []byte(o.value[:20])
	}
	return []byte(o.value[:1])
}

// ValueType returns the contained KGObject type.
func (o KGObject) ValueType() KGObjectType {
	if len(o.value) > 0 {
		return KGObjectType(o.value[0])
	}
	return KtNil
}

// IsType returns true if this KGObject contains the indicated type
func (o KGObject) IsType(t KGObjectType) bool {
	return o.ValueType() == t
}

// ValBool returns the contained bool value if the type is KtBool
// otherwise it returns false
func (o KGObject) ValBool() bool {
	// TODO: Why does Bool has a units field?
	if o.ValueType() == KtBool {
		return o.value[1+19] > 0
	}
	return false
}

// ValInt64 returns the contained Int64 value if the type is KtInt64
// otherwise it returns 0
func (o KGObject) ValInt64() int64 {
	if o.ValueType() == KtInt64 {
		return int64(binary.BigEndian.Uint64([]byte(o.value[20:28])) ^ maskMsbOnly)
	}
	return 0
}

// ValFloat64 returns the contained Float64 value if the type is KtFloat64.
// otherwise it returns 0
func (o KGObject) ValFloat64() float64 {
	if o.ValueType() == KtFloat64 {
		u := binary.BigEndian.Uint64([]byte(o.value[20:28]))
		if u&maskMsbOnly != 0 {
			u = u ^ maskMsbOnly
		} else {
			u = u ^ maskAllBits
		}
		return math.Float64frombits(u)
	}
	return 0
}

// ValString returns the contained string value if the type is KtString
// otherwise it returns ""
// Don't get this confused with String() which returns a human readable
// representation of the Object.
func (o KGObject) ValString() string {
	if o.ValueType() == KtString {
		return string(o.value[1 : len(o.value)-20])
	}
	return ""
}

// ValKID returns the contained KID value if the type is KtKID
// otherwise it returns 0
func (o KGObject) ValKID() uint64 {
	if o.ValueType() == KtKID {
		return binary.BigEndian.Uint64([]byte(o.value[1:]))
	}
	return 0
}

// ValTimestamp returns the contained Timestamp if the type is KtTimestamp
// otherwise it returns an empty/zero value KGTimestamp
func (o KGObject) ValTimestamp() logentry.KGTimestamp {
	if o.ValueType() == KtTimestamp {
		p := logentry.TimestampPrecision(o.value[31])
		year := int(binary.BigEndian.Uint16([]byte(o.value[20:22])))
		month := int(o.value[22])
		day := int(o.value[23])
		hour := int(o.value[24])
		mins := int(o.value[25])
		secs := int(o.value[26])
		nano := int(binary.BigEndian.Uint32([]byte(o.value[27:31])))
		return logentry.KGTimestamp{
			Precision: p,
			Value:     time.Date(year, time.Month(month), day, hour, mins, secs, nano, time.UTC),
		}
	}
	return logentry.KGTimestamp{}
}

// LangID returns the Language ID if the contained type is KtString,
// otherwise it returns 0
func (o KGObject) LangID() uint64 {
	if o.ValueType() == KtString {
		v, err := parseUInt([]byte(o.value), len(o.value)-19, len(o.value))
		if err != nil {
			panic(fmt.Sprintf("Unable to decode LangID from rpc.KGObject: %v", err))
		}
		return v
	}
	return 0
}

// UnitID returns the Units ID for the contained types that have units (bool, int, float, timestamp)
// otherwise it returns 0
func (o KGObject) UnitID() uint64 {
	vt := o.ValueType()
	if vt != KtString && vt != KtKID {
		unit, err := parseUInt([]byte(o.value), 1, 20)
		if err != nil {
			panic(fmt.Sprintf("Unable to decode UnitID from rpc.KGObject: %v", err))
		}
		return unit
	}
	return 0
}
