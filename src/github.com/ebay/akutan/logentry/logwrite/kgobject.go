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

package logwrite

import (
	"time"

	"github.com/ebay/akutan/logentry"
)

// koKid returns a KIDOrOffset configured with the supplied KID
func koKid(k uint64) logentry.KIDOrOffset {
	return logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: k}}
}

// koOffset returns a KIDOrOffset configured with the supplied offset
func koOffset(o int32) logentry.KIDOrOffset {
	return logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Offset{Offset: o}}
}

// AString returns a new KGObject instance containing the supplied string and language ID.
func AString(s string, langID uint64) logentry.KGObject {
	return logentry.KGObject{LangID: koKid(langID), Value: &logentry.KGObject_AString{AString: s}}
}

// AFloat64 returns a new KGObject instance containing the supplied float and Units ID.
func AFloat64(f float64, unitID uint64) logentry.KGObject {
	return logentry.KGObject{UnitID: koKid(unitID), Value: &logentry.KGObject_AFloat64{AFloat64: f}}
}

// AInt64 returns a new KGObject instance containing the supplied int and Units ID.
func AInt64(i int64, unitID uint64) logentry.KGObject {
	return logentry.KGObject{UnitID: koKid(unitID), Value: &logentry.KGObject_AInt64{AInt64: i}}
}

// ATimestamp returns a new KGObject instance containing a Timestamp for the
// supplied dateTime, precision and Units ID.
func ATimestamp(t time.Time, p logentry.TimestampPrecision, unitID uint64) logentry.KGObject {
	return logentry.KGObject{
		UnitID: koKid(unitID),
		Value: &logentry.KGObject_ATimestamp{
			ATimestamp: &logentry.KGTimestamp{Precision: p, Value: t},
		},
	}
}

// ABool returns an new KGObject instance containing a Boolean value and Units
// ID.
func ABool(b bool, unitID uint64) logentry.KGObject {
	return logentry.KGObject{UnitID: koKid(unitID), Value: &logentry.KGObject_ABool{ABool: b}}
}

// AKID returns an new KGObject instance containing a KID value.
func AKID(kid uint64) logentry.KGObject {
	return logentry.KGObject{Value: &logentry.KGObject_AKID{AKID: kid}}
}

// AKIDOffset returns a new KGObject instance containing a KID offset.
// The offset describes a KID that is relative to the containing log
// entry
func AKIDOffset(offset int32) logentry.KGObject {
	return logentry.KGObject{Value: &logentry.KGObject_AKIDOffset{AKIDOffset: offset}}
}
