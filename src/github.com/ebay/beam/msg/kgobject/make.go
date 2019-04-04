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

// Package kgobject contains helper methods to construct common api KGObject instances
package kgobject

import (
	"time"

	"github.com/ebay/beam/api"
)

// AString returns a new KGObject instance containing the supplied string and language ID.
func AString(s string, langID uint64) api.KGObject {
	return api.KGObject{LangID: langID, Value: &api.KGObject_AString{AString: s}}
}

// AFloat64 returns a new KGObject instance containing the supplied float and Units ID.
func AFloat64(f float64, uintID uint64) api.KGObject {
	return api.KGObject{UnitID: uintID, Value: &api.KGObject_AFloat64{AFloat64: f}}
}

// AInt64 returns a new KGObject instance containing the supplied int and Units ID.
func AInt64(i int64, uintID uint64) api.KGObject {
	return api.KGObject{UnitID: uintID, Value: &api.KGObject_AInt64{AInt64: i}}
}

// ATimestampY returns a new KGObject instance containing a Timestamp for the specified year and Units ID.
func ATimestampY(year int, uintID uint64) api.KGObject {
	return ATimestamp(time.Date(year, time.January, 1, 0, 0, 0, 0, time.UTC), api.Year, uintID)
}

// ATimestampYM returns a new KGObject instance containing a Timestamp for the specified year, month and Units ID.
func ATimestampYM(year int, month int, uintID uint64) api.KGObject {
	return ATimestamp(time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC), api.Month, uintID)
}

// ATimestampYMD returns a new KGObject instance containing a Timestamp for the specified year, month, day and Units ID.
func ATimestampYMD(year int, month int, day int, uintID uint64) api.KGObject {
	return ATimestamp(time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC), api.Day, uintID)
}

// ATimestampYMDH returns a new KGObject instance containing a Timestamp for the specified year, month, day, hour and Units ID.
func ATimestampYMDH(year, month, day, hour int, uintID uint64) api.KGObject {
	return ATimestamp(time.Date(year, time.Month(month), day, hour, 0, 0, 0, time.UTC), api.Hour, uintID)
}

// ATimestampYMDHM returns a new KGObject instance containing a Timestamp for the specified year, month, day, hour, minutes and Units ID.
func ATimestampYMDHM(year, month, day, hour, minute int, uintID uint64) api.KGObject {
	return ATimestamp(time.Date(year, time.Month(month), day, hour, minute, 0, 0, time.UTC), api.Minute, uintID)
}

// ATimestampYMDHMS returns a new KGObject instance containing a Timestamp for the specified year, month, day, hour, minutes, seconds and Units ID.
func ATimestampYMDHMS(year, month, day, hour, minute, second int, uintID uint64) api.KGObject {
	return ATimestamp(time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC), api.Second, uintID)
}

// ATimestampYMDHMSN returns a new KGObject instance containing a Timestamp for the specified year, month, day, hour, minutes, seconds, nanoseonds, and Units ID.
func ATimestampYMDHMSN(year, month, day, hour, minute, second, nsec int, uintID uint64) api.KGObject {
	return ATimestamp(time.Date(year, time.Month(month), day, hour, minute, second, nsec, time.UTC), api.Nanosecond, uintID)
}

// ATimestamp returns a new KGObject instance containing a ATimestamp for the supplied dateTime, precision and Units ID.
func ATimestamp(t time.Time, p api.Precision, uintID uint64) api.KGObject {
	return api.KGObject{UnitID: uintID, Value: &api.KGObject_ATimestamp{ATimestamp: &api.KGTimestamp{Precision: p, Value: t}}}
}

// ABool returns an new KGObject instance containing a Boolean value and Units ID.
func ABool(b bool, uintID uint64) api.KGObject {
	return api.KGObject{UnitID: uintID, Value: &api.KGObject_ABool{ABool: b}}
}

// AKID returns an new KGObject instance containing a AKID value.
func AKID(kid uint64) api.KGObject {
	return api.KGObject{Value: &api.KGObject_AKID{AKID: kid}}
}
