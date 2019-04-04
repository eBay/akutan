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
	"fmt"
	"math"
	"time"

	"github.com/ebay/beam/api"
	"github.com/ebay/beam/logentry"
)

// KGObjectFromAPI returns a new KGObject instance that is equivilent to the
// supplied API representation of a KGObject. The rpc.KGObject encapsulates
// a binary encoding of the KGObject
func KGObjectFromAPI(from api.KGObject) KGObject {
	if from.Value == nil {
		return KGObject{}
	}
	switch t := from.Value.(type) {
	case *api.KGObject_AString:
		return AString(t.AString, from.LangID)
	case *api.KGObject_AFloat64:
		return AFloat64(t.AFloat64, from.UnitID)
	case *api.KGObject_AInt64:
		return AInt64(t.AInt64, from.UnitID)
	case *api.KGObject_ATimestamp:
		return ATimestamp(t.ATimestamp.Value, logentry.TimestampPrecision(t.ATimestamp.Precision), from.UnitID)
	case *api.KGObject_ABool:
		return ABool(t.ABool, from.UnitID)
	case *api.KGObject_AKID:
		return AKID(t.AKID)
	default:
		panic(fmt.Sprintf("KGObjectFromAPI encountered a KGObject with an unexpected type %T/%v", from.Value, from.Value))
	}
}

// AString returns a new KGObject instance containing the supplied string and language ID.
func AString(s string, langID uint64) KGObject {
	b := new(kgObjectBuilder)
	b.resetAndWriteType(KtString, 1+len(s)+19)
	b.buff.WriteString(s)
	appendUInt64(&b.buff, 19, langID)
	return KGObject{b.buff.String()}
}

// AFloat64 returns a new KGObject instance containing the supplied float and Units ID.
func AFloat64(fv float64, unitID uint64) KGObject {
	b := new(kgObjectBuilder)
	b.resetAndWriteType(KtFloat64, 1+19+8)
	appendUInt64(&b.buff, 19, unitID)
	u := math.Float64bits(fv)
	if fv < 0 {
		u = u ^ maskAllBits
	} else {
		u = u ^ maskMsbOnly
	}
	b.writeUInt64(u)
	return KGObject{b.buff.String()}
}

// AInt64 returns a new KGObject instance containing the supplied int and Units ID.
func AInt64(v int64, unitID uint64) KGObject {
	b := new(kgObjectBuilder)
	b.resetAndWriteType(KtInt64, 1+19+8)
	appendUInt64(&b.buff, 19, unitID)
	b.writeUInt64(uint64(v) ^ maskMsbOnly)
	return KGObject{b.buff.String()}
}

// ATimestampY returns a new KGObject instance containing a Timestamp for the specified year and Units ID.
func ATimestampY(year int, unitID uint64) KGObject {
	return ATimestamp(time.Date(year, time.January, 1, 0, 0, 0, 0, time.UTC), logentry.Year, unitID)
}

// ATimestampYM returns a new KGObject instance containing a Timestamp for the specified year, month and Units ID.
func ATimestampYM(year int, month int, unitID uint64) KGObject {
	return ATimestamp(time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC), logentry.Month, unitID)
}

// ATimestampYMD returns a new KGObject instance containing a Timestamp for the specified year, month, day and Units ID.
func ATimestampYMD(year int, month int, day int, unitID uint64) KGObject {
	return ATimestamp(time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC), logentry.Day, unitID)
}

// ATimestampYMDH returns a new KGObject instance containing a Timestamp for the specified year, month, day, hour and Units ID.
func ATimestampYMDH(year, month, day, hour int, unitID uint64) KGObject {
	return ATimestamp(time.Date(year, time.Month(month), day, hour, 0, 0, 0, time.UTC), logentry.Hour, unitID)
}

// ATimestampYMDHM returns a new KGObject instance containing a Timestamp for the specified year, month, day, hour, minutes and Units ID.
func ATimestampYMDHM(year, month, day, hour, minute int, unitID uint64) KGObject {
	return ATimestamp(time.Date(year, time.Month(month), day, hour, minute, 0, 0, time.UTC), logentry.Minute, unitID)
}

// ATimestampYMDHMS returns a new KGObject instance containing a Timestamp for the specified year, month, day, hour, minutes, seconds and Units ID.
func ATimestampYMDHMS(year, month, day, hour, minute, second int, unitID uint64) KGObject {
	return ATimestamp(time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC), logentry.Second, unitID)
}

// ATimestampYMDHMSN returns a new KGObject instance containing a Timestamp for the specified year, month, day, hour, minutes, seconds, nanoseonds, and Units ID.
func ATimestampYMDHMSN(year, month, day, hour, minute, second, nsec int, unitID uint64) KGObject {
	return ATimestamp(time.Date(year, time.Month(month), day, hour, minute, second, nsec, time.UTC), logentry.Nanosecond, unitID)
}

// ATimestamp returns a new KGObject instance containing a Timestamp for the supplied dateTime, precision and Units ID.
func ATimestamp(v time.Time, p logentry.TimestampPrecision, unitID uint64) KGObject {
	b := new(kgObjectBuilder)
	b.resetAndWriteType(KtTimestamp, 1+19+12)
	appendUInt64(&b.buff, 19, unitID)
	t := v.UTC()
	y, mo, d, h, mi, s, n := 0, time.January, 1, 0, 0, 0, 0
	if p >= logentry.Year {
		y = t.Year()
	}
	if p >= logentry.Month {
		mo = t.Month()
	}
	if p >= logentry.Day {
		d = t.Day()
	}
	if p >= logentry.Hour {
		h = t.Hour()
	}
	if p >= logentry.Minute {
		mi = t.Minute()
	}
	if p >= logentry.Second {
		s = t.Second()
	}
	if p >= logentry.Nanosecond {
		n = t.Nanosecond()
	}
	b.writeUInt16(y)
	b.writeUInt8(int(mo))
	b.writeUInt8(d)
	b.writeUInt8(h)
	b.writeUInt8(mi)
	b.writeUInt8(s)
	b.writeUInt32(n)
	b.writeUInt8(int(p))
	return KGObject{b.buff.String()}
}

// ABool returns an new KGObject intance containing a Boolean value and Units ID.
func ABool(v bool, unitID uint64) KGObject {
	b := new(kgObjectBuilder)
	b.resetAndWriteType(KtBool, 1+19+1)
	appendUInt64(&b.buff, 19, unitID)
	if v {
		b.buff.WriteByte(1)
	} else {
		b.buff.WriteByte(0)
	}
	return KGObject{b.buff.String()}
}

// AKID returns an new KGObject intance containing a KID value.
func AKID(kid uint64) KGObject {
	b := new(kgObjectBuilder)
	b.resetAndWriteType(KtKID, 1+8)
	b.writeUInt64(kid)
	return KGObject{b.buff.String()}
}
