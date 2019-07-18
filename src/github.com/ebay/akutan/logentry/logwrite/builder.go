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

// InsertFactBuilder is used to construct new logentry.InsertFact instances. It
// can be used to create many facts, however it is not safe for concurrent use.
// By default each call to Fact() will auto assign it an incrementing
// FactIDOffset, if you want to explicitly manage the offsets you can call
// SetAutoFactID(false) to disable that behavior.
type InsertFactBuilder struct {
	fact             logentry.InsertFact
	lastFactIDOffset int32
	disableAutoID    bool
}

// SetAutoFactID will enable or disable the automatic setting of the
// FactIDOffset. If set each call to Fact() will automatically assign an
// increasing value to FactIDOffset if FactIDOffset was otherwise unset. This is
// enabled by default.
func (i *InsertFactBuilder) SetAutoFactID(enabled bool) *InsertFactBuilder {
	i.disableAutoID = !enabled
	return i
}

// FactID sets a specific FactIDOffset
func (i *InsertFactBuilder) FactID(offset int32) *InsertFactBuilder {
	i.fact.FactIDOffset = offset
	return i
}

// SID sets a specific KID for the Subject.
func (i *InsertFactBuilder) SID(kid uint64) *InsertFactBuilder {
	i.fact.Subject = koKid(kid)
	return i
}

// SOffset sets a specific Offset for the Subject.
func (i *InsertFactBuilder) SOffset(offset int32) *InsertFactBuilder {
	i.fact.Subject = koOffset(offset)
	return i
}

// PID sets a specific KID for the Predicate.
func (i *InsertFactBuilder) PID(kid uint64) *InsertFactBuilder {
	i.fact.Predicate = koKid(kid)
	return i
}

// POffset sets a specific Offset for the Predicate.
func (i *InsertFactBuilder) POffset(offset int32) *InsertFactBuilder {
	i.fact.Predicate = koOffset(offset)
	return i
}

// OKID sets a specific KID for the Object.
func (i *InsertFactBuilder) OKID(kid uint64) *InsertFactBuilder {
	i.fact.Object = AKID(kid)
	return i
}

// OString sets a specific String and LangID for the Object.
func (i *InsertFactBuilder) OString(s string, langID uint64) *InsertFactBuilder {
	i.fact.Object = AString(s, langID)
	return i
}

// OInt64 sets a specific Int64 and UnitID for the Object.
func (i *InsertFactBuilder) OInt64(v int64, unitID uint64) *InsertFactBuilder {
	i.fact.Object = AInt64(v, unitID)
	return i
}

// OFloat64 sets a specific Float64 and UnitID for the Object.
func (i *InsertFactBuilder) OFloat64(f float64, unitID uint64) *InsertFactBuilder {
	i.fact.Object = AFloat64(f, unitID)
	return i
}

// OBool sets a specific Bool and UnitID for the Object.
func (i *InsertFactBuilder) OBool(b bool, unitID uint64) *InsertFactBuilder {
	i.fact.Object = ABool(b, unitID)
	return i
}

// OTimestamp sets a specific Timestamp and UnitID for the Object.
func (i *InsertFactBuilder) OTimestamp(v time.Time, p logentry.TimestampPrecision, unitID uint64) *InsertFactBuilder {
	i.fact.Object = ATimestamp(v, p, unitID)
	return i
}

// OOffset sets a specific KID Offset for the Object.
func (i *InsertFactBuilder) OOffset(offset int32) *InsertFactBuilder {
	i.fact.Object = AKIDOffset(offset)
	return i
}

// Fact returns a new InsertFact constructed from the prior calls to S*, P*, O*
// If AutoFactID is enabled (the default) an incrementing value for the
// FactIDOffset will be set if it hasn't been explicitly set. If AutoFactID is
// disabled, the FactID() method is used to specify the FactIDOffset that should
// be populated in the Fact.
//
// Calls to Fact do not reset the state of any the fields. Subsequent calls to
// Fact will use the prior set value if not explicitly set since the last call
// to Fact.
func (i *InsertFactBuilder) Fact() logentry.InsertFact {
	result := i.fact
	if !i.disableAutoID && result.FactIDOffset == 0 {
		result.FactIDOffset = i.lastFactIDOffset + 1
		i.lastFactIDOffset++
	}
	return result
}
