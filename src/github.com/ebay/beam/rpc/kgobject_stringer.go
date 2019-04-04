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
	"strconv"
	"strings"

	"github.com/ebay/beam/logentry"
)

var (
	patterns = map[logentry.TimestampPrecision]string{
		logentry.Year:       "2006",
		logentry.Month:      "2006-01",
		logentry.Day:        "2006-01-02",
		logentry.Hour:       "2006-01-02T15",
		logentry.Minute:     "2006-01-02T15:04",
		logentry.Second:     "2006-01-02T15:04:05",
		logentry.Nanosecond: "2006-01-02T15:04:05.999999999",
	}
)

// String returns a human readable representation of the contained value
// to aid in debugging.
// TODO: this should match the format used in the query language?
func (o KGObject) String() string {
	var b strings.Builder
	o.string(&b)
	return b.String()
}

// string writes the String() output into b.
func (o KGObject) string(b *strings.Builder) {
	b.Grow(32)
	switch o.ValueType() {
	case KtBool:
		if o.ValBool() {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
		if unit := o.UnitID(); unit != 0 {
			b.WriteString("^^#")
			b.WriteString(strconv.FormatUint(unit, 10))
		}

	case KtFloat64:
		s := strconv.FormatFloat(o.ValFloat64(), 'g', -1, 64)
		b.WriteString(s)

		// Add a .0 to disambiguate floats that don't have a decimal or
		// exponent (and aren't NaN or Inf) from integers.
		onlyDigits := true
		for _, r := range s {
			if r < '0' || r > '9' {
				onlyDigits = false
				break
			}
		}
		if onlyDigits {
			b.WriteString(".0")
		}

		if unit := o.UnitID(); unit != 0 {
			b.WriteString("^^#")
			b.WriteString(strconv.FormatUint(unit, 10))
		}

	case KtInt64:
		b.WriteString(strconv.FormatInt(o.ValInt64(), 10))
		if unit := o.UnitID(); unit != 0 {
			b.WriteString("^^#")
			b.WriteString(strconv.FormatUint(unit, 10))
		}

	case KtKID:
		b.WriteByte('#')
		b.WriteString(strconv.FormatUint(o.ValKID(), 10))

	case KtString:
		b.WriteByte('"')
		b.WriteString(o.ValString())
		b.WriteByte('"')
		if lang := o.LangID(); lang != 0 {
			b.WriteString("@#")
			b.WriteString(strconv.FormatUint(lang, 10))
		}

	case KtTimestamp:
		// TODO: An unfortunate ambiguity in the current output is that you
		// can't tell the difference between 2018 as a year or 2018 as an int64.
		val := o.ValTimestamp()
		b.WriteString(val.Value.Format(patterns[val.Precision]))
		if unit := o.UnitID(); unit != 0 {
			b.WriteString("^^#")
			b.WriteString(strconv.FormatUint(unit, 10))
		}

	case KtNil:
		b.WriteString("(nil)")

	default:
		panic(fmt.Sprintf("Unknown KGObject value type: %v", o.ValueType()))
	}
}

// Key implements cmp.Key; it writes the identity key for the object to the
// given strings.Builder. This key has two properties. First, two KGObjects are
// equal if and only if their Key output is equal. Second, the Key output is
// human-readable. Note that the current String() method does not currently have
// the first property (is "2018" a timestamp or an int64?), and the binary
// encoding clearly doesn't have the second property.
func (o KGObject) Key(b *strings.Builder) {
	switch o.ValueType() {
	case KtBool:
		b.WriteString("bool:")
		o.string(b)
	case KtFloat64:
		b.WriteString("float64:")
		o.string(b)
	case KtInt64:
		b.WriteString("int64:")
		o.string(b)
	case KtKID:
		b.WriteString("kid:")
		o.string(b)
	case KtString:
		b.WriteString("string:")
		o.string(b)
	case KtTimestamp:
		b.WriteString("timestamp:")
		o.string(b)
	case KtNil:
		b.WriteString("nil")
	default:
		panic(fmt.Sprintf("Unknown KGObject value type: %v", o.ValueType()))
	}
}
