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

package api

import (
	"fmt"
	"strings"
)

var (
	patterns = map[Precision]string{
		Year:       "2006",
		Month:      "2006-01",
		Day:        "2006-01-02",
		Hour:       "2006-01-02T15",
		Minute:     "2006-01-02T15:04",
		Second:     "2006-01-02T15:04:05",
		Nanosecond: "2006-01-02T15:04:05.999999999",
	}
)

func (m QueryFactsRequest) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "QueryFactsRequest{")
	fmt.Fprintf(&buf, "\n Index: %d", m.Index)
	fmt.Fprintf(&buf, "\n Query: '%s'", strings.Replace(m.Query, "\n", "; ", -1))
	fmt.Fprintf(&buf, "\n}")
	return buf.String()
}

// String returns a string version of the KGValue in the same format that
// the query parser expects.
func (m KGValue) String() string {
	addUnit := func(val string, unit *KGID) string {
		if unit == nil || unit.QName == "" {
			return val
		}
		return fmt.Sprintf(`%s^^%s`, val, unit.QName)
	}
	switch v := m.Value.(type) {
	case *KGValue_Bool:
		return addUnit(fmt.Sprintf("%v", v.Bool.Value), v.Bool.Unit)
	case *KGValue_Float64:
		return addUnit(fmt.Sprintf("%f", v.Float64.Value), v.Float64.Unit)
	case *KGValue_Int64:
		return addUnit(fmt.Sprintf("%d", v.Int64.Value), v.Int64.Unit)
	case *KGValue_Node:
		return v.Node.QName
	case *KGValue_Str:
		val := fmt.Sprintf(`"%s"`, v.Str.Value)
		if v.Str.Lang != nil && v.Str.Lang.QName != "" {
			val = val + "@" + v.Str.Lang.QName
		}
		return val
	case *KGValue_Timestamp:
		return v.Timestamp.String()
	case nil:
		return "(nil)"
	default:
		panic(fmt.Sprintf("Unknown KGValue value type: %T", m.Value))
	}
}

func (m KGObject) String() string {
	switch m.Value.(type) {
	case *KGObject_ABool:
		return fmt.Sprintf("%v", m.GetABool())
	case *KGObject_AFloat64:
		return fmt.Sprintf("%f", m.GetAFloat64())
	case *KGObject_AInt64:
		return fmt.Sprintf("%d", m.GetAInt64())
	case *KGObject_AKID:
		return fmt.Sprintf("#%d", m.GetAKID())
	case *KGObject_AString:
		return fmt.Sprintf("'%s'", m.GetAString())
	case *KGObject_ATimestamp:
		return m.GetATimestamp().String()
	case nil:
		return "(nil)"
	default:
		panic(fmt.Sprintf("Unknown KGObject value type: %T", m.Value))
	}
}

func (m KGTimestamp) String() string {
	return m.Value.Format(patterns[m.Precision])
}

func (e Precision) String() string {
	switch e {
	case Year:
		return "y"
	case Month:
		return "y-m"
	case Day:
		return "y-m-d"
	case Hour:
		return "y-m-d h"
	case Minute:
		return "y-m-d h:m"
	case Second:
		return "y-m-d h:m:s"
	case Nanosecond:
		return "y-m-d h:m:s.n"
	default:
		panic(fmt.Sprintf("Unknown precision value: %d", e))
	}
}

func (m ResolvedFact) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "{%d", m.Index)
	fmt.Fprintf(&buf, " %d", m.Id)
	fmt.Fprintf(&buf, " %d", m.Subject)
	fmt.Fprintf(&buf, " %d", m.Predicate)
	fmt.Fprintf(&buf, " %s}", m.Object)
	return buf.String()
}
