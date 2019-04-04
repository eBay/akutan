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
	"strings"
)

func (o Operator) String() string {
	switch o {
	case OpEqual:
		return "="
	case OpNotEqual:
		return "!="
	case OpLess:
		return "<"
	case OpLessOrEqual:
		return "<="
	case OpGreater:
		return ">"
	case OpGreaterOrEqual:
		return ">="
	case OpRangeIncExc:
		return "rangeIncExc"
	case OpRangeIncInc:
		return "rangeIncInc"
	case OpRangeExcInc:
		return "rangeExcInc"
	case OpRangeExcExc:
		return "rangeExcExc"
	case OpPrefix:
		return "prefix"
	case OpIn:
		return "in"
	default:
		return fmt.Sprintf("unknown(%d)", o)
	}
}

// Key implements cmp.Key.
func (o Operator) Key(b *strings.Builder) {
	// o.String() is cheap for any expected inputs.
	b.WriteString(o.String())
}
