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

package main

import (
	"fmt"
	"strings"

	"github.com/ebay/akutan/api"
	"github.com/ebay/akutan/query/parser"
	"github.com/ebay/akutan/rpc"
)

func parseOperator(in string) (parser.Operator, error) {
	// try real API names first
	if op, exists := rpc.Operator_value[in]; exists {
		return parser.Operator{Value: rpc.Operator(op)}, nil
	}
	// try akutan-client names
	names := map[string]rpc.Operator{
		"<":           rpc.OpLess,
		"<=":          rpc.OpLessOrEqual,
		"=":           rpc.OpEqual,
		">":           rpc.OpGreater,
		">=":          rpc.OpGreaterOrEqual,
		"rangeincinc": rpc.OpRangeIncInc,
		"rangeincexc": rpc.OpRangeIncExc,
		"rangeexcinc": rpc.OpRangeExcInc,
		"rangeexcexc": rpc.OpRangeExcExc,
		"prefix":      rpc.OpPrefix,
	}
	if op, exists := names[strings.ToLower(in)]; exists {
		return parser.Operator{Value: op}, nil
	}
	// try parsing via ql
	term, err := parser.ParseTerm(in)
	if err != nil {
		return parser.Operator{}, err
	}
	if op, ok := term.(*parser.Operator); ok {
		return *op, nil
	}
	return parser.Operator{}, fmt.Errorf("invalid operator: %s", in)
}

func parseLiteralObject(in string, langID, unitID uint64) (api.KGObject, error) {
	if in == "" {
		return api.KGObject{}, nil
	}
	literal, err := parser.ParseLiteral(in)
	if err != nil {
		return api.KGObject{}, err
	}
	obj := literal.Literal()
	obj.LangID = langID
	obj.UnitID = unitID
	return *obj, nil
}

// parseLiteralID parses an input string into a KID or returns an error.
func parseLiteralID(in string) (parser.LiteralID, error) {
	if in == "" {
		return parser.LiteralID{}, nil
	}
	literal, err := parser.ParseLiteral(in)
	if err != nil {
		return parser.LiteralID{}, err
	}
	if lint, ok := literal.(*parser.LiteralInt); ok {
		if lint.Value > 0 {
			return parser.LiteralID{Value: uint64(lint.Value)}, nil
		}
		return parser.LiteralID{}, fmt.Errorf("unable to parse literal id: '%s'", in)
	}
	if lid, ok := literal.(*parser.LiteralID); ok {
		return *lid, nil
	}
	return parser.LiteralID{}, fmt.Errorf("invalid literal id: '%s'", in)
}
