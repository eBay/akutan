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

package parser

import (
	"fmt"
	"strings"

	"github.com/ebay/akutan/util/errors"
	"github.com/ebay/akutan/util/unicode"
)

// These are possible values for ParseInsert formats.
const (
	// InsertFormatTSV takes one fact per line, consisting of three or four
	// whitespace-separated columns. It's far more forgiving about whitespace
	// than normal tab-separated values files. Blank lines and lines consisting
	// of # comments are allowed.
	//
	// <dont_care> may be used as a dummy fact ID.
	//
	// Variables like ?var capture a fact ID and must be used as subject or
	// object in a subsequent fact.
	InsertFormatTSV = "tsv"
)

// Insert is the result of ParseInsert.
type Insert struct {
	// The possible types for each Quad are restricted significantly.
	//
	// Each ID is either Nil or Variable.
	//
	// Each Subject is either LiteralID, QName, Entity, or Variable.
	//
	// Each Predicate is either LiteralID, QName, or Entity.
	//
	// Each Object is either LiteralID, QName, Entity, Variable, or a primitive
	// literal type (LiteralBool, LiteralInt, LiteralFloat, LiteralString,
	// LiteralTime).
	//
	// For QName, Entity, unit, or language values, only the string form is
	// given.
	//
	// All returned Quads will have a MatchSpecificity of MatchRequired.
	Facts []*Quad
}

func (insert *Insert) String() string {
	var str strings.Builder
	for i, quad := range insert.Facts {
		if i > 0 {
			str.WriteByte('\n')
		}
		str.WriteString(quad.String())
	}
	return str.String()
}

// ParseInsert parses 'input' for inserting facts into the graph. The given
// format identifies the syntax of the input; the possible values are defined as
// InsertFormat* constants.
func ParseInsert(format string, input string) (*Insert, error) {
	insert := &Insert{}
	switch format {
	case InsertFormatTSV:
		input = unicode.Normalize(input)
		// It just so happens that the legacy query parser does an OK job at
		// parsing whitespace-separated columns of input. It barfs on empty
		// input, however.
		if strings.TrimSpace(input) != "" {
			query, err := Parse(QueryFactPattern, input)
			if err != nil {
				return nil, fmt.Errorf("parser: error parsing as legacy query: %v", err)
			}
			insert.Facts = query.Where
		}
		patchDontCares(insert)

	default:
		return nil, fmt.Errorf("parser: unsupported input format: %v", format)
	}

	err := validateInsertTypes(insert)
	if err != nil {
		return nil, err
	}
	err = validateInsertVars(insert)
	if err != nil {
		return nil, err
	}
	return insert, nil
}

// patchDontCares replaces any Fact ID that is <dont_care> with Nil.
func patchDontCares(insert *Insert) {
	for _, fact := range insert.Facts {
		if id, ok := fact.ID.(*Entity); ok {
			if id.Value == "dont_care" {
				fact.ID = &Nil{}
			}
		}
	}
}

// validateInsertTypes checks whether all of the Quads have acceptable types to
// be inserted as static facts.
func validateInsertTypes(insert *Insert) error {
	for i, fact := range insert.Facts {
		if fact.Specificity != MatchRequired {
			return fmt.Errorf("parser: unexpected specificity (%v) for fact %v",
				fact.Specificity, i+1)
		}

		switch id := fact.ID.(type) {
		case *Nil:
		case *Variable:
			// ok
		default:
			return fmt.Errorf("parser: unexpected type %T as fact ID of fact %v",
				id, i+1)
		}

		switch subject := fact.Subject.(type) {
		case *LiteralID:
		case *QName:
		case *Entity:
		case *Variable:
			// ok
		default:
			return fmt.Errorf("parser: unexpected type %T as subject of fact %v",
				subject, i+1)
		}

		switch predicate := fact.Predicate.(type) {
		case *LiteralID:
		case *QName:
		case *Entity:
			// ok
		default:
			return fmt.Errorf("parser: unexpected type %T as predicate of fact %v",
				predicate, i+1)
		}

		switch object := fact.Object.(type) {
		case *LiteralID:
		case *QName:
		case *Entity:
		case *Variable:
		case *LiteralBool:
		case *LiteralFloat:
		case *LiteralInt:
		case *LiteralString:
		case *LiteralTime:
			// ok
		default:
			return fmt.Errorf("parser: unexpected type %T as object of fact %v",
				object, i+1)
		}
	}
	return nil
}

// validateInsertVars checks that variables are used correctly. Each variable
// must capture a fact ID before it's used. Each variable must be used. No
// variable may capture more than one fact ID.
func validateInsertVars(insert *Insert) error {
	vars := make(map[string]int) // value is number of uses
	captured := func(name string) bool {
		_, exists := vars[name]
		return exists
	}
	handleTerm := func(term Term) error {
		if v, ok := term.(*Variable); ok {
			if !captured(v.Name) {
				return fmt.Errorf("parser: variable ?%v used but not (yet) captured", v.Name)
			}
			vars[v.Name]++
		}
		return nil
	}
	for _, fact := range insert.Facts {
		if v, ok := fact.ID.(*Variable); ok {
			if captured(v.Name) {
				return fmt.Errorf("parser: variable ?%v captured more than once", v.Name)
			}
			vars[v.Name] = 0
		}
		err := errors.Any(
			handleTerm(fact.Subject),
			handleTerm(fact.Predicate),
			handleTerm(fact.Object),
		)
		if err != nil {
			return err
		}
	}
	for name, uses := range vars {
		if uses == 0 {
			return fmt.Errorf("parser: variable ?%v captured but not used", name)
		}
	}
	return nil
}
