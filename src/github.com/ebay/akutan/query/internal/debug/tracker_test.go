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

package debug

import (
	"errors"
	"strings"
	"testing"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/query/parser"
	"github.com/ebay/akutan/util/clocks"
	"github.com/stretchr/testify/assert"
)

func Test_ExtractKIDs(t *testing.T) {
	q := parser.Query{
		Where: []*parser.Quad{{
			ID:        &parser.Nil{},
			Subject:   &parser.Variable{Name: "s"},
			Predicate: &parser.LiteralID{Value: 42, Hint: "size"},
			Object:    &parser.LiteralID{Value: 100, Hint: "small"},
		}, {
			ID:        &parser.Nil{},
			Subject:   &parser.LiteralID{Value: 101},
			Predicate: &parser.LiteralInt{Value: 102},
			Object:    &parser.LiteralString{Value: "103"},
		}},
	}
	actual := extractKIDs(&q)
	exp := map[uint64]string{
		42:  "size",
		100: "small",
	}
	assert.Equal(t, exp, actual)
}

// debugTracker shouldn't barf if a query fails at one of the steps and not all
// the Tracker calls are made to it.
func Test_DebugTrackerIncompleteQuery(t *testing.T) {
	out := strings.Builder{}
	d := New(true, &out, clocks.NewMock(), blog.Index(1), "not a valid query\n")
	d.Parsed(nil, errors.New("Invalid query"))
	d.Close()
	assert.Equal(t, `
Started at: 1970-01-01 00:00:00.000000 UTC
Parsing   0s
Query Ended at: 1970-01-01 00:00:00.000000 UTC
Total: 0s

Query @ Index: 1
not a valid query

Parsed Query:
Error: Invalid query

`, "\n"+out.String())
}
