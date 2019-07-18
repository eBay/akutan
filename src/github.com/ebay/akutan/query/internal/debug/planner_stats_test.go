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
	"strings"
	"testing"

	"github.com/ebay/akutan/rpc"
	"github.com/stretchr/testify/assert"
)

func Test_PlannerStatsFmt(t *testing.T) {
	s := plannerStats{
		kids: map[uint64]string{
			42: "<size>",
			43: "rdf:type",
			44: "Apple",
		},
	}
	assert.Equal(t, "rdf:type", s.fmtKID(43))
	assert.Equal(t, "<size>", s.fmtKID(42))
	assert.Equal(t, "#123", s.fmtKID(123))

	assert.Equal(t, "Apple", s.fmtObj(rpc.AKID(44)))
	assert.Equal(t, "#123", s.fmtObj(rpc.AKID(123)))
	assert.Equal(t, `"Apple"`, s.fmtObj(rpc.AString("Apple", 0)))
}

func Test_PlannerStatsDump(t *testing.T) {
	s := plannerStats{
		used: []statItem{{
			description: "LookupS <tree>",
			value:       5,
		}, {
			description: "InferPO <size> 15.0",
			value:       10,
		}, {
			description: "LookupPO rdf:type types:vehicle",
			value:       5000,
		}},
	}
	out := strings.Builder{}
	s.dump(&out)
	assert.Equal(t, `
InferPO <size> 15.0             10
LookupPO rdf:type types:vehicle 5000
LookupS <tree>                  5
`, "\n"+out.String())

	// Even if there are duplicated entries in used, they should be removed during dump.
	// This can happen if the planner asks for the same statistic more than one.
	s.used = append(s.used, s.used...)
	out = strings.Builder{}
	s.dump(&out)
	assert.Equal(t, `
InferPO <size> 15.0             10
LookupPO rdf:type types:vehicle 5000
LookupS <tree>                  5
`, "\n"+out.String())
}
