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

package plandef

import (
	"testing"

	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/cmp"
	"github.com/stretchr/testify/assert"
)

func Test_Enumerate_String(t *testing.T) {
	assert := assert.New(t)
	op := Enumerate{
		Output: &Variable{Name: "var"},
	}
	assert.Equal("Enumerate ?var {}", op.String())
	op.Values = append(op.Values, &Literal{
		Value: rpc.AString("tires", 0),
	})
	assert.Equal(`Enumerate ?var {"tires"}`, op.String())
	op.Values = append(op.Values, &Literal{
		Value: rpc.AString("rims", 0),
	})
	assert.Equal(`Enumerate ?var {"tires", "rims"}`, op.String())
	op.Values = append(op.Values, &Literal{
		Value: rpc.AString("spinners", 0),
	})
	assert.Equal(`Enumerate ?var {"tires", "rims", "spinners"}`, op.String())
	op.Values = append(op.Values, &Literal{
		Value: rpc.AString("cones", 0),
	})
	assert.Equal(`Enumerate ?var {"tires", "rims", "spinners", "cones"}`, op.String())
	op.Values = append(op.Values, &Literal{
		Value: rpc.AString("spikes", 0),
	})
	assert.Equal(`Enumerate ?var {"tires", "rims", "spinners", ...}`, op.String())
	op.Values = append(op.Values, &Literal{
		Value: rpc.AString("flamethrowers", 0),
	})
	assert.Equal(`Enumerate ?var {"tires", "rims", "spinners", ...}`, op.String())
	op.Values = []FixedTerm{
		&OID{Value: 32},
	}
	assert.Equal("Enumerate ?var {#32}", op.String())
}

func Test_Enumerate_Key(t *testing.T) {
	assert := assert.New(t)

	op := &Enumerate{
		Output: &Variable{Name: "var"},
	}
	assert.Equal("Enumerate ?var", cmp.GetKey(op))

	op.Values = []FixedTerm{
		&Literal{Value: rpc.AString("tires", 0)},
		&Literal{Value: rpc.AString("rims", 0)},
		&Literal{Value: rpc.AString("spinners", 0)},
		&Literal{Value: rpc.AString("cones", 0)},
		&Literal{Value: rpc.AString("spikes", 0)},
	}
	assert.Equal(`Enumerate ?var string:"tires" string:"rims" `+
		`string:"spinners" string:"cones" string:"spikes"`,
		cmp.GetKey(op))
}
