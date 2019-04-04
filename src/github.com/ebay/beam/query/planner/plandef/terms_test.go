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
	"fmt"
	"testing"
	"unsafe"

	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/cmp"
	"github.com/stretchr/testify/assert"
)

func Test_DontCare_unequal(t *testing.T) {
	assert := assert.New(t)
	dc1 := new(DontCare)
	dc2 := new(DontCare)
	assert.False(dc1 == dc2)
	// Even if the above assertion holds on this machine, we need the size of the
	// struct to be at least one byte to guarantee inequality per the Go spec.
	assert.True(unsafe.Sizeof(DontCare{}) > 0)
}
func Test_DontCare_StringKey(t *testing.T) {
	term := new(DontCare)
	assert.Equal(t, "_", term.String())
	assert.Equal(t, "_", cmp.GetKey(term))
}

func Test_Variable_StringKey(t *testing.T) {
	term := &Variable{Name: "foo"}
	assert.Equal(t, "?foo", term.String())
	assert.Equal(t, "?foo", cmp.GetKey(term))
}

func Test_Binding_StringKey(t *testing.T) {
	term := &Binding{Var: &Variable{Name: "foo"}}
	assert.Equal(t, "$foo", term.String())
	assert.Equal(t, "$foo", cmp.GetKey(term))
}

func Test_OID_String(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("#30", fmt.Sprint(&OID{Value: 30}))
	assert.Equal("<hi>", fmt.Sprint(&OID{Value: 30, Hint: "<hi>"}))
}

func Test_OID_Key(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("#30", cmp.GetKey(&OID{Value: 30}))
	assert.Equal("#30", cmp.GetKey(&OID{Value: 30, Hint: "<hi>"}))
}

func Test_Literal_StringKey(t *testing.T) {
	term := &Literal{Value: rpc.AInt64(30, 0)}
	assert.Equal(t, "30", term.String())
	assert.Equal(t, "int64:30", cmp.GetKey(term))
}
