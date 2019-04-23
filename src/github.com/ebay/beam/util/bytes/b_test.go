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

package bytes

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Copy(t *testing.T) {
	assert.Nil(t, Copy(nil))
	assert.Equal(t, []byte{}, Copy([]byte{}))
	x := []byte{1, 2, 3}
	c := Copy(x)
	assert.Equal(t, x, c)
	x[0] = 42
	assert.Equal(t, []byte{1, 2, 3}, c)
}

func Test_Concat(t *testing.T) {
	assert.Equal(t, []byte{}, Concat())
	a := []byte("aaa")
	b := []byte("bbbb")
	c := []byte("ccccc")
	assert.Equal(t, []byte("aaabbbbccccc"), Concat(a, b, c))
}
