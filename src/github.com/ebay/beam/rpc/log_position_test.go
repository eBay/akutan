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
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MinLogPosition(t *testing.T) {
	p1 := LogPosition{Index: 10, Version: 1}
	p2 := LogPosition{Index: 10, Version: 3}
	p3 := LogPosition{Index: 11, Version: 1}
	p4 := LogPosition{Index: 11, Version: 3}
	assert.Equal(t, p1, MinPosition(p1, p3))
	assert.Equal(t, p1, MinPosition(p1, p4))
	assert.Equal(t, p1, MinPosition(p3, p1))
	assert.Equal(t, p1, MinPosition(p4, p1))
	assert.Equal(t, p2, MinPosition(p2, p3))
	assert.Equal(t, p2, MinPosition(p4, p2))
}

func Test_LogPositionString(t *testing.T) {
	p := LogPosition{Index: 1234, Version: 3}
	assert.Equal(t, "v3 @ 1234", p.String())
}
