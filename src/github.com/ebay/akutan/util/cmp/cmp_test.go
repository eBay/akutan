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

package cmp

import (
	"math"
	"testing"
)

func Test_Int64(t *testing.T) {
	ints := []int64{-1, 0, 1, 1234, math.MaxInt64 - 1, math.MaxInt64}
	testint64Values(t, ints)
}

func Test_UInt64(t *testing.T) {
	ints := []uint64{0, 1, 1234, math.MaxInt64 - 1, math.MaxInt64, math.MaxUint64 - 1, math.MaxUint64}
	testuint64Values(t, ints)
}

func Test_Int32(t *testing.T) {
	ints := []int32{-1, 0, 1, 1234, math.MaxInt32 - 1, math.MaxInt32}
	testint32Values(t, ints)
}
func Test_UInt32(t *testing.T) {
	ints := []uint32{0, 1, 1234, math.MaxInt32 - 1, math.MaxInt32}
	testuint32Values(t, ints)
}

func Test_Int(t *testing.T) {
	ints := []int{-1, 0, 1, 1234, math.MaxInt32 - 1, math.MaxInt32}
	testintValues(t, ints)
}

func Test_String(t *testing.T) {
	s := []string{"", "a", "abba", "alice", "bob", "eve", "zebra", "zzzzzz"}
	teststringValues(t, s)
}
