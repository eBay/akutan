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

package keys

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_parseUInt(t *testing.T) {
	pad := []byte("999999")
	for _, v := range []uint64{0, 1, 9, 10, 12, 123, 999, 12345, math.MaxInt16, math.MaxInt32, math.MaxInt64} {
		d := strconv.FormatUint(v, 10)
		parsed, err := parseUInt([]byte(d), 0, 1000)
		assert.NoError(t, err)
		assert.Equal(t, v, parsed)

		parsed, err = parseUInt(append(pad, []byte(d)...), len(pad), 1000)
		assert.NoError(t, err)
		assert.Equal(t, v, parsed)
	}
	pv, err := parseUInt([]byte("12334x"), 0, 2)
	assert.NoError(t, err)
	assert.Equal(t, uint64(12), pv)

	pv, err = parseUInt([]byte("12334x"), 2, 4)
	assert.NoError(t, err)
	assert.Equal(t, uint64(33), pv)

	_, err = parseUInt([]byte("12334x"), 0, 100)
	assert.EqualError(t, err, "unable to parse '12334x' into an int, unexpected char 'x'")

	_, err = parseUInt([]byte("x12334"), 0, 100)
	assert.EqualError(t, err, "unable to parse 'x12334' into an int, unexpected char 'x'")

	pv, err = parseUInt([]byte("x12334"), 1, 100)
	assert.NoError(t, err)
	assert.Equal(t, uint64(12334), pv)
}

func Benchmark_FormatUInt64_fmt(b *testing.B) {
	buf := bytes.Buffer{}
	val := uint64(12345001)
	for i := 0; i < b.N; i++ {
		fmt.Fprintf(&buf, "%019d", val)
		buf.Reset()
	}
}

func Benchmark_FormatUInt64_ours(b *testing.B) {
	buf := bytes.Buffer{}
	val := uint64(12345001)
	for i := 0; i < b.N; i++ {
		appendUInt64(&buf, 19, val)
		buf.Reset()
	}
}

func Test_AppendUint64(t *testing.T) {
	check := func(v uint64, padTo int, expected string) {
		buf := bytes.Buffer{}
		appendUInt64(&buf, padTo, v)
		assert.Equal(t, expected, buf.String())
	}
	check(1234567890123456789, 19, "1234567890123456789")
	check(12345001, 19, "0000000000012345001")
	check(0, 5, "00000")
	check(1, 5, "00001")
	check(123456790, 5, "123456790")
	check(12345, 5, "12345")
}
