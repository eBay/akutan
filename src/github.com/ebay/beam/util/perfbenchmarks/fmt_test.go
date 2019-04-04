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

package perfbenchmarks

import (
	"fmt"
	"math"
	"strconv"
	"testing"
)

func Benchmark_fmt_Sprintf_int(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = fmt.Sprintf("%d", i)
	}
}

func Benchmark_fmt_Sprint_int(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = fmt.Sprint(i)
	}
}

func Benchmark_strconv_FormatInt_incrementing(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = strconv.FormatInt(int64(i), 10)
	}
}

func Benchmark_strconv_FormatInt_large(b *testing.B) {
	x := math.MaxInt32
	for i := 0; i < b.N; i++ {
		_ = strconv.FormatInt(int64(x), 10)
	}
}
