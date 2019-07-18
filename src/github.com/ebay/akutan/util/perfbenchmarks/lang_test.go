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
	"sync/atomic"
	"testing"
)

// See also "runtime: defer is slow" at
// https://github.com/golang/go/issues/14939.
func Benchmark_defer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		func() {
			defer inc()
		}()
	}
}

func inc() {
	Dummy++
}

func Benchmark_closure(b *testing.B) {
	for i := 0; i < b.N; i++ {
		inc := func() { Dummy++ }
		inc()
	}
}

func Benchmark_go(b *testing.B) {
	running := uint64(1)
	for i := 0; i < b.N; i++ {
		go func() {
			Dummy++
			atomic.StoreUint64(&running, 0)
		}()
		for atomic.CompareAndSwapUint64(&running, 0, 1) {
			// busy loop
		}
	}
}

func Benchmark_go_baseline(b *testing.B) {
	running := uint64(1)
	for i := 0; i < b.N; i++ {
		func() {
			Dummy++
			atomic.StoreUint64(&running, 0)
		}()
		for atomic.CompareAndSwapUint64(&running, 0, 1) {
			// busy loop
		}
	}
}
