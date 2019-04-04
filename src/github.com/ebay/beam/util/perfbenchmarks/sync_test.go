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
	"sync"
	"sync/atomic"
	"testing"
)

func Benchmark_chan_create(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ch := make(chan struct{})
		_ = ch
	}
}

func Benchmark_chan_createAndClose(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ch := make(chan struct{})
		close(ch)
	}
}

func Benchmark_atomicInc(b *testing.B) {
	for i := 0; i < b.N; i++ {
		atomic.AddUint64(&Dummy, 1)
	}
}

func Benchmark_twoAtomicIncs(b *testing.B) {
	for i := 0; i < b.N; i++ {
		atomic.AddUint64(&Dummy, 1)
		atomic.AddUint64(&Dummy, 1)
	}
}

func Benchmark_threeAtomicIncs(b *testing.B) {
	for i := 0; i < b.N; i++ {
		atomic.AddUint64(&Dummy, 1)
		atomic.AddUint64(&Dummy, 1)
		atomic.AddUint64(&Dummy, 1)
	}
}

func Benchmark_Mutex_Lock_Unlock(b *testing.B) {
	var lock sync.Mutex
	for i := 0; i < b.N; i++ {
		lock.Lock()
		Dummy++
		lock.Unlock()
	}
}

func Benchmark_Mutex_Lock_defer_Unlock(b *testing.B) {
	var lock sync.Mutex
	for i := 0; i < b.N; i++ {
		func() {
			lock.Lock()
			defer lock.Unlock()
			Dummy++
		}()
	}
}

func Benchmark_RWMutex_Lock_Unlock(b *testing.B) {
	var lock sync.RWMutex
	for i := 0; i < b.N; i++ {
		lock.Lock()
		Dummy++
		lock.Unlock()
	}
}

func Benchmark_RWMutex_RLock_RUnlock(b *testing.B) {
	var lock sync.RWMutex
	for i := 0; i < b.N; i++ {
		lock.RLock()
		Dummy++
		lock.RUnlock()
	}
}
