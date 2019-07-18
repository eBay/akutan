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

// Package random helps seed the math/rand pseudo-random number generator.
package random

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	mathrand "math/rand"
)

// SeedMath seeds the global math/rand pseudo-random number generator with a
// crypto/rand input. It panics if crypto/rand returns an error.
//
// This should be called before using math/rand, which is otherwise seeded with
// the value 1. It's safe to call this multiple times, and doing so should
// produce no observable effects. However, it should not be called often for
// performance reasons.
//
// Note: this does not make the math/rand output secure for cryptographic
// purposes, but it does make the math/rand output more useful for things like
// load balancing and randomized algorithms.
func SeedMath() {
	mathrand.Seed(SecureInt64())
}

// SecureInt64 returns 64 bits of randomness from crypto/rand. It panics if
// crypto/rand returns an error. This is useful for passing into math/rand.
func SecureInt64() int64 {
	var value int64
	err := binary.Read(cryptorand.Reader, binary.BigEndian, &value)
	if err != nil {
		panic(fmt.Sprintf("Failed to read 8 bytes from crypto/rand: %v", err))
	}
	return value
}
