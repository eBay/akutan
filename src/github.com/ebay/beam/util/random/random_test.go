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

package random

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_SecureInt64(t *testing.T) {
	const samples = 30
	// I don't know if this formula is correct, but it seems to be pretty close
	// empirically.
	falseFailureInterval := 1.0 / (math.Pow(0.5, samples) * 2 * 64)
	assert.True(t, falseFailureInterval > 1e6,
		"Number of samples (%v) too low. "+
			"Expecting spurious test failures every ~%v iterations",
		samples, falseFailureInterval)
	info := fmt.Sprintf("This test is expected to fail about once every %v iterations.",
		falseFailureInterval)

	values := make(map[int64]struct{}, samples)
	for i := 0; i < samples; i++ {
		values[SecureInt64()] = struct{}{}
	}
	assert.Len(t, values, samples, "duplicate value found")
	for bit := uint(0); bit < 64; bit++ {
		var found [2]bool
		for value := range values {
			found[(value>>bit)&1] = true
		}
		assert.True(t, found[0],
			"bit %v always 1 over %v samples.\n%v", bit, samples, info)
		assert.True(t, found[1],
			"bit %v always 0 over %v samples.\n%v", bit, samples, info)
	}
}
