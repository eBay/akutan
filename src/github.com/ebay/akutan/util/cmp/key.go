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
	"strings"
)

// The Key interface is satisfied by any object whose identity can be serialized
// into a string.
type Key interface {
	// Key writes a serialization of the object's identity to the given
	// strings.Builder. This serialization should be optimized for machine
	// consumption. However, it should also be human-readable to make debugging
	// and unit testing easier.
	Key(*strings.Builder)
}

// GetKey returns the identity/comparison key of the object.
func GetKey(object Key) string {
	var b strings.Builder
	object.Key(&b)
	return b.String()
}
