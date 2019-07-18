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

package logread

import (
	"testing"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/logentry"
	"github.com/stretchr/testify/assert"
)

func Test_KID(t *testing.T) {
	assert.Equal(t, uint64(1001), KID(1, 1))
	assert.Equal(t, uint64(1002), KID(1, 2))
	assert.Equal(t, uint64(2001), KID(2, 1))
	assert.Equal(t, uint64(2002), KID(2, 2))
	assert.Equal(t, uint64(2999), KID(2, 999))
	assert.Equal(t, uint64(3001), KID(3, 1))
	assert.Equal(t, uint64(3002), KID(3, 2))
	assert.Panics(t, func() { KID(0, 1) })
	assert.Panics(t, func() { KID(0, 0) })
	assert.Panics(t, func() { KID(1, -1) })
	assert.Panics(t, func() { KID(1, MaxOffset) })
}

func Test_KIDOfKIDOrOffset(t *testing.T) {
	kid := logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 4242}}
	off := logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Offset{Offset: 3}}
	idx := blog.Index(1)
	assert.Equal(t, uint64(4242), KIDof(idx, &kid))
	assert.Equal(t, uint64(1003), KIDof(idx, &off))
}
