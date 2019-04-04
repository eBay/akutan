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

	"github.com/ebay/beam/util/cmp"
)

// parseInt will parse a base 10 from val starting at index 'startPos'
// strconv.ParseInt requires a string, which is annoying
// especially as ParseInt converts it back to a []byte anyway
// also see this stupid stance on not adding a version that takes []byte
//  https://github.com/golang/go/issues/2632
// Note that this will not detect tht your string overflows a int64
func parseUInt(val []byte, startPos, endPos int) (uint64, error) {
	res := uint64(0)
	vl := len(val)
	endPos = cmp.MinInt(vl, endPos)
	for p := startPos; p < endPos; p++ {
		if val[p] < '0' || val[p] > '9' {
			return res, fmt.Errorf("unable to parse '%s' into an int, unexpected char '%c'", val[startPos:endPos], val[p])
		}
		n := uint64(val[p] - '0')
		res = res*10 + n
	}
	return res, nil
}

// appendUInt64 will append to the buffer the val formatted to base 10
// padded with leading 0's to the indicated padTo size.
// this is equivilent to fmt.Fprintf(&buf, "%0{padSize}d", val)
// but is much faster and does zero heap allocations
func appendUInt64(b *bytes.Buffer, padTo int, val uint64) {
	s := [20]uint8{
		'0', '0', '0', '0',
		'0', '0', '0', '0',
		'0', '0', '0', '0',
		'0', '0', '0', '0',
		'0', '0', '0', '0',
	}
	pos := len(s) - 1
	for val != 0 {
		s[pos] = uint8(val%10) + '0'
		val = val / 10
		pos--
	}
	pos = cmp.MinInt(pos+1, len(s)-padTo)
	b.Write(s[pos:])
}
