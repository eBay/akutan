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

package rpc

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/ebay/beam/util/cmp"
	"github.com/sirupsen/logrus"
)

// kgObjectBuilder is used to help create new instances of KGObject. It is not concurrent
// safe, don't share kgObjectBuilder across goroutines. The zero value for a kgObjectBuilder
// is a valid builder ready for use.
type kgObjectBuilder struct {
	buff strings.Builder
}

func (f *kgObjectBuilder) resetAndWriteType(t KGObjectType, growTo int) {
	f.buff.Reset()
	f.buff.Grow(growTo)
	f.buff.WriteByte(uint8(t))
}

func (f *kgObjectBuilder) writeUInt8(v int) {
	if v > math.MaxInt8 || v < 0 {
		logrus.Panicf("kgObjectFactory.writeUInt8 given a value out of range: %d", v)
	}
	f.buff.WriteByte(uint8(v))
}

func (f *kgObjectBuilder) writeUInt16(v int) {
	if v > math.MaxInt16 || v < 0 {
		logrus.Panicf("kgObjectFactory.writeUInt16 given a value out of range: %d", v)
	}
	t := make([]byte, 2)
	binary.BigEndian.PutUint16(t, uint16(v))
	f.buff.Write(t)
}

func (f *kgObjectBuilder) writeUInt32(v int) {
	if v > math.MaxInt32 || v < 0 {
		logrus.Panicf("kgObjectFactory.writeInt32 given a value out of range: %d", v)
	}
	t := make([]byte, 4)
	binary.BigEndian.PutUint32(t, uint32(v))
	f.buff.Write(t)
}

func (f *kgObjectBuilder) writeUInt64(v uint64) {
	dest := make([]byte, 8)
	binary.BigEndian.PutUint64(dest, v)
	f.buff.Write(dest)
}

// isKGObject will look at the provided byte slice and do some sanity checks to see
// if it is a encoded KGObject, it returns nil if it passes or the checks, or an error
// describing the problem
func isKGObject(d []byte) error {
	if len(d) == 0 {
		return io.ErrUnexpectedEOF
	}
	t := KGObjectType(d[0])
	switch t {
	case KtNil:
		if len(d) > 1 {
			return fmt.Errorf("KGObject of type KtNil expected to only have 1 byte, but has %d", len(d))
		}
		return nil
	case KtString:
		// is expected to contain a string followed by a 19 char langID
		if len(d) < 20 {
			return fmt.Errorf("data is not long enough for a KtString type KGObject")
		}
		return nil
	case KtInt64:
		fallthrough
	case KtFloat64:
		// is expected to contain a typeID, a 19 char unitID followed by an 8 byte value
		if len(d) != 28 {
			return fmt.Errorf("data of incorrect length for a KGObject of type %d", t)
		}
		return nil
	case KtTimestamp:
		if len(d) != 1+19+2+1+1+1+1+1+4+1 {
			return fmt.Errorf("data of incorrect length for a KGObject of type KtTimestamp")
		}
		return nil
	case KtBool:
		if len(d) != 21 {
			return fmt.Errorf("data of incorrect length for a KGObject of type KtBool")
		}
		return nil
	case KtKID:
		// type + 8 byte KID
		if len(d) != 9 {
			return fmt.Errorf("data of incorrect length for a KGObject of type KtKID")
		}
		return nil
	}
	return fmt.Errorf("data contains an invalid KGObjectType value %d", d[0])
}

// maskMsbOnly has the msb aka sign bit set, you can xor (^) this to flip the sign bit
const maskMsbOnly = uint64(1 << 63)

// maskAllBits has all 64 bits set
const maskAllBits = uint64(0xFFFFFFFFFFFFFFFF)

// The 2 functions below are copied from diskview/keys/unit64.go
// We can delete them when we switch to a binary encoding for KIDs in
// KGObjects & Facts on disk

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
func appendUInt64(b *strings.Builder, padTo int, val uint64) {
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
