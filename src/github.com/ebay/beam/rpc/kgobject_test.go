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
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"testing"
	"time"

	"github.com/cespare/xxhash"
	"github.com/ebay/beam/api"
	"github.com/ebay/beam/msg/kgobject"
	"github.com/stretchr/testify/assert"
)

// if you add new cases here, put them in value order, as the test will verify
// that the generated key of each subsequent item of the same type is lexicographically
// after the previous one
var orderedVals = []api.KGObject{
	api.KGObject{},
	kgobject.AString("", 0),
	kgobject.AString("Bob", 0),
	kgobject.AString("Bob's House", 0),
	kgobject.AString("Bob's House", 1),
	kgobject.AString("Bob's Housf", 0),
	kgobject.AString("Hello World", 0),
	kgobject.AString("Hello World \u65e5\u672c\u8a9e", 0),
	kgobject.AString("a", 0),
	kgobject.AString("b", 0),
	kgobject.AString("cookoos like to fly around the fields looking for gold. here's a really long string to exercise the buffer length check in ToKGObject", 42),
	kgobject.AInt64(math.MinInt64, 0),
	kgobject.AInt64(math.MinInt32, 0),
	kgobject.AInt64(math.MinInt16, 0),
	kgobject.AInt64(-2, 0),
	kgobject.AInt64(-1, 0),
	kgobject.AInt64(0, 0),
	kgobject.AInt64(1, 0),
	kgobject.AInt64(2, 0),
	kgobject.AInt64(math.MaxInt16, 0),
	kgobject.AInt64(math.MaxInt32, 0),
	kgobject.AInt64(math.MaxInt64, 0),
	kgobject.ATimestamp(time.Date(0, time.January, 31, 23, 59, 59, int(time.Second-time.Millisecond), time.UTC), api.Nanosecond, 0),
	kgobject.ATimestampY(1990, 0),
	kgobject.ATimestampYM(1990, 3, 0),
	kgobject.ATimestampYMD(1990, 3, 24, 0),
	kgobject.ATimestampYMDH(1990, 3, 24, 14, 0),
	kgobject.ATimestampYMDHM(1990, 3, 24, 14, 24, 0),
	kgobject.ATimestampYMDHMS(1990, 3, 24, 14, 24, 59, 0),
	kgobject.ATimestampYMDHMSN(1990, 3, 24, 14, 24, 59, 951, 0),
	kgobject.ATimestamp(time.Now().Add(-time.Second*10).UTC(), api.Nanosecond, 0),
	kgobject.ATimestamp(time.Now().UTC(), api.Nanosecond, 0),
	kgobject.ABool(false, 0),
	kgobject.ABool(true, 0),
	kgobject.AFloat64(-math.MaxFloat64, 0),
	kgobject.AFloat64(-math.MaxFloat32, 0),
	kgobject.AFloat64(-42420000, 0),
	kgobject.AFloat64(-100, 0),
	kgobject.AFloat64(-0.00000000001, 0),
	kgobject.AFloat64(-math.SmallestNonzeroFloat64, 0),
	kgobject.AFloat64(0, 0),
	kgobject.AFloat64(math.SmallestNonzeroFloat64, 0),
	kgobject.AFloat64(math.SmallestNonzeroFloat32, 0),
	kgobject.AFloat64(0.0000001, 0),
	kgobject.AFloat64(0.000001, 0),
	kgobject.AFloat64(1.0, 0),
	kgobject.AFloat64(42.42, 0),
	kgobject.AFloat64(42.43, 0),
	kgobject.AFloat64(42420000, 0),
	kgobject.AFloat64(math.MaxUint64, 0),
	kgobject.AFloat64(math.MaxFloat32, 0),
	kgobject.AFloat64(math.MaxFloat64, 0),
	kgobject.AKID(0),
	kgobject.AKID(1),
	kgobject.AKID(12344),
	kgobject.AKID(math.MaxUint32),
	kgobject.AKID(math.MaxUint64),
}

func Test_KGObjectRoundTrip(t *testing.T) {
	var prevKey []byte
	var prevObj KGObject
	for idx, apiObj := range orderedVals {
		rpcObj := KGObjectFromAPI(apiObj)
		key := rpcObj.AsBytes()
		decRPCObj, err := KGObjectFromBytes(key)
		assert.NoError(t, err)
		decAPIObj := decRPCObj.ToAPIObject()
		assert.Equal(t, apiObj, decAPIObj, "failed to roundtrip kgobject %+v", apiObj)
		if idx > 0 && (prevKey[0] == key[0]) {
			cmp := bytes.Compare(prevKey, key)
			assert.Equal(t, -1, cmp, "previous key expected to be smaller:\n  %d:%16x\n  %d:%16x\n  %d:%v\n  %d:%v\n", idx-1, prevKey, idx, key, idx-1, orderedVals[idx-1], idx, apiObj)
			assert.True(t, prevObj.Less(rpcObj))
			assert.False(t, rpcObj.Less(prevObj))
		}
		prevKey = key
		prevObj = rpcObj
	}
}

func Test_KGObject_AsString(t *testing.T) {
	for i := range orderedVals {
		obj := KGObjectFromAPI(orderedVals[i])
		assert.Equal(t, string(obj.AsBytes()), obj.AsString(),
			"obj %v (index %v) bytes and string differ",
			obj, i)
		assert.Equal(t, obj.Size(), len(obj.AsString()),
			"obj %v (index %v) AsString() returned wrong size",
			obj, i)
	}
}

func Test_TypePrefix(t *testing.T) {
	type tc struct {
		obj            KGObject
		expectedPrefix []byte
	}
	// Add the rest of the types after we fix the encoding
	tests := []tc{
		{KGObject{}, []byte{byte(KtNil)}},
		{AString("bob", 42), []byte{byte(KtString)}},
		{AInt64(128, 42), append([]byte{byte(KtInt64)}, "0000000000000000042"...)},
	}
	for _, c := range tests {
		actualPrefix := c.obj.TypePrefix()
		assert.Equal(t, c.expectedPrefix, actualPrefix, "For KGObject: %v", c.obj)
	}
}

func Test_ValueType(t *testing.T) {
	type tc struct {
		obj          KGObject
		expectedType KGObjectType
	}
	tests := []tc{
		{KGObject{}, KtNil},
		{AString("bob", 42), KtString},
		{AInt64(42, 42), KtInt64},
		{AFloat64(42, 42), KtFloat64},
		{ATimestampY(2042, 42), KtTimestamp},
		{ABool(true, 42), KtBool},
	}
	notType := func(vt KGObjectType) KGObjectType {
		if vt > 1 {
			return vt - 1
		}
		return KtKID
	}
	for _, c := range tests {
		assert.Equal(t, c.expectedType, c.obj.ValueType(), "Unexpected ValueType for obj %v", c.obj)
		assert.True(t, c.obj.IsType(c.expectedType), "Unexpected result for IsType() for obj %v", c.obj)
		assert.False(t, c.obj.IsType(notType(c.expectedType)), "Unexpected result for IsType() for obj %v", c.obj)
	}
}

func Test_Marshal(t *testing.T) {
	// round trips all the test KGObjects through the pb marshal funcs
	for _, apiKGObject := range orderedVals {
		rpcKGObject := KGObjectFromAPI(apiKGObject)
		assert.True(t, rpcKGObject.Size() >= 1)
		m, err := rpcKGObject.Marshal()
		assert.NoError(t, err)
		assert.Equal(t, rpcKGObject.Size(), len(m))
		m2 := make([]byte, len(m)+10)
		n, err := rpcKGObject.MarshalTo(m2)
		assert.NoError(t, err)
		assert.Equal(t, m, m2[:n])

		obj2 := KGObject{}
		assert.NoError(t, obj2.Unmarshal(m))
		assert.Equal(t, rpcKGObject.value, obj2.value)
		assert.True(t, rpcKGObject.Equal(obj2))
		assert.True(t, obj2.Equal(rpcKGObject))
	}
	empty := []byte{}
	rpcObj := AString("Bob", 42)
	n, err := rpcObj.MarshalTo(empty)
	assert.NoError(t, err)
	assert.Zero(t, n)
	x := KGObject{}
	assert.Equal(t, io.ErrUnexpectedEOF, x.Unmarshal(empty))
	n, err = x.MarshalTo(empty)
	assert.NoError(t, err)
	assert.Zero(t, n)
}

func Test_WriteTo(t *testing.T) {
	for _, o := range orderedVals {
		b := bytes.Buffer{}
		kgo := KGObjectFromAPI(o)
		kgo.WriteTo(&b, WriteOpts{})
		assert.Equal(t, kgo.Size(), b.Len())
		assert.Equal(t, b.Bytes(), kgo.AsBytes())
		r, err := KGObjectFromBytes(b.Bytes())
		assert.NoError(t, err)
		assert.Equal(t, kgo, r)
	}
	// Currently the LangID is encoded into a trailing 19 byte ASCII string
	// [utf8 bytes][0x00][19 char langID]
	x := AString("Bob", 42)
	b := bytes.Buffer{}
	x.WriteTo(&b, WriteOpts{NoLangID: true})
	assert.True(t, bytes.HasSuffix(b.Bytes(), []byte("Bob")))
	assert.False(t, bytes.Contains(b.Bytes(), []byte("42")))
	b2 := bytes.Buffer{}
	x.WriteTo(&b2, WriteOpts{})
	assert.Equal(t, b2.Len(), b.Len()+20)
}

// This is an old test from when KGObject had a Hash method. The test has been
// ported over to use AsString(). It's probably not providing much value
// anymore, but it's easy enough to keep for now.
func Test_Hash(t *testing.T) {
	assert := assert.New(t)

	objHash := func(o KGObject) uint64 {
		digest := xxhash.New()
		digest.WriteString(o.AsString())
		return digest.Sum64()
	}
	assert.Equal(objHash(AString("Bob", 0)), objHash(AString("Bob", 0)))
	assert.NotEqual(objHash(AString("Bob", 0)), objHash(AString("Bob", 1)))
	assert.NotEqual(objHash(KGObject{}), objHash(AString("Bob", 0)))

	for _, o := range orderedVals {
		obj := KGObjectFromAPI(o)
		var b bytes.Buffer
		obj.WriteTo(&b, WriteOpts{})
		assert.Equal(xxhash.Sum64(b.Bytes()), objHash(obj), "o=%#v", o)
	}
	// Two objects are not required to be equal when it's hash value are equal,
	// since xxhash isn't a cryptographic hash function. However, below test
	// scenario passes as we happen to not get any hash collisions with the
	// current hash function.
	for _, x := range orderedVals {
		for _, y := range orderedVals {
			xObj := KGObjectFromAPI(x)
			yObj := KGObjectFromAPI(y)
			assert.Equal(xObj.Equal(yObj), objHash(xObj) == objHash(yObj),
				"x=%#v, y=%#v", x, y)
		}
	}
}

func Test_ZeroValue(t *testing.T) {
	a := KGObject{}
	b := KGObject{}
	assert.True(t, a.IsType(KtNil))
	assert.Equal(t, KtNil, a.ValueType())
	assert.True(t, a.Equal(b))
	assert.False(t, a.Less(b))
	assert.False(t, b.Less(a))
	assert.Equal(t, []byte{byte(KtNil)}, a.AsBytes())
	c, err := KGObjectFromBytes(a.AsBytes())
	assert.NoError(t, err)
	assert.Equal(t, a, c)
	assert.Equal(t, 1, a.Size())
	m, err := a.Marshal()
	assert.NoError(t, err)
	assert.Equal(t, []byte{byte(KtNil)}, m)
	r := KGObject{}
	r.Unmarshal(m)
	assert.Equal(t, a, r)
	assert.Equal(t, "", r.value)
}

func Test_FactoryWriteHelpers(t *testing.T) {
	// verify the embedded write helper in kgObjectFactory
	f := new(kgObjectBuilder)
	assert.Panics(t, func() { f.writeUInt8(-3) })
	assert.Panics(t, func() { f.writeUInt8(300) })
	assert.Panics(t, func() { f.writeUInt16(-1) })
	assert.Panics(t, func() { f.writeUInt16(65536 + 2) })
	assert.Panics(t, func() { f.writeUInt32(-1) })
	assert.Panics(t, func() { f.writeUInt32(math.MaxUint32 + 1) })
	assert.Equal(t, 0, f.buff.Len())

	f.writeUInt8('a')
	assert.Equal(t, "a", f.buff.String())

	f.buff.Reset()
	f.writeUInt16(300)
	assert.Equal(t, "\x01\x2C", f.buff.String())

	f.buff.Reset()
	f.writeUInt32(math.MaxUint16 + 1)
	assert.Equal(t, "\x00\x01\x00\x00", f.buff.String())
}
func Test_IsKGObject(t *testing.T) {
	m := func(t KGObjectType, r string) []byte {
		return append([]byte{byte(t)}, r...)
	}
	tests := [][]byte{
		[]byte{},
		m(100, ""),
		m(101, "bob"),
		m(KtNil, "x"),
		m(KtInt64, ""),
		m(KtInt64, "1234567890123456789AABBCCDDe"),
		m(KtInt64, "1234567890123456789AABBCCD"),
		m(KtFloat64, ""),
		m(KtFloat64, "1234567890123456789AABBCCDDe"),
		m(KtFloat64, "1234567890123456789AABBCCD"),
		m(KtBool, ""),
		m(KtBool, "xx"),
		m(KtString, ""),
		m(KtString, "123456789012345678"),
		m(KtTimestamp, ""),
		m(KtTimestamp, "123456789012345678qqqqqqqq"),
		m(KtKID, ""),
		m(KtKID, "1234567"),
		m(KtKID, "123456789"),
	}
	for _, tc := range tests {
		e := isKGObject(tc)
		assert.NotNil(t, e)
		_, err := KGObjectFromBytes(tc)
		assert.NotNil(t, err)
		assert.Equal(t, e, err)
	}
}

func Benchmark_BinaryWrite(b *testing.B) {
	bf := bytes.Buffer{}
	for i := 0; i < b.N; i++ {
		bf.Reset()
		binary.Write(&bf, binary.BigEndian, uint64(i))
	}
}

func Benchmark_PutUint64ThenWrite(b *testing.B) {
	bf := bytes.Buffer{}
	for i := 0; i < b.N; i++ {
		bf.Reset()
		t := [8]byte{}
		s := t[:]
		binary.BigEndian.PutUint64(s, uint64(i))
		bf.Write(s)
	}
}

func Benchmark_PutUint64WithMakeThenWrite(b *testing.B) {
	bf := bytes.Buffer{}
	for i := 0; i < b.N; i++ {
		bf.Reset()
		s := make([]byte, 8)
		binary.BigEndian.PutUint64(s, uint64(i))
		bf.Write(s)
	}
}

func Benchmark_kgObjectFactory_AString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		AString("Bob", 0)
	}
}

func Benchmark_kgObjectFactory_AInt64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		AInt64(int64(i), 0)
	}
}
func Benchmark_kgObjectFactory_AFloat64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		AFloat64(42.42, 0)
	}
}
func Benchmark_kgObjectFactory_ABool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ABool(true, 0)
	}
}
func Benchmark_kgObjectFactory_AKID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		AKID(1234)
	}
}

// For reference, this is the benchmark output from a 15" Macbook Pro Laptop
// as of March 2019
// run with go test -bench=. -benchmem github.com/ebay/beam/rpc/...

// Benchmark_BinaryWrite-8                  	50000000	        35.2 ns/op	      16 B/op	       2 allocs/op
// Benchmark_PutUint64ThenWrite-8           	300000000	         5.84 ns/op	       0 B/op	       0 allocs/op
// Benchmark_PutUint64WithMakeThenWrite-8   	300000000	         5.84 ns/op	       0 B/op	       0 allocs/op
// Benchmark_kgObjectFactory_AString-8      	50000000	        36.6 ns/op	      32 B/op	       1 allocs/op
// Benchmark_kgObjectFactory_AInt64-8       	50000000	        35.3 ns/op	      32 B/op	       1 allocs/op
// Benchmark_kgObjectFactory_AFloat64-8     	50000000	        35.1 ns/op	      32 B/op	       1 allocs/op
// Benchmark_kgObjectFactory_ABool-8        	50000000	        33.4 ns/op	      32 B/op	       1 allocs/op
// Benchmark_kgObjectFactory_AKID-8         	50000000	        25.4 ns/op	      16 B/op	       1 allocs/op
