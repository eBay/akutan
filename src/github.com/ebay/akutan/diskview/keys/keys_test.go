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
	"testing"
	"time"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/logentry"
	"github.com/ebay/akutan/rpc"
	"github.com/stretchr/testify/assert"
)

func Benchmark_MakeSPO(t *testing.B) {
	fact := makeFactString(12345)
	spec := FactKey{Fact: &fact, Encoding: rpc.KeyEncodingSPO}
	for i := 0; i < t.N; i++ {
		_ = spec.Bytes()
	}
}

func Benchmark_MakePOS(t *testing.B) {
	fact := makeFactString(12345)
	spec := FactKey{Fact: &fact, Encoding: rpc.KeyEncodingPOS}
	for i := 0; i < t.N; i++ {
		_ = spec.Bytes()
	}
}

func Benchmark_ParseSPO(t *testing.B) {
	fact := makeFactString(12345)
	spec := FactKey{Fact: &fact, Encoding: rpc.KeyEncodingSPO}
	key := spec.Bytes()
	var err error
	for i := 0; i < t.N; i++ {
		_, err = ParseKey(key)
		assert.NoError(t, err)
	}
}

func Benchmark_ParsePOS(t *testing.B) {
	fact := makeFactString(12345)
	spec := FactKey{Fact: &fact, Encoding: rpc.KeyEncodingPOS}
	key := spec.Bytes()
	var err error
	for i := 0; i < t.N; i++ {
		_, err = ParseKey(key)
		assert.NoError(t, err)
	}
}

func Test_encodePOS(t *testing.T) {
	idx := blog.Index(12345)
	for _, tc := range makeKeySpecs(idx) {
		if !tc.index {
			continue
		}
		pos := []posKeyPrefix{
			posPredicate,
			posPredicateObjectType,
			posPredicateObjectNoLang,
			posPredicateObjectSubject,
			posFull,
		}
		var prevPrefix []byte
		for _, p := range pos {
			prefix := encodePOS(tc.spec.(FactKey).Fact, p)
			assert.True(t, len(prefix) > 0)
			assert.True(t, len(prefix) > len(prevPrefix))
			assert.True(t, bytes.HasPrefix(prefix, prevPrefix))
		}
	}
}

func Test_encodePOS_ChecksPosn(t *testing.T) {
	f := rpc.Fact{}
	assert.PanicsWithValue(t, "Invalid posKeyPrefix of -1 passed to encodePOS", func() {
		encodePOS(&f, posKeyPrefix(-1))
	})
	assert.PanicsWithValue(t, "Invalid posKeyPrefix of 42 passed to encodePOS", func() {
		encodePOS(&f, posKeyPrefix(42))
	})
}

func Test_encodeSPO(t *testing.T) {
	idx := blog.Index(12345)
	for _, tc := range makeKeySpecs(idx) {
		if !tc.index {
			continue
		}
		pos := []spoKeyPrefix{
			spoSubject,
			spoSubjectPredicate,
			spoSubjectPredicateObjectNoLang,
			spoSubjectPredicateObject,
			spoFull,
		}
		var prevPrefix []byte
		for _, p := range pos {
			prefix := encodeSPO(tc.spec.(FactKey).Fact, p)
			assert.True(t, len(prefix) > 0)
			assert.True(t, len(prefix) > len(prevPrefix))
			assert.True(t, bytes.HasPrefix(prefix, prevPrefix))
		}
	}
}

func Test_encodeSPO_ChecksPosn(t *testing.T) {
	f := rpc.Fact{}
	assert.PanicsWithValue(t, "Invalid spoKeyPrefix of -1 passed to encodeSPO", func() {
		encodeSPO(&f, spoKeyPrefix(-1))
	})
	assert.PanicsWithValue(t, "Invalid spoKeyPrefix of 42 passed to encodeSPO", func() {
		encodeSPO(&f, spoKeyPrefix(42))
	})
}

func Test_SPOKeyOrder(t *testing.T) {
	// This doesn't extensive test KGObject ordering, as thats already covered in KGObject.
	facts := []rpc.Fact{
		// Facts should be increasing encoded key value
		{Subject: 1, Predicate: 1, Object: rpc.AString("Bob", 11), Id: 1, Index: 1},
		{Subject: 12345, Predicate: 56789, Object: rpc.AString("Bob", 11), Id: 6666, Index: 7777},
		{Subject: 12345, Predicate: 56789, Object: rpc.AString("Bob", 11), Id: 6666, Index: 7778},
		{Subject: 12345, Predicate: 56789, Object: rpc.AString("Bob", 12), Id: 6666, Index: 7778},
		{Subject: 12345, Predicate: 56789, Object: rpc.AString("Eve", 1), Id: 6666, Index: 7778},
		{Subject: 12345, Predicate: 56790, Object: rpc.AString("Eve", 1), Id: 6666, Index: 7778},
		{Subject: 12346, Predicate: 1, Object: rpc.AString("Eve", 1), Id: 6666, Index: 7778},
	}
	var prev []byte
	for _, f := range facts {
		k := FactKey{Fact: &f, Encoding: rpc.KeyEncodingSPO}.Bytes()
		assert.NotNil(t, k)
		assert.Equal(t, 1, bytes.Compare(k, prev),
			"Fact %+v with Key %v should be larger than %v", f, k, prev)
		prev = k
	}
}

func Test_POSKeyOrder(t *testing.T) {
	// This doesn't extensive test KGObject ordering, as thats already covered in KGObject.
	facts := []rpc.Fact{
		// Facts should be increasing encoded key value
		{Subject: 1, Predicate: 1, Object: rpc.AString("Bob", 11), Id: 1, Index: 1},
		{Subject: 12345, Predicate: 56789, Object: rpc.AString("Bob", 11), Id: 6666, Index: 7777},
		{Subject: 12345, Predicate: 56789, Object: rpc.AString("Bob", 11), Id: 6666, Index: 7778},
		{Subject: 12345, Predicate: 56789, Object: rpc.AString("Bob", 12), Id: 6666, Index: 7778},
		{Subject: 10000, Predicate: 56789, Object: rpc.AString("Eve", 1), Id: 6666, Index: 7778},
		{Subject: 12345, Predicate: 56790, Object: rpc.AString("Eve", 1), Id: 6666, Index: 7778},
		{Subject: 12346, Predicate: 56790, Object: rpc.AString("Eve", 1), Id: 6666, Index: 7778},
	}
	var prev []byte
	for _, f := range facts {
		k := FactKey{Fact: &f, Encoding: rpc.KeyEncodingPOS}.Bytes()
		assert.NotNil(t, k)
		assert.Equal(t, 1, bytes.Compare(k, prev),
			"Fact %+v with Key %v should be larger than %v", f, k, prev)
		prev = k
	}
}

var (
	kid54321 = []byte("\x00\x00\x00\x00\x00\x00\xD4\x31")
	kid12345 = []byte("\x00\x00\x00\x00\x00\x00\x30\x39")
	kid66666 = []byte("\x00\x00\x00\x00\x00\x01\x04\x6A")
	kid77777 = []byte("\x00\x00\x00\x00\x00\x01\x2F\xD1")
)

func Test_POSPrefixes(t *testing.T) {
	pre := []byte("p")
	assert.Equal(t, concat(pre, kid54321), KeyPrefixPredicate(54321))

	assert.Equal(t, concat(pre, kid54321, []byte{0x01}),
		KeyPrefixPredicateObjectType(54321, rpc.AString("Bob", 11)))
	assert.Equal(t, concat(pre, kid54321, []byte("\x03\x00\x00\x00\x00\x00\x00\x00\x0B")),
		KeyPrefixPredicateObjectType(54321, rpc.AInt64(5, 11)))

	assert.Equal(t, concat(pre, kid54321, []byte("\x01Bob")),
		KeyPrefixPredicateObjectNoLang(54321, rpc.AString("Bob", 11)))
	assert.Equal(t, concat(pre, kid54321, []byte("\x03\x00\x00\x00\x00\x00\x00\x00\x0B\x80\x00\x00\x00\x00\x00\x00\x05")),
		KeyPrefixPredicateObjectNoLang(54321, rpc.AInt64(5, 11)))
}

func concat(parts ...[]byte) []byte {
	return bytes.Join(parts, nil)
}

func Test_SPOPrefixes(t *testing.T) {
	pre := []byte("s")
	assert.Equal(t, concat(pre, kid12345), KeyPrefixSubject(12345))
	assert.Equal(t, concat(pre, kid12345, kid54321), KeyPrefixSubjectPredicate(12345, 54321))

	assert.Equal(t, concat(pre, kid12345, kid54321, []byte("\x01Bob")),
		KeyPrefixSubjectPredicateObjectNoLang(12345, 54321, rpc.AString("Bob", 1)))
	assert.Equal(t, concat(pre, kid12345, kid54321, []byte("\x05\x00\x00\x00\x00\x00\x00\x00\x02\x01")),
		KeyPrefixSubjectPredicateObjectNoLang(12345, 54321, rpc.ABool(true, 2)))
}

func Test_FactKeyBytes(t *testing.T) {
	f := rpc.Fact{
		Subject:   12345,
		Predicate: 54321,
		Object:    rpc.AString("Bob", 1),
		Index:     66666,
		Id:        77777,
	}
	pos := FactKey{Fact: &f, Encoding: rpc.KeyEncodingPOS}
	assert.Equal(t, concat([]byte("p"), kid54321,
		[]byte("\x01Bob\x00\x00\x00\x00\x00\x00\x00\x00\x01"),
		kid12345,
		kid77777,
		kid66666), pos.Bytes())

	spo := FactKey{Fact: &f, Encoding: rpc.KeyEncodingSPO}
	assert.Equal(t, concat([]byte("s"), kid12345,
		kid54321,
		[]byte("\x01Bob\x00\x00\x00\x00\x00\x00\x00\x00\x01"),
		kid77777,
		kid66666), spo.Bytes())
}

func Test_FactKeyBytes_ChecksEncoding(t *testing.T) {
	f := FactKey{Fact: &rpc.Fact{}, Encoding: rpc.FactKeyEncoding(42)}
	assert.PanicsWithValue(t, "Unexpected FactKeyEncoding in FactKey: 42", func() {
		f.Bytes()
	})
}

func Test_ParseIndex(t *testing.T) {
	idx := blog.Index(0x8182838485868788)
	for _, tc := range makeKeySpecs(idx) {
		if !tc.index {
			continue
		}
		k := tc.spec.Bytes()
		pIdx := ParseIndex(k)
		assert.Equal(t, idx, pIdx)
	}

	assert.EqualValues(t, 0, ParseIndex([]byte("1234567")), "Should get 0 as there aren't 8 bytes")
	assert.EqualValues(t, 0, ParseIndex(nil))
}

func Test_EqualIgnoreIndex(t *testing.T) {
	fk1 := FactKey{
		Fact: &rpc.Fact{
			Subject:   1,
			Predicate: 1,
			Object:    rpc.AString("bob", 0),
			Index:     12,
		},
		Encoding: rpc.KeyEncodingSPO,
	}
	fk2 := fk1
	fk2.Fact.Index = 13
	k1 := fk1.Bytes()
	k2 := fk2.Bytes()
	assert.True(t, FactKeysEqualIgnoreIndex(k1, k2))
	assert.False(t, FactKeysEqualIgnoreIndex(k1, k2[1:]))
	fk3 := fk1
	fk3.Fact.Object = rpc.AString("bobb", 0)
	k3 := fk3.Bytes()
	assert.False(t, FactKeysEqualIgnoreIndex(k1, k3))
	k4 := []byte{1, 2, 3}
	assert.False(t, FactKeysEqualIgnoreIndex(k4, k4))
}

func Test_ByteParseRoundTrip(t *testing.T) {
	for _, tc := range makeKeySpecs(12345) {
		k := tc.spec.Bytes()
		assert.NotNil(t, k)
		pk, err := ParseKey(k)
		assert.NoError(t, err, "ParseKey error for keyType %T: %s: %v", tc.spec, k, err)
		assert.Equal(t, tc.spec, pk, "ParseKey result not equal for keyType %T: %s", tc.spec, k)
	}
}

func Test_ParseKey(t *testing.T) {
	k, err := ParseKey(nil)
	assert.Nil(t, k)
	assert.EqualError(t, err, "keys.ParseKey unable to determine key type: ")

	k, err = ParseKey([]byte("bob"))
	assert.Nil(t, k)
	assert.EqualError(t, err, "keys.ParseKey unable to determine key type: bob")
}

func Test_staticKeys(t *testing.T) {
	m := MetaKey{}.Bytes()
	assert.Equal(t, []byte("viewmeta"), m)
	m[0] = 'x'
	assert.Equal(t, []byte("viewmeta"), viewMetaKeyBytes, "Caller to MakeKey should not be able to mutate Keys internal viewmeta key")

	m = StatsKey{}.Bytes()
	assert.Equal(t, []byte("viewstats"), m)
	m[0] = 'x'
	assert.Equal(t, []byte("viewstats"), viewStatsKeyBytes, "Caller to MakeKey should not be able to mutate Keys internal viewstats key")
}

type keySpec struct {
	spec   Spec
	object bool
	index  bool
}

func makeKeySpecs(idx blog.Index) []keySpec {
	facts := makeFacts(idx)
	specs := []keySpec{
		{MetaKey{}, false, false},
		{StatsKey{}, false, false},
	}
	for i := range facts {
		specs = append(specs, keySpec{FactKey{Fact: &facts[i], Encoding: rpc.KeyEncodingSPO}, true, true})
		specs = append(specs, keySpec{FactKey{Fact: &facts[i], Encoding: rpc.KeyEncodingPOS}, true, true})
	}
	return specs
}

func makeFacts(idx blog.Index) []rpc.Fact {
	return []rpc.Fact{
		rpc.Fact{Index: idx, Id: 111111111111111110, Subject: 22222222222222222, Predicate: 33333333333333, Object: rpc.KGObject{}},
		makeFactString(idx),
		rpc.Fact{Index: idx, Id: 111111111111111112, Subject: 22222222222222222, Predicate: 33333333333333, Object: rpc.AFloat64(3.1415926535, 0)},
		rpc.Fact{Index: idx, Id: 111111111111111113, Subject: 22222222222222222, Predicate: 33333333333333, Object: rpc.AInt64(33, 0)},
		rpc.Fact{Index: idx, Id: 111111111111111114, Subject: 22222222222222222, Predicate: 33333333333333, Object: rpc.ATimestamp(time.Now().In(time.UTC), logentry.Nanosecond, 0)},
		rpc.Fact{Index: idx, Id: 111111111111111115, Subject: 22222222222222222, Predicate: 33333333333333, Object: rpc.ABool(true, 0)},
		rpc.Fact{Index: idx, Id: 111111111111111116, Subject: 22222222222222222, Predicate: 33333333333333, Object: rpc.AKID(22222222222222222)},
	}
}

func makeFactString(idx blog.Index) rpc.Fact {
	return rpc.Fact{
		Index:     idx,
		Id:        111111111111111111,
		Subject:   22222222222222222,
		Predicate: 33333333333333,
		Object:    rpc.AString("Hello Knowledge Graph", 0),
	}
}
