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

// Package keys provides support for building and parsing the DiskView's binary
// key format that facts are encoded into.
package keys

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/rpc"
	log "github.com/sirupsen/logrus"
)

const (
	// Min size of an encoded fact is prefix(1) + subj,pred,id,index(4x8), + obj(at least 1) = 34.
	minFactKeySize = 34
)

var (
	prefixPOSBytes = []byte("p")
	prefixSPOBytes = []byte("s")

	viewMetaKeyBytes  = []byte("viewmeta")
	viewStatsKeyBytes = []byte("viewstats")
)

// Spec is a common interface that all Key types implement.
type Spec interface {
	isKeySpec()
	// Returns the raw bytes version of this key.
	Bytes() []byte
}

// FactKey can be used to generate a fully serialized Fact in the specified
// encoding
type FactKey struct {
	Fact     *rpc.Fact
	Encoding rpc.FactKeyEncoding
}

func (fk FactKey) isKeySpec() {}

// Bytes returns the fact encoded in the relevant.
func (fk FactKey) Bytes() []byte {
	switch fk.Encoding {
	case rpc.KeyEncodingPOS:
		return encodePOS(fk.Fact, posFull)

	case rpc.KeyEncodingSPO:
		return encodeSPO(fk.Fact, spoFull)

	default:
		panic(fmt.Sprintf("Unexpected FactKeyEncoding in FactKey: %d", fk.Encoding))
	}
}

// posKeyPrefix defines specific logical prefixes of a POS encoded key.
type posKeyPrefix int

// spoKeyPrefix defines specific logical prefixes of a SPO encoded key.
type spoKeyPrefix int

const (
	// These should be in an order that represents increasingly larger prefixes.
	posPredicate posKeyPrefix = iota
	posPredicateObjectType
	posPredicateObjectNoLang
	posPredicateObjectSubject
	posFull

	// These should be in an order that represents increasingly larger prefixes.
	spoSubject spoKeyPrefix = iota
	spoSubjectPredicate
	spoSubjectPredicateObjectNoLang
	spoSubjectPredicateObject
	spoFull
)

// encodePOS returns the fact encoded as a byte key in the POS space. 'to'
// specifies how much of the key should be in the output.
func encodePOS(f *rpc.Fact, to posKeyPrefix) []byte {
	if to < posPredicate || to > posFull {
		panic(fmt.Sprintf("Invalid posKeyPrefix of %d passed to encodePOS", to))
	}
	var b bytes.Buffer
	tmp := make([]byte, 8)
	appendUInt64 := func(v uint64) {
		binary.BigEndian.PutUint64(tmp, v)
		b.Write(tmp)
	}
	// [prefix_POS][pred_8][obj_pb][subj_8][id_8][idx_8]
	b.Write(prefixPOSBytes)
	appendUInt64(f.Predicate)

	switch {
	case to == posPredicateObjectType:
		b.Write(f.Object.TypePrefix())

	case to == posPredicateObjectNoLang:
		f.Object.WriteTo(&b, rpc.WriteOpts{NoLangID: true})

	case to >= posPredicateObjectSubject:
		f.Object.WriteTo(&b, rpc.WriteOpts{NoLangID: false})
		appendUInt64(f.Subject)

		if to == posFull {
			appendUInt64(f.Id)
			appendUInt64(f.Index)
		}
	}
	return b.Bytes()
}

// encodeSPO returns the fact encoded as a byte key in the SPO space. 'to'
// specifies how much of the key should be in the output.
func encodeSPO(f *rpc.Fact, to spoKeyPrefix) []byte {
	if to < spoSubject || to > spoFull {
		panic(fmt.Sprintf("Invalid spoKeyPrefix of %d passed to encodeSPO", to))
	}
	var b bytes.Buffer
	tmp := make([]byte, 8)
	appendUInt64 := func(v uint64) {
		binary.BigEndian.PutUint64(tmp, v)
		b.Write(tmp)
	}
	// [prefix_SPO][sub_8][pred_8][obj_pb][id_8][idx_8]
	b.Write(prefixSPOBytes)
	appendUInt64(f.Subject)

	if to >= spoSubjectPredicate {
		appendUInt64(f.Predicate)

		switch {
		case to == spoSubjectPredicateObjectNoLang:
			f.Object.WriteTo(&b, rpc.WriteOpts{NoLangID: true})

		case to >= spoSubjectPredicateObject:
			f.Object.WriteTo(&b, rpc.WriteOpts{NoLangID: false})

			if to == spoFull {
				appendUInt64(f.Id)
				appendUInt64(f.Index)
			}
		}
	}
	return b.Bytes()
}

// KeyPrefixPredicate returns a byte key prefix containing the supplied
// predicate in the POS space.
func KeyPrefixPredicate(p uint64) []byte {
	return encodePOS(&rpc.Fact{Predicate: p}, posPredicate)
}

// KeyPrefixPredicateObjectType returns a byte key prefix containing the supplied
// predicate and Object type in the POS space.
func KeyPrefixPredicateObjectType(p uint64, obj rpc.KGObject) []byte {
	return encodePOS(&rpc.Fact{Predicate: p, Object: obj}, posPredicateObjectType)
}

// KeyPrefixPredicateObjectNoLang returns a byte key prefix containing the
// supplied predicate and object value in the POS space. If the object value is
// of type string, the language identifier is not included in the returned key.
func KeyPrefixPredicateObjectNoLang(p uint64, obj rpc.KGObject) []byte {
	return encodePOS(&rpc.Fact{Predicate: p, Object: obj}, posPredicateObjectNoLang)
}

// KeyPrefixSubject returns a byte key prefix containing the supplied subject in
// the SPO space.
func KeyPrefixSubject(s uint64) []byte {
	return encodeSPO(&rpc.Fact{Subject: s}, spoSubject)
}

// KeyPrefixSubjectPredicate returns a byte key prefix containing the supplied
// subject and predicate in the SPO space.
func KeyPrefixSubjectPredicate(s, p uint64) []byte {
	return encodeSPO(&rpc.Fact{Subject: s, Predicate: p}, spoSubjectPredicate)
}

// KeyPrefixSubjectPredicateObjectNoLang returns a byte key prefix containing
// the supplied subject, predicate & object in the SPO space. If the Object is
// of type string, the Language identifier is not included in the returned key
// prefix.
func KeyPrefixSubjectPredicateObjectNoLang(s, p uint64, obj rpc.KGObject) []byte {
	return encodeSPO(&rpc.Fact{Subject: s, Predicate: p, Object: obj}, spoSubjectPredicateObjectNoLang)
}

// FactKeysEqualIgnoreIndex returns true if the 2 supplied keys are for the same fact,
// ignoring the log index part of the key.
func FactKeysEqualIgnoreIndex(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	return len(a) >= minFactKeySize && bytes.Equal(a[:len(a)-8], b[:len(b)-8])
}

// ParseIndex extracts the index from the provided key [if it has one] and returns it
// otherwise returns 0.
func ParseIndex(key []byte) blog.Index {
	if len(key) < 8 {
		log.Debugf("keys.ParseIndex called with key with no index: %s", key)
		return 0
	}
	return binary.BigEndian.Uint64(key[len(key)-8:])
}

// MetaKey represents the well known key for that contains the current diskview
// metadata.
type MetaKey struct {
}

func (m MetaKey) isKeySpec() {}

// Bytes returns the byte key version of the MetaKey.
func (m MetaKey) Bytes() []byte {
	return append([]byte(nil), viewMetaKeyBytes...)
}

// StatsKey represents the static key that is used to store fact statistics.
type StatsKey struct {
}

func (s StatsKey) isKeySpec() {}

// Bytes returns the key bytes for the StatsKey.
func (s StatsKey) Bytes() []byte {
	return append([]byte(nil), viewStatsKeyBytes...)
}

// ParseKey will parse a serialized key and return a FactKey, MetaKey, StatsKey
// or nil/error
func ParseKey(key []byte) (Spec, error) {
	switch {
	case bytes.HasPrefix(key, prefixPOSBytes) && len(key) >= minFactKeySize:
		return toFactKey(rpc.KeyEncodingPOS, key)

	case bytes.HasPrefix(key, prefixSPOBytes) && len(key) >= minFactKeySize:
		return toFactKey(rpc.KeyEncodingSPO, key)

	case bytes.Equal(key, viewMetaKeyBytes):
		return MetaKey{}, nil

	case bytes.Equal(key, viewStatsKeyBytes):
		return StatsKey{}, nil
	}
	return nil, fmt.Errorf("keys.ParseKey unable to determine key type: %s", key)
}

func toFactKey(enc rpc.FactKeyEncoding, key []byte) (FactKey, error) {
	switch enc {
	case rpc.KeyEncodingPOS:
		// prefix[pred_8][obj_pb][sub_8][id_8][idx_8]
		key = key[len(prefixPOSBytes):]
		l := len(key)
		p := binary.BigEndian.Uint64(key[:8])
		obj, err := rpc.KGObjectFromBytes(key[8 : l-24])
		sub := binary.BigEndian.Uint64(key[l-24 : l-16])
		id := binary.BigEndian.Uint64(key[l-16 : l-8])
		idx := binary.BigEndian.Uint64(key[l-8:])
		if err != nil {
			return FactKey{}, err
		}
		return FactKey{
			Encoding: enc,
			Fact: &rpc.Fact{
				Index:     idx,
				Id:        id,
				Subject:   sub,
				Predicate: p,
				Object:    obj,
			},
		}, nil

	case rpc.KeyEncodingSPO:
		// prefix[sub_8][pred_8][obj_pb][id_8][idx_8]
		key = key[len(prefixSPOBytes):]
		l := len(key)
		sub := binary.BigEndian.Uint64(key[:8])
		p := binary.BigEndian.Uint64(key[8:16])
		obj, err := rpc.KGObjectFromBytes(key[16 : l-16])
		id := binary.BigEndian.Uint64(key[l-16 : l-8])
		idx := binary.BigEndian.Uint64(key[l-8:])
		if err != nil {
			return FactKey{}, err
		}
		return FactKey{
			Encoding: enc,
			Fact: &rpc.Fact{
				Index:     idx,
				Id:        id,
				Subject:   sub,
				Predicate: p,
				Object:    obj,
			},
		}, nil

	default:
		panic(fmt.Sprintf("Unexpected FactKeyEncoding passed to toFactKey() %d", enc))
	}
}
