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

package diskview

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ebay/beam/blog"
	"github.com/ebay/beam/config"
	"github.com/ebay/beam/diskview/database"
	"github.com/ebay/beam/diskview/keys"
	_ "github.com/ebay/beam/diskview/rocksdb" // ensure rocksdb database type is registered
	"github.com/ebay/beam/logentry"
	"github.com/ebay/beam/logentry/logread"
	"github.com/ebay/beam/logentry/logwrite"
	"github.com/ebay/beam/msg/facts"
	"github.com/ebay/beam/rpc"
	"github.com/google/btree"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeTestDB(t testing.TB) (database.DB, func()) {
	dir, err := ioutil.TempDir("", "test")
	assert.NoError(t, err)
	db, err := database.New("rocksdb", database.FactoryArgs{
		Dir:    dir,
		Config: &config.DiskView{},
	})
	if !assert.NoError(t, err) {
		os.RemoveAll(dir)
		return nil, func() {}
	}
	return db, func() {
		db.Close()
		os.RemoveAll(dir)
	}
}

func poKey(id, subject, predicate uint64, object string, index blog.Index) []byte {
	return keys.FactKey{Fact: &rpc.Fact{
		Id:        id,
		Subject:   subject,
		Predicate: predicate,
		Object:    rpc.AString(object, 0),
		Index:     index,
	}, Encoding: rpc.KeyEncodingPOS}.Bytes()
}

func spKey(id, subject, predicate uint64, object string, index blog.Index) []byte {
	return keys.FactKey{Fact: &rpc.Fact{
		Id:        id,
		Subject:   subject,
		Predicate: predicate,
		Object:    rpc.AString(object, 0),
		Index:     index,
	}, Encoding: rpc.KeyEncodingSPO}.Bytes()
}

func Test_lookup_waitOnTransactions(t *testing.T) {
	assert := assert.New(t)
	db, cleanup := makeTestDB(t)
	defer cleanup()
	view := &DiskView{
		db:           db,
		nextPosition: logPos(3001, 1),
		pending:      btree.New(4),
	}
	// undecided transaction <= index
	tx1 := &transaction{
		position: logPos(400, 1),
		decided:  nil,
	}
	// undecided transaction > index
	tx2 := &transaction{
		position: logPos(401, 1),
		decided:  nil,
	}
	// decided transaction <= index
	tx3 := &transaction{
		position: logPos(400, 1),
		decided:  make(chan struct{}), // unbuffered,
	}
	waited := make(chan struct{})
	go func() {
		tx3.decided <- struct{}{}
		waited <- struct{}{}
	}()

	view.pending.ReplaceOrInsert(pendingItem{key: []byte("A"), tx: tx1})
	view.pending.ReplaceOrInsert(pendingItem{key: []byte("b"), tx: tx1})
	view.pending.ReplaceOrInsert(pendingItem{key: []byte("asdf"), tx: tx2})
	view.pending.ReplaceOrInsert(pendingItem{key: []byte("abc"), tx: tx3})

	enumerations := []enumerateSpec{{
		startKey: []byte("a"),
		endKey:   []byte("b"),
		emit: func(key []byte, fact rpc.Fact) error {
			return nil
		},
	}}
	err := view.lookup(context.Background(), 400, enumerations)
	assert.NoError(err)
	select {
	case <-waited:
		// ok
	case <-time.After(time.Second):
		// we need to give the above go() func that's triggering the channels
		// a chance to have gotten scheduled.
		assert.Fail("lookup didn't wait on tx3")
	}
}

func Test_LookupPO(t *testing.T) {
	assert := assert.New(t)
	db, cleanup := makeTestDB(t)
	defer cleanup()
	view := &DiskView{
		db:           db,
		pending:      btree.New(4),
		nextPosition: logPos(2, 1),
	}
	assert.NoError(db.Writes([]database.KV{
		{Key: poKey(1, 2, 3, "Bob", view.LastApplied())},
		{Key: poKey(1, 2, 3, "Bob's House", view.LastApplied())},
	}))
	req := &rpc.LookupPORequest{
		Index: view.LastApplied(),
		Lookups: []rpc.LookupPORequest_Item{
			{Predicate: 3, Object: rpc.AString("Bob", 0)},
		},
	}
	stream := rpc.MockChunkStream{}
	err := view.lookupPO(context.Background(), req, stream.Send)
	assert.NoError(err)
	result := stream.Flatten()
	if assert.Len(result.Facts, 1) {
		assert.Equal(uint32(0), result.Facts[0].Lookup)
		assert.Equal(uint64(2), result.Facts[0].Fact.Subject)
	}
}

func Test_LookupS(t *testing.T) {
	assert := assert.New(t)
	db, cleanup := makeTestDB(t)
	defer cleanup()
	view := &DiskView{
		db:           db,
		pending:      btree.New(4),
		nextPosition: logPos(12346, 1),
	}
	assert.NoError(db.Writes([]database.KV{
		{Key: spKey(1, 2, 3, "Bob", view.LastApplied())},
		{Key: spKey(1, 2, 3, "Bob's House", view.LastApplied())},
		{Key: spKey(1, 6, 3, "Koala", view.LastApplied())},
	}))
	req := &rpc.LookupSRequest{
		Index:    view.LastApplied(),
		Subjects: []uint64{50, 2},
	}
	stream := &rpc.MockChunkStream{}
	err := view.lookupS(context.Background(), req, stream.Send)
	assert.NoError(err)
	result := stream.Flatten()
	if assert.Len(result.Facts, 2) {
		assert.Equal(uint32(1), result.Facts[0].Lookup)
		assert.Equal(uint64(3), result.Facts[0].Fact.Predicate)
		assert.Equal(uint32(1), result.Facts[1].Lookup)
		assert.Equal(uint64(3), result.Facts[1].Fact.Predicate)
	}
}

func Test_LookupSP(t *testing.T) {
	assert := assert.New(t)
	db, cleanup := makeTestDB(t)
	defer cleanup()
	view := &DiskView{
		db:           db,
		nextPosition: logPos(41, 1),
		pending:      btree.New(4),
	}
	assert.NoError(db.Writes([]database.KV{
		{Key: spKey(1, 2, 3, "Arrow", 20)},
		{Key: spKey(4, 2, 5, "sp mismatch", 20)},
		{Key: spKey(6, 5, 8, "Carolina", 20)},
		{Key: spKey(9, 2, 3, "index too high", 38)},
		{Key: spKey(7, 2, 3, "Bob's House", 30)},
	}))
	req := &rpc.LookupSPRequest{
		Index: 34,
		Lookups: []rpc.LookupSPRequest_Item{
			{Subject: 2, Predicate: 3},
			{Subject: 5, Predicate: 8},
		},
	}
	stream := new(rpc.MockChunkStream)
	err := view.lookupSP(context.Background(), req, stream.Send)
	assert.NoError(err)
	result := stream.Flatten()
	if assert.Len(result.Facts, 3) {
		assert.Equal(uint32(0), result.Facts[0].Lookup)
		assert.Equal(rpc.Fact{
			Id:     1,
			Object: rpc.AString("Arrow", 0),
			Index:  20,
		}, result.Facts[0].Fact)
		assert.Equal(uint32(0), result.Facts[1].Lookup)
		assert.Equal(rpc.Fact{
			Id:     7,
			Object: rpc.AString("Bob's House", 0),
			Index:  30,
		}, result.Facts[1].Fact)
		assert.Equal(uint32(1), result.Facts[2].Lookup)
		assert.Equal(rpc.Fact{
			Id:     6,
			Object: rpc.AString("Carolina", 0),
			Index:  20,
		}, result.Facts[2].Fact)
	}
}

func Test_LookupSPO(t *testing.T) {
	assert := assert.New(t)
	db, cleanup := makeTestDB(t)
	defer cleanup()
	view := &DiskView{
		db:           db,
		nextPosition: logPos(41, 1),
		pending:      btree.New(4),
	}
	assert.NoError(db.Writes([]database.KV{
		{Key: spKey(1, 2, 3, "Arrow", 20)},
		{Key: spKey(9, 2, 3, "Arrow", 38)}, // index too high
		{Key: spKey(6, 5, 8, "Carolina", 30)},
	}))
	req := &rpc.LookupSPORequest{
		Index: 34,
		Lookups: []rpc.LookupSPORequest_Item{
			{Subject: 2, Predicate: 3, Object: rpc.AString("Arrow", 0)},
			{Subject: 5, Predicate: 8, Object: rpc.AString("Carolina", 0)},
			{Subject: 7, Predicate: 8, Object: rpc.AString("Arkansas", 0)},
		},
	}
	stream := new(rpc.MockChunkStream)
	err := view.lookupSPO(context.Background(), req, stream.Send)
	assert.NoError(err)
	result := stream.Flatten()
	if assert.Len(result.Facts, 2) {
		assert.Equal(uint32(0), result.Facts[0].Lookup)
		assert.Equal(rpc.Fact{
			Id:    1,
			Index: 20,
		}, result.Facts[0].Fact)
		assert.Equal(uint32(1), result.Facts[1].Lookup)
		assert.Equal(rpc.Fact{
			Id:    6,
			Index: 30,
		}, result.Facts[1].Fact)
	}
}

// This is a sanity check for LookupPOCmp itself, but most of the testing effort
// should be focused on enumeratePOCmp.
func Test_LookupPOCmp(t *testing.T) {
	assert := assert.New(t)
	db, cleanup := makeTestDB(t)
	defer cleanup()
	view := &DiskView{
		db:           db,
		nextPosition: logPos(101, 1),
		pending:      btree.New(4),
	}
	assert.NoError(db.Writes([]database.KV{
		{Key: poKey(10, 20, 3, "Bob's Boat", 1)},
		{Key: poKey(12, 22, 3, "Bob's Car", 60)},
		{Key: poKey(11, 21, 3, "Bob's House", 2)},
		{Key: poKey(13, 60, 3, "Koala", 3)},
	}))
	req := &rpc.LookupPOCmpRequest{
		Index: 50,
		Lookups: []rpc.LookupPOCmpRequest_Item{
			{
				Predicate: 3,
				Operator:  rpc.OpPrefix,
				Object:    rpc.AString("Bob", 0),
			},
			{
				Predicate: 6,
				Operator:  rpc.OpPrefix,
				Object:    rpc.AString("Bob", 0),
			},
			{
				Predicate: 3,
				Operator:  rpc.OpGreaterOrEqual,
				Object:    rpc.AString("Jello", 0),
			},
		},
	}
	stream := new(rpc.MockChunkStream)
	err := view.lookupPOCmp(context.Background(), req, stream.Send)
	assert.NoError(err)
	result := stream.Flatten()
	if assert.Len(result.Facts, 3) {
		assert.Equal(uint32(0), result.Facts[0].Lookup)
		assert.Equal(rpc.Fact{
			Id:        10,
			Subject:   20,
			Predicate: 0,
			Object:    rpc.AString("Bob's Boat", 0),
			Index:     1,
		}, result.Facts[0].Fact)
		assert.Equal(uint32(0), result.Facts[1].Lookup)
		assert.Equal(rpc.Fact{
			Id:        11,
			Subject:   21,
			Predicate: 0,
			Object:    rpc.AString("Bob's House", 0),
			Index:     2,
		}, result.Facts[1].Fact)
		assert.Equal(uint32(2), result.Facts[2].Lookup)
		assert.Equal(rpc.Fact{
			Id:        13,
			Subject:   60,
			Predicate: 0,
			Object:    rpc.AString("Koala", 0),
			Index:     3,
		}, result.Facts[2].Fact)
	}
}

func Test_EnumerateKeyPrefixNoMatches(t *testing.T) {
	stored := []rpc.Fact{{Index: 5, Id: 1, Subject: 2, Predicate: 3, Object: rpc.AKID(4)}}
	validateEnumerateKeys(t, stored, rpc.KeyEncodingSPO, keys.KeyPrefixSubjectPredicate(3, 1), 100, []rpc.Fact{})
}

func Test_EnumerateKeyPrefixOneMatch(t *testing.T) {
	stored := []rpc.Fact{{Index: 5, Id: 1, Subject: 2, Predicate: 3, Object: rpc.AKID(4)}}
	validateEnumerateKeys(t, stored, rpc.KeyEncodingSPO, keys.KeyPrefixSubjectPredicate(2, 3), 100, stored)
}
func Test_EnumerateKeyPrefixIndexFiltering(t *testing.T) {
	stored := []rpc.Fact{
		{Index: 5, Id: 1, Subject: 2, Predicate: 3, Object: rpc.AKID(4)},
		{Index: 15, Id: 1, Subject: 2, Predicate: 3, Object: rpc.AKID(4)},
		{Index: 115, Id: 1, Subject: 2, Predicate: 3, Object: rpc.AKID(4)},
		{Index: 125, Id: 21, Subject: 2, Predicate: 4, Object: rpc.AKID(4)},
	}
	validateEnumerateKeys(t, stored, rpc.KeyEncodingSPO, keys.KeyPrefixSubjectPredicate(2, 3), 4, []rpc.Fact{})
	validateEnumerateKeys(t, stored, rpc.KeyEncodingSPO, keys.KeyPrefixSubjectPredicate(2, 3), 5, stored[:1])
	validateEnumerateKeys(t, stored, rpc.KeyEncodingSPO, keys.KeyPrefixSubjectPredicate(2, 3), 25, stored[1:2])
	validateEnumerateKeys(t, stored, rpc.KeyEncodingSPO, keys.KeyPrefixSubjectPredicate(2, 4), 120, []rpc.Fact{})
	validateEnumerateKeys(t, stored, rpc.KeyEncodingSPO, keys.KeyPrefixSubjectPredicate(2, 4), 125, stored[3:4])
}

func Test_EnumerateKeyPrefixMultipleMatches(t *testing.T) {
	stored := []rpc.Fact{
		{Index: 6, Id: 2, Subject: 2, Predicate: 3, Object: rpc.AKID(31)},
		{Index: 7, Id: 3, Subject: 2, Predicate: 3, Object: rpc.AKID(32)},
		{Index: 8, Id: 4, Subject: 2, Predicate: 3, Object: rpc.AKID(33)},
		{Index: 5, Id: 1, Subject: 2, Predicate: 3, Object: rpc.AKID(41)},
		{Index: 8, Id: 5, Subject: 1, Predicate: 3, Object: rpc.AKID(33)},
	}
	validateEnumerateKeys(t, stored, rpc.KeyEncodingSPO, keys.KeyPrefixSubjectPredicate(2, 3), 10, stored[0:4])
}

func validateEnumerateKeys(t *testing.T, keysSource []rpc.Fact, keyEncoding rpc.FactKeyEncoding, prefix []byte, idx blog.Index, expected []rpc.Fact) {
	k := make([][]byte, len(keysSource))
	for i := range keysSource {
		k[i] = keys.FactKey{Encoding: keyEncoding, Fact: &keysSource[i]}.Bytes()
	}
	sort.SliceStable(k, func(i, j int) bool {
		return bytes.Compare(k[i], k[j]) < 0
	})
	actual := make([]rpc.Fact, 0, len(expected))
	snap := &mockKeyEnum{rows: k}
	log.Printf("Prefix %v", prefix)
	spec := enumerateSpec{
		startKey: prefix,
		endKey:   incremented(prefix),
		emit: func(key []byte, f rpc.Fact) error {
			log.Printf("Emit %v %v", key, f)
			actual = append(actual, f)
			return nil
		},
	}
	err := enumerateDB(snap, spec, idx)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
	assert.False(t, snap.closed)
	assert.NoError(t, snap.Close())
}

type mockKeyEnum struct {
	rows   [][]byte
	closed bool
}

func (m *mockKeyEnum) Close() error {
	m.closed = true
	return nil
}
func (m *mockKeyEnum) EnumerateKeys(startKey, endKey []byte, emit func(key []byte) error) error {
	// find start key
	offset := 0
	for ; offset < len(m.rows) && bytes.Compare(m.rows[offset], startKey) < 0; offset++ {
	}
	// emit the remaining keys
	for ; offset < len(m.rows) && bytes.Compare(m.rows[offset], endKey) < 0; offset++ {
		err := emit(m.rows[offset])
		if err != nil {
			if err == database.ErrHalt {
				return nil
			}
			return err
		}
	}
	return nil
}

type lookupTestSuite struct {
	dir      string
	view     *DiskView
	aSubject uint64
	myPred   uint64
}

func Test_LookupCmp(t *testing.T) {
	s := setupLookupTestSuite(t)
	defer s.tearDownSuite()
	t.Run("Greater", s.testGreater)
	t.Run("Less", s.testLessThan)
	t.Run("Equal", s.testEqual)
	t.Run("RangeIncInc", s.testRangeIncInc)
	t.Run("RangeIncExc", s.testRangeIncExc)
	t.Run("RangeExcInc", s.testRangeExcInc)
	t.Run("RangeExcExc", s.testRangeExcExc)
	t.Run("Prefix", s.testPrefix)
}

func setupLookupTestSuite(t *testing.T) *lookupTestSuite {
	s := lookupTestSuite{}
	var err error
	s.dir, err = ioutil.TempDir("", t.Name())
	require.NoError(t, err)
	cfg := &config.Beam{
		DiskView: &config.DiskView{
			Space:     "hashpo",
			FirstHash: "0x0000000000000000",
			LastHash:  "0xffffffffffffffff",
			Backend:   "rocksdb",
			Dir:       s.dir,
		},
	}
	s.view, err = New(cfg)
	require.NoError(t, err)
	b := new(logwrite.InsertFactBuilder)
	myPred := int32(500)
	aSubject := int32(501)
	nf := logentry.InsertTxCommand{
		Facts: []logentry.InsertFact{
			b.SOffset(myPred).PID(facts.InstanceOf).OKID(facts.Predicate).Fact(),
			b.SOffset(aSubject).POffset(myPred).OString("a", 0).Fact(),
			b.SOffset(aSubject).POffset(myPred).OString("aa", 0).Fact(),
			b.SOffset(aSubject).POffset(myPred).OString("b", 0).Fact(),
			b.SOffset(aSubject).POffset(myPred).OString("car", 0).Fact(),
			b.SOffset(aSubject).POffset(myPred).OString("fred", 0).Fact(),
			b.SOffset(aSubject).POffset(myPred).OInt64(1, 0).Fact(),
			b.SOffset(aSubject).POffset(myPred).OInt64(2, 0).Fact(),
			b.SOffset(aSubject).POffset(myPred).OInt64(3, 0).Fact(),
			b.SOffset(aSubject).POffset(myPred).OInt64(4, 0).Fact(),
			b.SOffset(aSubject).POffset(myPred).OInt64(5, 0).Fact(),
			b.SOffset(aSubject).POffset(myPred).OInt64(6, 0).Fact(),
			b.SOffset(aSubject).POffset(myPred).OFloat64(1, 0).Fact(),
			b.SOffset(aSubject).POffset(myPred).OFloat64(2, 0).Fact(),
			b.SOffset(aSubject).POffset(myPred).OFloat64(3, 0).Fact(),
			b.SOffset(aSubject).POffset(myPred).OFloat64(4, 0).Fact(),
			b.SOffset(aSubject).POffset(myPred).OFloat64(5, 0).Fact(),
			b.SOffset(aSubject).POffset(myPred).OFloat64(6, 0).Fact(),
		},
	}
	logPosition := logPos(blog.Index(123), 1)
	effects := applyEffects{
		dbWriter: s.view.db.BulkWrite(),
	}
	s.view.applyInsertFactsAt(logPosition.Index, &nf, &effects)
	assert.NoError(t, effects.dbWriter.Close())
	s.view.nextPosition = logPos(124, 1)
	s.aSubject = logread.KID(logPosition.Index, aSubject)
	s.myPred = logread.KID(logPosition.Index, myPred)
	return &s
}

func (s *lookupTestSuite) tearDownSuite() {
	s.view.Close()
	os.RemoveAll(s.dir)
}

func (s *lookupTestSuite) testGreater(t *testing.T) {
	req := rpc.LookupPOCmpRequest_Item{
		Predicate: s.myPred,
		Object:    rpc.AString("bobby", 0),
		Operator:  rpc.OpGreater,
	}
	s.assertLookupPOCmp(t, req, rpc.AString("car", 0), rpc.AString("fred", 0))

	req.Object = rpc.AInt64(4, 0)
	s.assertLookupPOCmp(t, req, rpc.AInt64(5, 0), rpc.AInt64(6, 0))

	req.Object = rpc.AFloat64(4, 0)
	s.assertLookupPOCmp(t, req, rpc.AFloat64(5, 0), rpc.AFloat64(6, 0))

	req.Operator = rpc.OpGreaterOrEqual
	req.Object = rpc.AInt64(4, 0)
	s.assertLookupPOCmp(t, req, rpc.AInt64(4, 0), rpc.AInt64(5, 0), rpc.AInt64(6, 0))

	req.Object = rpc.AFloat64(4, 0)
	s.assertLookupPOCmp(t, req, rpc.AFloat64(4, 0), rpc.AFloat64(5, 0), rpc.AFloat64(6, 0))

	req.Object = rpc.AString("bobby", 0)
	s.assertLookupPOCmp(t, req, rpc.AString("car", 0), rpc.AString("fred", 0))

	req.Object = rpc.AString("aa", 0)
	s.assertLookupPOCmp(t, req, rpc.AString("aa", 0), rpc.AString("b", 0), rpc.AString("car", 0), rpc.AString("fred", 0))
}

func (s *lookupTestSuite) testLessThan(t *testing.T) {
	req := rpc.LookupPOCmpRequest_Item{
		Object:    rpc.AString("car", 0),
		Operator:  rpc.OpLess,
		Predicate: s.myPred,
	}
	s.assertLookupPOCmp(t, req, rpc.AString("a", 0), rpc.AString("aa", 0), rpc.AString("b", 0))

	req.Operator = rpc.OpLessOrEqual
	s.assertLookupPOCmp(t, req, rpc.AString("a", 0), rpc.AString("aa", 0), rpc.AString("b", 0), rpc.AString("car", 0))

	req.Operator = rpc.OpLess
	req.Object = rpc.AInt64(3, 0)
	s.assertLookupPOCmp(t, req, rpc.AInt64(1, 0), rpc.AInt64(2, 0))

	req.Operator = rpc.OpLessOrEqual
	s.assertLookupPOCmp(t, req, rpc.AInt64(1, 0), rpc.AInt64(2, 0), rpc.AInt64(3, 0))

	req.Operator = rpc.OpLess
	req.Object = rpc.AFloat64(3, 0)
	s.assertLookupPOCmp(t, req, rpc.AFloat64(1, 0), rpc.AFloat64(2, 0))

	req.Operator = rpc.OpLessOrEqual
	s.assertLookupPOCmp(t, req, rpc.AFloat64(1, 0), rpc.AFloat64(2, 0), rpc.AFloat64(3, 0))
}

func (s *lookupTestSuite) testEqual(t *testing.T) {
	req := rpc.LookupPOCmpRequest_Item{
		Predicate: s.myPred,
		Operator:  rpc.OpEqual,
		Object:    rpc.AString("car", 0),
	}
	s.assertLookupPOCmp(t, req, rpc.AString("car", 0))

	req.Object = rpc.AString("c", 0)
	s.assertLookupPOCmp(t, req)

	req.Object = rpc.AInt64(5, 0)
	s.assertLookupPOCmp(t, req, rpc.AInt64(5, 0))

	req.Object = rpc.AInt64(42, 0)
	s.assertLookupPOCmp(t, req)
}

func (s *lookupTestSuite) testRangeIncInc(t *testing.T) {
	req := rpc.LookupPOCmpRequest_Item{
		Predicate: s.myPred,
		Operator:  rpc.OpRangeIncInc,
		Object:    rpc.AString("aa", 0),
		EndObject: rpc.AString("car", 0),
	}
	s.assertLookupPOCmp(t, req, rpc.AString("aa", 0), rpc.AString("b", 0), rpc.AString("car", 0))

	req.EndObject = rpc.AString("card", 0)
	s.assertLookupPOCmp(t, req, rpc.AString("aa", 0), rpc.AString("b", 0), rpc.AString("car", 0))

	req.Object = rpc.AString("aaa", 0)
	s.assertLookupPOCmp(t, req, rpc.AString("b", 0), rpc.AString("car", 0))

	req.Object = rpc.AInt64(3, 0)
	req.EndObject = rpc.AInt64(6, 0)
	s.assertLookupPOCmp(t, req, rpc.AInt64(3, 0), rpc.AInt64(4, 0), rpc.AInt64(5, 0), rpc.AInt64(6, 0))

	req.Object = rpc.AInt64(-1, 0)
	req.EndObject = rpc.AInt64(2, 0)
	s.assertLookupPOCmp(t, req, rpc.AInt64(1, 0), rpc.AInt64(2, 0))
}

func (s *lookupTestSuite) testRangeIncExc(t *testing.T) {
	req := rpc.LookupPOCmpRequest_Item{
		Predicate: s.myPred,
		Operator:  rpc.OpRangeIncExc,
		Object:    rpc.AString("aa", 0),
		EndObject: rpc.AString("car", 0),
	}
	s.assertLookupPOCmp(t, req, rpc.AString("aa", 0), rpc.AString("b", 0))

	req.EndObject = rpc.AString("card", 0)
	s.assertLookupPOCmp(t, req, rpc.AString("aa", 0), rpc.AString("b", 0), rpc.AString("car", 0))
}

func (s *lookupTestSuite) testRangeExcInc(t *testing.T) {
	req := rpc.LookupPOCmpRequest_Item{
		Predicate: s.myPred,
		Operator:  rpc.OpRangeExcInc,
		Object:    rpc.AString("aa", 0),
		EndObject: rpc.AString("car", 0),
	}
	s.assertLookupPOCmp(t, req, rpc.AString("b", 0), rpc.AString("car", 0))
}

func (s *lookupTestSuite) testRangeExcExc(t *testing.T) {
	req := rpc.LookupPOCmpRequest_Item{
		Predicate: s.myPred,
		Operator:  rpc.OpRangeExcExc,
		Object:    rpc.AString("aa", 0),
		EndObject: rpc.AString("car", 0),
	}
	s.assertLookupPOCmp(t, req, rpc.AString("b", 0))
}

func (s *lookupTestSuite) testPrefix(t *testing.T) {
	req := rpc.LookupPOCmpRequest_Item{
		Predicate: s.myPred,
		Operator:  rpc.OpPrefix,
		Object:    rpc.AString("a", 0),
	}
	s.assertLookupPOCmp(t, req, rpc.AString("a", 0), rpc.AString("aa", 0))
}

func (s *lookupTestSuite) assertLookupPOCmp(t *testing.T, req rpc.LookupPOCmpRequest_Item, expKGObjects ...interface{}) {
	t.Helper()
	snap := s.view.db.Snapshot()
	defer snap.Close()
	var facts []rpc.Fact
	spec := poCmpSpec(req, 5000)
	spec.emit = func(key []byte, fact rpc.Fact) error {
		facts = append(facts, fact)
		return nil
	}
	err := enumerateDB(snap, spec, 5000)
	assert.NoError(t, err)
	assert.Len(t, facts, len(expKGObjects))
	for i := range expKGObjects {
		if i < len(facts) {
			assert.Equal(t, expKGObjects[i], facts[i].Object)
		} else {
			assert.Equal(t, expKGObjects[i], rpc.AString("(missing fact)", 0))
		}
	}
}

func Test_waitForIndex(t *testing.T) {
	assert := assert.New(t)
	view := &DiskView{
		nextPosition: logPos(41, 1),
	}
	view.updateCond = sync.NewCond(view.lock.RLocker())
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
	defer cancelFunc()

	err := view.waitForIndex(ctx, 39)
	assert.NoError(err)
	err = view.waitForIndex(ctx, 40)
	assert.NoError(err)

	// The following test used to wedge before the if condition was added around
	// updateCond.Wait().
	go func() {
		view.lock.Lock()
		view.nextPosition.Index++
		view.updateCond.Broadcast()
		view.lock.Unlock()
	}()
	err = view.waitForIndex(ctx, 41)
	assert.NoError(err)
}

func Test_increment(t *testing.T) {
	assert := assert.New(t)
	x := []byte{0, 0, 0}
	increment(x)
	assert.Equal([]byte{0, 0, 1}, x)
	x = []byte{0, 0, 255}
	increment(x)
	assert.Equal([]byte{0, 1, 0}, x)
}
