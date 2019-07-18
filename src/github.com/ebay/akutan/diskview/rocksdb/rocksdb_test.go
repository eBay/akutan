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

package rocksdb

import (
	"encoding/binary"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"testing"

	"github.com/ebay/akutan/config"
	"github.com/ebay/akutan/util/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tecbot/gorocksdb"
)

func TestEnumerateKeyRange(t *testing.T) {
	assert := assert.New(t)
	db, clean := makeTestDb(t)
	defer clean()
	rdb := RocksDB{
		db:   db,
		ropt: gorocksdb.NewDefaultReadOptions(),
		wopt: gorocksdb.NewDefaultWriteOptions(),
	}
	assert.NoError(rdb.Write([]byte("a"), nil))
	assert.NoError(rdb.Write([]byte("aa"), nil))
	assert.NoError(rdb.Write([]byte("ab"), nil))
	assert.NoError(rdb.Write([]byte("abc"), nil))
	assert.NoError(rdb.Write([]byte("ac"), nil))
	assert.NoError(rdb.Write([]byte("acdc"), nil))
	snap := rdb.Snapshot()
	defer snap.Close()
	enumerate := func(startKey, endKey string) []string {
		var keys []string
		err := snap.EnumerateKeys([]byte(startKey), []byte(endKey),
			func(key []byte) error {
				keys = append(keys, string(key))
				return nil
			})
		assert.NoError(err)
		return keys
	}
	assert.Equal([]string{"ab", "abc", "ac"},
		enumerate("aardvark", "acceptable"))
	assert.Equal([]string{"aa", "ab", "abc"},
		enumerate("aa", "ac"))
	assert.Equal([]string{"a", "aa", "ab", "abc", "ac", "acdc"},
		enumerate("a", "z"))
	assert.Len(enumerate("abe", "abolish"), 0)
}

func TestUpdateDelete(t *testing.T) {
	db, clean := makeTestDb(t)
	defer clean()
	rdb := RocksDB{
		db:   db,
		ropt: gorocksdb.NewDefaultReadOptions(),
		wopt: gorocksdb.NewDefaultWriteOptions(),
	}

	k1 := []byte("k1")
	v1 := []byte("v1")
	bw := rdb.BulkWrite()
	assert.NoError(t, bw.Buffer(k1, v1))
	assert.NoError(t, bw.Remove(k1))
	assert.NoError(t, bw.Close())
	v, err := rdb.Read(k1)
	assert.NoError(t, err)
	assert.Nil(t, v)

	bw = rdb.BulkWrite()
	assert.NoError(t, bw.Remove(k1))
	assert.NoError(t, bw.Buffer(k1, v1))
	assert.NoError(t, bw.Close())
	v, err = rdb.Read(k1)
	assert.NoError(t, err)
	assert.Equal(t, v1, v)
}

func init() {
	random.SeedMath()
}

func Test_BulkWriteCgo(t *testing.T) {
	db, cleanup := makeTestDb(t)
	defer cleanup()
	rdb := RocksDB{
		db:   db,
		ropt: gorocksdb.NewDefaultReadOptions(),
		wopt: gorocksdb.NewDefaultWriteOptions(),
	}

	val := make([]byte, 32)
	rand.Read(val)
	key := make([]byte, 32)
	wb := rdb.BulkWrite()
	for key[0] = 0; key[0] < 200; key[0]++ {
		val[0] = key[0]
		wb.Buffer(key, val)
		if key[0] == 75 {
			require.NoError(t, wb.Close())
		}
	}
	require.NoError(t, wb.Close())
	for key[0] = 0; key[0] < 200; key[0]++ {
		v, err := db.Get(rdb.ropt, key)
		require.NoError(t, err)
		defer v.Free()
		val[0] = key[0]
		require.Equal(t, v.Data(), val)
	}

	for key[0] = 0; key[0] < 200; key[0]++ {
		if 0 == key[0]%3 {
			wb.Remove(key)
		}
		if key[0] == 75 {
			require.NoError(t, wb.Close())
		}
	}
	require.NoError(t, wb.Close())
	for key[0] = 0; key[0] < 200; key[0]++ {
		v, err := db.Get(rdb.ropt, key)
		require.NoError(t, err)
		if 0 != key[0]%3 {
			val[0] = key[0]
			require.Equal(t, v.Data(), val)
			continue
		}
		require.Equal(t, v.Size(), 0)
	}
	// check that the WriteBuffer regularly flushes
	for i := uint32(0); i < 10000; i++ {
		binary.BigEndian.PutUint32(key, i)
		wb.Buffer(key, key)
	}
	binary.BigEndian.PutUint32(key, 12)
	v, err := db.Get(rdb.ropt, key)
	assert.NoError(t, err)
	defer v.Free()
	assert.Equal(t, key, v.Data())
	assert.NoError(t, wb.Close())
}

func Test_GetDBProperty(t *testing.T) {
	db, cleanup := makeTestDb(t)
	defer cleanup()

	// read existing rocksdb property
	for _, m := range metrics.properties {
		value := getDBProperty(db, m.property)
		assert.False(t, math.IsNaN(value), "Property %v should not be NaN", m.property)
	}

	// read unknown rocksdb property
	value := getDBProperty(db, "rocksdb.unknown-property-name")
	assert.True(t, math.IsNaN(value), "Unknown property should be NaN")
}

func makeTestDb(t testing.TB) (*gorocksdb.DB, func()) {
	rc := loadRocksConfig(new(config.DiskView))
	bbto := rc.tableOptions()
	opts := rc.options()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	dir, err := ioutil.TempDir("", "test")
	require.NoError(t, err)
	db, err := gorocksdb.OpenDb(opts, dir)
	require.NoError(t, err)
	return db, func() {
		db.Close()
		os.RemoveAll(dir)
	}
}
