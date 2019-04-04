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
	"fmt"
	"testing"
	"time"

	"github.com/ebay/beam/diskview/database"
	"github.com/ebay/beam/diskview/keys"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/clocks"
	proto "github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

func Test_CountTracker_init(t *testing.T) {
	db, cleanup := makeTestDB(t)
	defer cleanup()
	keys, expStats := poStatsTestData()
	assert.NoError(t, db.Writes(keys))

	cc := new(carouselConductor)
	cc.init(db)
	store := mockStorage{
		t:       t,
		readCh:  make(chan struct{}, 1),
		writeCh: make(chan struct{}),
	}

	counter := countTracker{}
	defer counter.stop()
	clock := clocks.NewMock()
	counter.init(cc, clock, &store, time.Minute)
	// The goroutine started by init should call readKeyValue and then
	// writeKeyValue soon, wait for that to happen.
	<-store.readCh
	<-store.writeCh
	assert.Equal(t, expStats.GoString(), store.stats.GoString())
	assert.Equal(t, expStats, counter.current())

	// If there's no write and the interval time passes, nothing should happen.
	// This is checked later by looking at writeCount.
	clock.Advance(time.Minute + time.Second)

	// If there's a write and the interval time passes, it should calculate
	// stats again.
	counter.haveWritten()
	clock.Advance(time.Minute + time.Second)
	<-store.writeCh
	expStats.AsOf = clock.Now()
	assert.Equal(t, expStats.GoString(), store.stats.GoString())
	assert.Equal(t, 1, store.readCount)
	assert.Equal(t, 2, store.writeCount)
	assert.Equal(t, expStats, counter.current())

	// There's a write and the interval time has not yet passed. Calling update
	// should force stats to be recalcuated.
	counter.haveWritten()
	clock.Advance(time.Second)
	counter.update()
	<-store.writeCh
	expStats.AsOf = clock.Now()
	assert.Equal(t, expStats.GoString(), store.stats.GoString())
	assert.Equal(t, 1, store.readCount)
	assert.Equal(t, 3, store.writeCount)
	assert.Equal(t, expStats, counter.current())
}

type mockStorage struct {
	t          *testing.T
	stats      GraphMeta
	readCh     chan struct{}
	readCount  int
	writeCh    chan struct{}
	writeCount int
}

func (m *mockStorage) readKeyValue(keySpec keys.Spec, val proto.Unmarshaler) error {
	assert.Equal(m.t, keys.StatsKey{}, keySpec)
	d, err := m.stats.Marshal()
	assert.NoError(m.t, err)
	err = val.Unmarshal(d)
	m.readCount++
	m.readCh <- struct{}{}
	return err
}

func (m *mockStorage) writeKeyValue(keySpec keys.Spec, val proto.Marshaler) error {
	assert.Equal(m.t, keys.StatsKey{}, keySpec)
	m.stats = *val.(*GraphMeta)
	m.writeCount++
	m.writeCh <- struct{}{}
	return nil
}

func Test_countNow(t *testing.T) {
	test := func(t *testing.T, keys []database.KV, expected GraphMeta) {
		db, cleanup := makeTestDB(t)
		defer cleanup()

		assert.NoError(t, db.Writes(keys))
		counter := countTracker{
			clock:  clocks.NewMock(),
			stopCh: make(chan struct{}),
		}
		defer counter.stop()

		cc := new(carouselConductor)
		cc.init(db)
		stats, err := counter.countNow(cc)
		assert.NoError(t, err)
		assert.Equal(t, expected.GoString(), stats.GoString())
	}
	t.Run("po", func(t *testing.T) {
		keys, exp := poStatsTestData()
		test(t, keys, exp)
	})
	t.Run("sp", func(t *testing.T) {
		keys, exp := spStatsTestData()
		test(t, keys, exp)
	})
}

func poStatsTestData() ([]database.KV, GraphMeta) {
	keys := make([]database.KV, 0, 512)
	for i := 0; i < 100; i++ {
		b := uint64(i * 1000)
		keys = append(keys, []database.KV{
			{Key: poKey(b+1, 12+b, 13, "Bob", 100)},
			{Key: poKey(b+2, 12+b, 14, "Bob's House", 100)},
			{Key: poKey(b+3, 15+b, 13, "Alice", 100)},
			{Key: poKey(b+4, 15+b, 11, "Person", 100)},
			{Key: poKey(b+5, 12+b, 11, "Person", 100)},
		}...)
	}
	keys = append(keys, []database.KV{
		{Key: poKey(1, 12, 13, "Bob_101", 100)},
		{Key: poKey(2, 12, 13, "Bob_102", 100)},
		{Key: poKey(3, 16, 12, "Eve", 100)},
	}...)

	expStats := GraphMeta{
		AsOf:         clocks.NewMock().Now(),
		FactCount:    503,
		FactVersions: 503,
		Stats: rpc.FactStatsResult{
			Predicates: []rpc.PredicateStats{
				{Predicate: 11, Count: 200},
				{Predicate: 13, Count: 202},
			},
			PredicateObjects: []rpc.PredicateObjectStats{
				{Predicate: 11, Object: rpc.AString("Person", 0), Count: 200},
			},
			NumFacts:                 503,
			DistinctPredicates:       4,
			DistinctPredicateObjects: 7,
		},
	}
	return keys, expStats
}

func spStatsTestData() ([]database.KV, GraphMeta) {
	keys := make([]database.KV, 0, 512)
	for i := 0; i < 100; i++ {
		b := uint64(i * 1000)
		keys = append(keys, []database.KV{
			{Key: spKey(b+1, 12, 13, fmt.Sprintf("Bob_%d", i), 100)},
			{Key: spKey(b+2, 12, 14+b, "Bob's House", 100)},
			{Key: spKey(b+3, 15, 13+b, "Alice", 100)},
			{Key: spKey(b+4, 15, 11+b, "Person", 100)},
			{Key: spKey(b+5, 12, 11+b, "Person", 100)},
		}...)
	}
	keys = append(keys, []database.KV{
		{Key: spKey(1, 12, 13, "Bob_101", 100)},
		{Key: spKey(2, 12, 13, "Bob_102", 100)},
		{Key: spKey(3, 16, 12, "Eve", 100)},
	}...)

	exp := GraphMeta{
		AsOf:         clocks.NewMock().Now(),
		FactCount:    503,
		FactVersions: 503,
		Stats: rpc.FactStatsResult{
			Subjects: []rpc.SubjectStats{
				{Subject: 12, Count: 302},
				{Subject: 15, Count: 200},
			},
			SubjectPredicates: []rpc.SubjectPredicateStats{
				{Subject: 12, Predicate: 13, Count: 102},
			},
			NumFacts:                  503,
			DistinctSubjects:          3,
			DistinctSubjectPredicates: 402,
		},
	}
	return keys, exp
}
