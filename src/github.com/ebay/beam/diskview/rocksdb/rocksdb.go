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

// +build cgo

package rocksdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/ebay/beam/config"
	"github.com/ebay/beam/diskview/database"
	pbytes "github.com/ebay/beam/util/bytes"
	"github.com/ebay/beam/util/clocks"
	log "github.com/sirupsen/logrus"
	"github.com/tecbot/gorocksdb"
)

// RocksDB provides an implementation of the database.DB interface. Its is backed
// by an embeded rocksDB key/value store.
type RocksDB struct {
	db   *gorocksdb.DB
	ropt *gorocksdb.ReadOptions
	wopt *gorocksdb.WriteOptions

	// Channel to send stop signal to the metrics collector goroutine when
	// closing the db.
	metricsCollectorStopCh chan struct{}
}

func init() {
	database.Register(New, "rocks", "rocksdb")
}

// RocksConfig contains all the settings that are configurable. These are used
// to configure the underlying RocksDB instance.
type RocksConfig struct {
	// BlockBasedTableOptions
	BlockSizeKB                int
	CacheIndexAndFilterBlocks  bool
	PinL0Cache                 bool
	BloomFilterBitsPerKey      int
	BlockCacheSizeMB           int
	CompressedBlockCacheSizeMB int

	// Options
	IncreaseParallelism          *int // explict 0 will use NumCPU
	AllowConcurrentMemtableWrite bool
	BytesPerSyncKB               uint64
	Compression                  string // "none", "snappy", "zlib", "bz2", "lz4", "lz4hc" [uses "none" if not set]
	AdviseRandomOnOpen           bool
	AllowMmapReads               bool
	UseDirectReads               bool
	UseAdaptiveMutex             bool

	WriteBufferSizeMB        int
	MaxWriteBuffers          int
	MinWriteBuffersToMerge   int
	TargetFileSizeBaseKB     uint64
	TargetFileSizeMultiplier int

	// Stats Logging
	StatsIntervalMinutes float32
	// Metrics collection interval. Can't use time.Duration type as JSON
	// decoding in Go cannot populate a time.Duration and require extra code
	// effort.
	MetricsIntervalMinutes float32

	// Write Options
	DisableWAL bool
}

func (r *RocksConfig) statsInterval() time.Duration {
	if r.StatsIntervalMinutes == 0 {
		return time.Hour
	}
	return time.Duration(r.StatsIntervalMinutes * float32(time.Minute))
}

func (r *RocksConfig) metricsInterval() time.Duration {
	if r.MetricsIntervalMinutes == 0 {
		return 30 * time.Second
	}
	return time.Duration(r.MetricsIntervalMinutes * float32(time.Minute))
}

func (r *RocksConfig) tableOptions() *gorocksdb.BlockBasedTableOptions {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	if r.BlockSizeKB > 0 {
		bbto.SetBlockSize(r.BlockSizeKB * 1024)
	}
	bbto.SetCacheIndexAndFilterBlocks(r.CacheIndexAndFilterBlocks)
	bbto.SetPinL0FilterAndIndexBlocksInCache(r.PinL0Cache)
	if r.BloomFilterBitsPerKey > 0 {
		bbto.SetFilterPolicy(gorocksdb.NewBloomFilter(r.BloomFilterBitsPerKey))
	}
	if r.BlockCacheSizeMB > 0 {
		bbto.SetBlockCache(gorocksdb.NewLRUCache(r.BlockCacheSizeMB * 1024 * 1024))
	}
	if r.CompressedBlockCacheSizeMB > 0 {
		bbto.SetBlockCacheCompressed(gorocksdb.NewLRUCache(r.CompressedBlockCacheSizeMB * 1024 * 1024))
	}
	return bbto
}

func (r *RocksConfig) options() *gorocksdb.Options {
	opts := gorocksdb.NewDefaultOptions()
	if r.IncreaseParallelism != nil {
		if *r.IncreaseParallelism == 0 {
			opts.IncreaseParallelism(runtime.NumCPU())
		} else {
			opts.IncreaseParallelism(*r.IncreaseParallelism)
		}
	}
	opts.SetAllowConcurrentMemtableWrites(r.AllowConcurrentMemtableWrite)
	opts.SetAdviseRandomOnOpen(r.AdviseRandomOnOpen)
	if r.BytesPerSyncKB > 0 {
		opts.SetBytesPerSync(r.BytesPerSyncKB * 1024)
	}
	if r.WriteBufferSizeMB > 0 {
		opts.SetWriteBufferSize(r.WriteBufferSizeMB * 1024 * 1024)
	}
	if r.MaxWriteBuffers > 0 {
		opts.SetMaxWriteBufferNumber(r.MaxWriteBuffers)
	}
	if r.MinWriteBuffersToMerge > 0 {
		opts.SetMinWriteBufferNumberToMerge(r.MinWriteBuffersToMerge)
	}
	if r.TargetFileSizeBaseKB > 0 {
		opts.SetTargetFileSizeBase(r.TargetFileSizeBaseKB * 1024)
	}
	if r.TargetFileSizeMultiplier > 0 {
		opts.SetTargetFileSizeMultiplier(r.TargetFileSizeMultiplier)
	}
	if r.Compression == "" {
		opts.SetCompression(gorocksdb.NoCompression)
	} else {
		opts.SetCompression(parseCompressionType(r.Compression))
	}
	opts.SetAllowMmapReads(r.AllowMmapReads)
	opts.SetUseDirectReads(r.UseDirectReads)
	opts.SetUseAdaptiveMutex(r.UseAdaptiveMutex)
	return opts
}

func parseCompressionType(v string) gorocksdb.CompressionType {
	switch strings.ToLower(v) {
	case "none":
		return gorocksdb.NoCompression
	case "snappy":
		return gorocksdb.SnappyCompression
	case "zlib":
		return gorocksdb.ZLibCompression
	case "bz2":
		return gorocksdb.Bz2Compression
	case "lz4":
		return gorocksdb.LZ4Compression
	case "lz4hc":
		return gorocksdb.LZ4HCCompression
	default:
		log.Fatalf("Unknown compression setting of %v, use one of none, snappy, zlib, bz2, lz4, lz4hc", v)
	}
	return gorocksdb.SnappyCompression
}

// loadRocksConfig will look for a /tmp/rocks.cfg file if it exists its used
// otherwise defaults from code are used
func loadRocksConfig(clusterCfg *config.DiskView) *RocksConfig {
	cb, err := ioutil.ReadFile("/tmp/rocks.cfg")
	if err == nil {
		var rc RocksConfig
		if err := json.Unmarshal(cb, &rc); err == nil {
			if rc.Compression == "" {
				rc.Compression = clusterCfg.Compression
			}
			return &rc
		}
	}
	cfg := DefaultRocksConfig()
	cfg.Compression = clusterCfg.Compression
	return cfg
}

// DefaultRocksConfig returns a new RocksConfig instance configured with the recommened defaults.
func DefaultRocksConfig() *RocksConfig {
	return &RocksConfig{
		WriteBufferSizeMB:        192,
		MaxWriteBuffers:          4,
		BlockSizeKB:              32,
		BlockCacheSizeMB:         1024,
		BytesPerSyncKB:           8092,
		TargetFileSizeMultiplier: 2,
		AdviseRandomOnOpen:       false,
	}
}

// Open will open the database stored at the supplied directory, if one
// doesn't exist, a new empty database will be created.
func (r *RocksConfig) Open(dir string) (*gorocksdb.DB, error) {
	bbto := r.tableOptions()
	opts := r.options()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	return gorocksdb.OpenDb(opts, dir)
}

// New will create or open a database configured from the supplied FactoryArgs
func New(args database.FactoryArgs) (database.DB, error) {
	dir := args.Dir
	if dir == "" {
		dir = fmt.Sprintf("/tmp/rocksdb-p%s", args.Partition.HashRange())
	}
	return NewWithDir(dir, args.Config)
}

// NewWithDir will create or open a database in the supplied directory, taking
// configuration settings from the supplied config.DiskView
func NewWithDir(dir string, cfg *config.DiskView) (database.DB, error) {
	rc := loadRocksConfig(cfg)
	log.Infof("Our rocksConfig: %+v", rc)
	log.Infof("RocksDB dir %v", dir)
	db, err := rc.Open(dir)
	if err != nil {
		return nil, fmt.Errorf("opening RocksDB returned error: %v", err)
	}
	go logDBStats(db, rc.statsInterval())
	rdb := RocksDB{
		db:                     db,
		ropt:                   gorocksdb.NewDefaultReadOptions(),
		wopt:                   gorocksdb.NewDefaultWriteOptions(),
		metricsCollectorStopCh: make(chan struct{}),
	}
	go collectMetrics(db, rc.metricsInterval(), rdb.metricsCollectorStopCh)
	rdb.wopt.DisableWAL(rc.DisableWAL)
	return &rdb, nil
}

func logDBStats(db *gorocksdb.DB, interval time.Duration) {
	for {
		time.Sleep(interval)
		stats := db.GetProperty("rocksdb.stats")
		fmt.Printf("RocksDB Stats\n%s\n", stats)
	}
}

// collectMetrics collects rocksdb internal property values and writes to
// Prometheus metrics periodically. It stops when a message is received from the
// abortCh or the channel is closed.
func collectMetrics(db *gorocksdb.DB, interval time.Duration, abortCh chan struct{}) {
	alarm := clocks.Wall.NewAlarm()
	defer alarm.Stop()
	for {
		start := clocks.Wall.Now()
		for _, m := range metrics.properties {
			m.gauge.Set(getDBProperty(db, m.property))
		}
		end := clocks.Wall.Now()
		metrics.propertiesCollectionDurationSeconds.Observe(end.Sub(start).Seconds())

		alarm.Set(end.Add(interval))
		select {
		case <-abortCh:
			return
		case <-alarm.WaitCh(): // wait for alarm to trigger
		}
	}
}

// getDBProperty reads the rocksdb property and returns the value as float64.
func getDBProperty(db *gorocksdb.DB, property string) float64 {
	s := db.GetProperty(property)
	value, err := strconv.ParseFloat(s, 64)
	if err != nil {
		value = math.NaN()
	}
	return value
}

// Close will close the database, freeing up any memory used by the underlying rocks cache
func (db *RocksDB) Close() error {
	// Before closing the db, send stop signal to the metrics collector
	// goroutine and wait for its termination, to avoid the goroutine read the
	// property from the closed db which causes panic.
	db.metricsCollectorStopCh <- struct{}{}
	db.wopt.Destroy()
	db.ropt.Destroy()
	db.db.Close()
	return nil
}

// Read returns the value currently stored for the provided key, if the key doesn't exist
// nil is returned [is this right?]
func (db *RocksDB) Read(key []byte) ([]byte, error) {
	value, err := db.db.GetBytes(db.ropt, key)
	// TODO: database.ErrKeyNotFound?
	if err != nil {
		return nil, err
	}
	return value, nil
}

// Write will update the database with the one provided key/value pair
func (db *RocksDB) Write(key []byte, value []byte) error {
	return db.db.Put(db.wopt, key, value)
}

// Writes will write a batch of key/values to the database, they are written
// in chunks, so this may not be an atomic operation depending on how large writes
// is
func (db *RocksDB) Writes(writes []database.KV) error {
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()
	for _, kv := range writes {
		wb.Put(kv.Key, kv.Value)
	}
	return db.db.Write(db.wopt, wb)
}

// Compact runs a manual compaction on the entire keyspace. This is
// not likely to be needed for typical usage. (unless you've done a
// significant number of writes)
func (db *RocksDB) Compact() {
	db.db.CompactRange(gorocksdb.Range{})
}

type bulkWriter struct {
	db *RocksDB
	wb *gorocksdb.WriteBatch
}

// BulkWrite returns a new BulkWriter which can be used to buffer up
// a series of Puts & Deletes, which are applied in chunks. You must
// call Close() to ensure the last chunk is flushed to the database
func (db *RocksDB) BulkWrite() database.BulkWriter {
	return &bulkWriter{
		db: db,
		wb: gorocksdb.NewWriteBatch(),
	}
}

const bulkWriteChunkCount = 1024

func (writer *bulkWriter) flushIfNeeded() error {
	if writer.wb.Count() >= bulkWriteChunkCount {
		return writer.Close()
	}
	return nil
}

// Buffer will buffer for write a single key/value pair, it may cause
// the current chunk to be flushed to the database
func (writer *bulkWriter) Buffer(key []byte, value []byte) error {
	if writer.wb == nil {
		writer.wb = gorocksdb.NewWriteBatch()
	}
	writer.wb.Put(pbytes.Copy(key), pbytes.Copy(value))
	return writer.flushIfNeeded()
}

// Close will flush any pending writes to the database, and clear the
// current buffer. The bulkWriter is still valid to continue to use
// after calling Close, and Close can be called multiple times
func (writer *bulkWriter) Close() error {
	if writer.wb == nil {
		return nil
	}
	err := writer.db.db.Write(writer.db.wopt, writer.wb)
	if err == nil {
		writer.wb.Destroy()
		writer.wb = nil
	}
	return err
}

// Remove will buffer a deletion of a single key, it may cause the
// current chunk to be flushed to the database
func (writer *bulkWriter) Remove(key []byte) error {
	if writer.wb == nil {
		writer.wb = gorocksdb.NewWriteBatch()
	}
	writer.wb.Delete(pbytes.Copy(key))
	return writer.flushIfNeeded()
}

type dbSnapshot struct {
	db   *RocksDB
	snap *gorocksdb.Snapshot
	ropt *gorocksdb.ReadOptions
	iter *gorocksdb.Iterator
}

// Snapshot returns a snapshot of the database at the current point in time
// it can be used to perform consistent iterations of the database. You
// must call Close() when you're finished with it to release any related
// resources.
func (db *RocksDB) Snapshot() database.Snapshot {
	snap := db.db.NewSnapshot()
	ropt := gorocksdb.NewDefaultReadOptions()
	ropt.SetSnapshot(snap)
	iter := db.db.NewIterator(ropt)
	return &dbSnapshot{
		db:   db,
		snap: snap,
		ropt: ropt,
		iter: iter,
	}
}

// Close will close the snapshot, freeing any memory or other resources
// that the snapshot used. The snapshot is not valid for further use
// after closing. You must Close() the snapshot when you're finished
// with it, otherwise it will contine to consume memory and potentially
// affect the efficient processing of new writes.
func (snap *dbSnapshot) Close() error {
	snap.iter.Close()
	snap.ropt.Destroy()
	snap.db.db.ReleaseSnapshot(snap.snap)
	return nil
}

// Read returns the value of the specified key, as of this snapshot
// it returns nil if they key doesn't exist
func (snap *dbSnapshot) Read(key []byte) ([]byte, error) {
	value, err := snap.db.db.GetBytes(snap.ropt, key)
	// TODO: database.ErrKeyNotFound?
	if err != nil {
		return nil, err
	}
	return value, nil
}

// ReadLast returns the key & value for the key that is the highest value
// but not over high, and larger than low. it returns database.ErrKeyNotFound
// in the event that no such key can be found
func (snap *dbSnapshot) ReadLast(low, high []byte) ([]byte, []byte, error) {
	snap.iter.SeekForPrev(high)
	if !snap.iter.Valid() {
		return nil, nil, database.ErrKeyNotFound
	}
	key := snap.iter.Key()
	if bytes.Compare(key.Data(), low) < 0 {
		return nil, nil, database.ErrKeyNotFound
	}
	value := pbytes.Copy(snap.iter.Value().Data())
	return pbytes.Copy(key.Data()), value, nil
}

func (snap *dbSnapshot) Enumerate(startKey, endKey []byte, emit func(key []byte, value []byte) error) error {
	snap.iter.Seek(startKey)
	for ; snap.iter.Valid(); snap.iter.Next() {
		k := snap.iter.Key().Data()
		if bytes.Compare(k, endKey) >= 0 {
			return nil
		}
		k = pbytes.Copy(k)
		v := pbytes.Copy(snap.iter.Value().Data())
		err := emit(k, v)
		if err != nil {
			if err == database.ErrHalt {
				return nil
			}
			return err
		}
	}
	return nil
}

func (snap *dbSnapshot) EnumerateKeys(startKey, endKey []byte, emit func(key []byte) error) error {
	return snap.Enumerate(startKey, endKey, func(k, _ []byte) error {
		return emit(k)
	})
}
