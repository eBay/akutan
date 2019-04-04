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

// Package database defines an abstract ordered Key/Value store that can be used
// as a backing store by the Disk View. Implementations should register themselves
// with the Register method.
package database

import (
	"errors"
)

// ErrKeyNotFound is returned when a Read is attempted for a key that doesn't exist
var ErrKeyNotFound = errors.New("key not found")

// DB represents a local, lazily persistent, ordered key-value store. It is
// thread-safe, except where otherwise noted.
type DB interface {
	Close() error
	// Read returns the stored value for the given key, or ErrKeyNotFound or unrecoverable errors.
	Read(key []byte) ([]byte, error)
	// Write sets the value for a key.
	Write(key []byte, value []byte) error
	// Writes sets the values for a set of keys.
	Writes(writes []KV) error
	// BulkWrite returns an object used for writing large numbers of keys non-atomically.
	BulkWrite() BulkWriter
	// Snapshot returns an object used for reading keys atomically.
	Snapshot() Snapshot
	// Trigger a manual compaction of storage
	Compact()
}

// KV contains a single Key & Value
type KV struct {
	Key   []byte
	Value []byte
}

// BulkWriter is used for writing and removing large numbers of keys non-atomically. It's not thread-safe.  Order of operation
// on the writer is respected, so that keys written then removed will be removed and vice versa.
type BulkWriter interface {
	// Buffer will cause key to be set to value sometime before Close returns.
	Buffer(key []byte, value []byte) error
	// Remove will cause key to be deleted sometime before Close returns.
	Remove(key []byte) error
	// Close must be called to finish writing and clean up resources. It may be
	// called multiple times.
	Close() error
}

// Snapshot represents a snapshot of the Db for reading values.
// Its Not thread-safe. However, it is safe to call Fetch* from the Enumerate callback.
type Snapshot interface {
	// Close must be called to clean up resources.
	Close() error

	// Read returns the stored value for the given key, or ErrKeyNotFound or unrecoverable errors.
	Read(key []byte) (value []byte, err error)

	// ReadLast finds the largest key k where low <= k <= high. Returns k and its
	// value, or ErrKeyNotFound or unrecoverable errors.
	ReadLast(low, high []byte) (key []byte, value []byte, err error)

	// Enumerate calls emit with every key-value pair in order
	// for all keys from startKey (inclusive) up to endKey (exclusive).
	// startKey may be nil to start at the beginning.
	// The emit function should normally return nil to continue enumerating, or it
	// can return any error to stop immediately. Except for Halt, such errors will
	// be returned from Enumerate. If emit returns Halt, Enumerate will stop
	// immediately and return nil.
	Enumerate(startKey, endKey []byte, emit func(key []byte, value []byte) error) error

	// EnumerateKeys calls emit with every key in order
	// for all keys from startKey (inclusive) up to endKey (exclusive).
	// The emit function should normally return nil to continue enumerating, or it
	// can return any error to stop immediately. Except for Halt, such errors will
	// be returned from Enumerate. If emit returns Halt, Enumerate will stop
	// immediately and return nil.
	EnumerateKeys(startKey, endKey []byte, emit func(key []byte) error) error
}

// ErrHalt may be returned by an Enumerate* callback to stop enumeration but not
// return an error.
var ErrHalt = errors.New("database: no need to continue enumerating")
