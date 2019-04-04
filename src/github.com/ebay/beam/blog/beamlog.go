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

// Package blog contains interfaces to Beam's data log.
package blog

import (
	"context"
	"fmt"

	"github.com/ebay/beam/config"
	"github.com/ebay/beam/discovery"
	"github.com/ebay/beam/discovery/discoveryfactory"
	"github.com/ebay/beam/logspec"
)

// An Index identifies entries in the log. It starts at 1.
type Index = uint64

// An Entry is an atomic unit in the log.
type Entry = logspec.Entry

// Info describes log metadata.
type Info = logspec.InfoReply_OK

// A BeamLog is a client to Beam's data log.
type BeamLog interface {
	// Append adds entries to the log. The data items might end up out of order in
	// the log and at non-consecutive indexes. If all the entries are appended
	// successfully, returns the corresponding indexes (in a slice the same length
	// as 'data') and nil. Otherwise, returns either a context error or a
	// ClosedError.
	Append(ctx context.Context, data [][]byte) ([]Index, error)

	// AppendSingle adds only a single entry to the log. If the entry is appended
	// successfully, returns its index and nil. Otherwise, returns either a context
	// error or a ClosedError.
	AppendSingle(ctx context.Context, data []byte) (Index, error)

	// Discard truncates the prefix of the log. After this is invoked, every entry
	// up to and excluding firstIndex is subject to deletion. If the server commits
	// to discarding the prefix, returns nil. Otherwise, returns either a context
	// error or a ClosedError.
	Discard(ctx context.Context, firstIndex Index) error

	// Read gets a suffix of the log and follows/tails the end of the log. nextIndex
	// is the index of the first entry to send on entriesCh. Read continues until it
	// encounters an error. Then it closes entriesCh and returns either a context
	// error, a TruncatedError, or a ClosedError.
	Read(ctx context.Context, nextIndex Index, entriesCh chan<- []Entry) error

	// Info fetches log metadata. Upon success, it returns the metadata and nil.
	// Otherwise, it returns either a context error or a ClosedError.
	Info(ctx context.Context) (*Info, error)

	// InfoStream subscribes to updates to log metadata. InfoStream continues until
	// it encounters an error. Then it closes infoCh and returns either a context
	// error or a ClosedError.
	InfoStream(ctx context.Context, infoCh chan<- *Info) error
}

// Disconnector is an optional interface that some BeamLog implementations provide.
type Disconnector interface {
	// Disconnect closes the connection to the current server, without blocking. The
	// BeamLog instance remains usable; it will automatically re-connect as needed.
	//
	// This is used by the logping package to measure the latency of different log
	// servers.
	Disconnect()
}

// A TruncatedError is returned from Read when attempting to fetch deleted log entries.
type TruncatedError struct {
	// The needed index that no longer exists.
	Requested Index
}

// Error implements the method defined by 'error'.
func (err TruncatedError) Error() string {
	return fmt.Sprintf("Beam log truncated before requested index (%v)",
		err.Requested)
}

// IsTruncatedError returns true if err has type TruncatedError, false otherwise.
func IsTruncatedError(err error) bool {
	_, ok := err.(TruncatedError)
	return ok
}

// A ClosedError is returned when attempting to use a BeamLog after the log's
// background context has been canceled. This error indicates the BeamLog
// instance is no longer useful and the process is likely shutting down.
type ClosedError struct{}

// Error implements the method defined by 'error'.
func (err ClosedError) Error() string {
	return "BeamLog client has been closed (client shutdown)"
}

// IsClosedError returns true if err has type ClosedError, false otherwise.
func IsClosedError(err error) bool {
	_, ok := err.(ClosedError)
	return ok
}

// Factories is a registry of named BeamLog constructors.
var Factories = make(map[string]Factory)

// A Factory creates a BeamLog instance. The only errors returned are fatal
// errors in the configuration.
//
// A factory returns immediately. The given context is used for all background
// activity. Canceling it will close any connections to the log servers, abort
// current requests, and prevent future requests from opening new connections.
//
// Note, however, that the Kafka factory is not well-behaved at this time.
type Factory = func(ctx context.Context, cfg *config.Beam, servers discovery.Locator) (BeamLog, error)

// New returns a new BeamLog instance. It assumes the factory corresponding to
// cfg.BeamLog (default "kafka") has been registered in the Factories map. It
// takes the same arguments as Factory.
//
// It assumes that cfg.BrokerList is set to a list of host:port pairs.
func New(ctx context.Context, cfg *config.Beam) (BeamLog, error) {
	factory, ok := Factories[cfg.BeamLog.Type]
	if !ok {
		return nil, fmt.Errorf("no BeamLog %v factory found", cfg.BeamLog.Type)
	}
	locator, err := discoveryfactory.NewLocator(ctx, &cfg.BeamLog.Locator)
	if err != nil {
		return nil, err
	}
	return factory(ctx, cfg, locator)
}
