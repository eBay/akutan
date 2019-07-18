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

// Package mockblog contains an in-process, in-memory implementation of a Akutan
// log client and server. This is a high-level mock intended for unit testing.
package mockblog

import (
	"context"
	"sync"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/logentry/logencoder"
	"github.com/ebay/akutan/logspec"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/cmp"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// Log is an in-process, in-memory implementation of blog.AkutanLog. It logically
// includes both the client and the server.
type Log struct {
	// A background context that is only closed when the log shuts down.
	ctx context.Context
	// lock protects the fields in locked.
	lock   sync.Mutex
	locked struct {
		// The first index in the log, which is possibly not yet created. Starts
		// at 1 and grows monotonically.
		startIndex blog.Index
		// entries[i] is the log entry with index startIndex+i. Only the slice
		// header is protected by the lock. The reachable underlying array of
		// pointers is immutable, as is each logspec.Entry value.
		entries []blog.Entry
		// changed is closed and re-initialized every time 'startIndex' or 'entries'
		// changes. It is never nil.
		changed chan struct{}
		// If not nil, the next call to Append will not append anything, return
		// this error, and set this to nil.
		nextAppendError error
	}
}

// Log implements blog.AkutanLog.
var _ blog.AkutanLog = (*Log)(nil)

// New constructs a Log. ctx is a background context that can be closed to shut
// down readers.
func New(ctx context.Context) *Log {
	log := &Log{
		ctx: ctx,
	}
	log.locked.startIndex = 1
	log.locked.changed = make(chan struct{})
	return log
}

// AppendSingle implements the method declared in blog.AkutanLog.
func (log *Log) AppendSingle(ctx context.Context, data []byte) (blog.Index, error) {
	indexes, err := log.Append(ctx, [][]byte{data})
	if err != nil {
		return 0, err
	}
	return indexes[0], nil
}

// Append implements the method declared in blog.AkutanLog.
func (log *Log) Append(ctx context.Context, proposals [][]byte) ([]blog.Index, error) {
	log.lock.Lock()
	defer log.lock.Unlock()
	err := log.locked.nextAppendError
	if err != nil {
		log.locked.nextAppendError = nil
		return nil, err
	}
	prevLastIndex := log.lastIndexLocked()
	nextIndex := prevLastIndex + 1
	indexes := make([]blog.Index, len(proposals))
	for i := range proposals {
		indexes[i] = nextIndex
		nextIndex++
		log.locked.entries = append(log.locked.entries, blog.Entry{
			Index: indexes[i],
			Data:  proposals[i],
			Skip:  false,
		})
	}
	logrus.WithFields(logrus.Fields{
		"count":         len(proposals),
		"startIndex":    log.locked.startIndex,
		"prevLastIndex": prevLastIndex,
		"newLastIndex":  log.lastIndexLocked(),
	}).Debug("Appended new entries")
	close(log.locked.changed)
	log.locked.changed = make(chan struct{})
	return indexes, nil
}

// lastIndexLocked returns the index of the last log entry. For an empty log, it
// returns one index before the log's start index. It must be called with the
// lock held.
func (log *Log) lastIndexLocked() blog.Index {
	return log.locked.startIndex + uint64(len(log.locked.entries)) - 1
}

// Read implements the method declared in blog.AkutanLog.
func (log *Log) Read(ctx context.Context, nextIndex blog.Index, entriesCh chan<- []blog.Entry) error {
	defer close(entriesCh)
	for {
		log.lock.Lock()
		startIndex := log.locked.startIndex
		lastIndex := log.lastIndexLocked()
		entries := log.locked.entries
		changedCh := log.locked.changed
		log.lock.Unlock()

		// Asking for discarded entries: reject.
		if nextIndex < startIndex {
			logrus.WithFields(logrus.Fields{
				"need":  nextIndex,
				"start": startIndex,
			}).Warn("Attempted to read discarded entries")
			return blog.TruncatedError{Requested: nextIndex}
		}

		// Reached the end of the log: wait.
		if nextIndex > lastIndex {
			select {
			case <-ctx.Done():
				logrus.Info("Read client disconnected")
				return ctx.Err()
			case <-log.ctx.Done():
				return blog.ClosedError{}
			case <-changedCh:
				continue
			}
		}

		// Send the entries.
		entries = entries[nextIndex-startIndex:]
		if len(entries) > 10 {
			entries = entries[:10]
		}
		select {
		case <-ctx.Done():
			logrus.Info("Read client disconnected")
			return ctx.Err()
		case <-log.ctx.Done():
			return blog.ClosedError{}
		case entriesCh <- entries:
		}
		nextIndex += uint64(len(entries))
	}
}

// Discard implements the method declared in blog.AkutanLog.
func (log *Log) Discard(ctx context.Context, firstIndex blog.Index) error {
	log.lock.Lock()
	defer log.lock.Unlock()
	if firstIndex <= log.locked.startIndex {
		return nil
	}
	diff := firstIndex - log.locked.startIndex
	if uint64(len(log.locked.entries)) < diff {
		log.locked.entries = nil
	} else {
		log.locked.entries = append([]logspec.Entry(nil),
			log.locked.entries[diff:]...)
	}
	logrus.WithFields(logrus.Fields{
		"prevStartIndex": log.locked.startIndex,
		"newStartIndex":  firstIndex,
		"lastIndex":      log.lastIndexLocked(),
		"retained":       len(log.locked.entries),
		"discarded":      firstIndex - log.locked.startIndex,
	}).Warn("Discarded entries")
	log.locked.startIndex = firstIndex
	close(log.locked.changed)
	log.locked.changed = make(chan struct{})
	return nil
}

// Info implements the method declared in blog.AkutanLog.
func (log *Log) Info(ctx context.Context) (*blog.Info, error) {
	info, _ := log.info()
	return info, nil
}

// info returns statistics about the log, and a channel that will be closed when
// the log changes. It must be called without holding the lock.
func (log *Log) info() (*blog.Info, chan struct{}) {
	log.lock.Lock()
	defer log.lock.Unlock()
	return &logspec.InfoReply_OK{
		FirstIndex: log.locked.startIndex,
		LastIndex:  log.lastIndexLocked(),
		BytesUsed:  uint64(len(log.locked.entries) * 10),
		BytesTotal: uint64(len(log.locked.entries) * 20),
	}, log.locked.changed
}

// InfoStream implements the method declared in blog.AkutanLog.
func (log *Log) InfoStream(ctx context.Context, infoCh chan<- *blog.Info) error {
	defer close(infoCh)
	for {
		info, changedCh := log.info()
		select {
		case <-ctx.Done():
			logrus.Info("InfoStream client disconnected")
			return ctx.Err()
		case <-log.ctx.Done():
			return blog.ClosedError{}
		case infoCh <- info:
		}
		select {
		case <-ctx.Done():
			logrus.Info("InfoStream client disconnected")
			return ctx.Err()
		case <-log.ctx.Done():
			return blog.ClosedError{}
		case <-changedCh:
		}
	}
}

// SetNextAppendError causes the next call to AppendSingle or Append to return
// the given error, without appending anything. Only the next call is affected;
// subsequent appends will proceed as normal.
func (log *Log) SetNextAppendError(err error) {
	log.lock.Lock()
	log.locked.nextAppendError = err
	log.lock.Unlock()
}

// Entries returns all of the entries presently in the log, starting with
// the given index.
func (log *Log) Entries(start blog.Index) ([]blog.Entry, error) {
	log.lock.Lock()
	entries := log.locked.entries
	currentStart := log.locked.startIndex
	lastIndex := log.lastIndexLocked()
	log.lock.Unlock()
	if start < currentStart {
		return nil, blog.TruncatedError{Requested: start}
	}
	if start > lastIndex {
		return nil, nil
	}
	return entries[start-currentStart:], nil
}

// DecodedEntries returns all of the entries presently in the log, starting with
// the given position, already decoded into Akutan log commands.
func (log *Log) DecodedEntries(start rpc.LogPosition,
) (decoded []logencoder.Entry, next rpc.LogPosition, err error) {
	entries, err := log.Entries(start.Index)
	if err != nil {
		return nil, start, err
	}
	decoded = make([]logencoder.Entry, 0, len(entries))
	next = start
	for i := range entries {
		var dec logencoder.Entry
		dec, next, err = logencoder.Decode(next.Version, entries[i])
		if err != nil {
			return nil, start, err
		}
		decoded = append(decoded, dec)
	}
	return decoded, next, nil
}

// AssertCommands causes the given test to fail if the entries in the log from
// the given starting position to not match the given commands. It returns true
// if the commands matched, false otherwise.
func (log *Log) AssertCommands(t assert.TestingT,
	start rpc.LogPosition, cmds []logencoder.ProtobufCommand) bool {
	entries, _, err := log.DecodedEntries(start)
	if !assert.NoError(t, err, "error decoding log entries") {
		return false
	}
	allOk := assert.Equal(t, len(cmds), len(entries),
		"number of log entries")
	for i := 0; i < cmp.MinInt(len(cmds), len(entries)); i++ {
		if !assert.Equal(t, cmds[i], entries[i].Command,
			"log entry at index %v", entries[i].Position.Index) {
			allOk = false
		}
	}
	return allOk
}
