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

package mockblog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/logentry"
	"github.com/ebay/akutan/logentry/logencoder"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/parallel"
	"github.com/stretchr/testify/assert"
)

func Test_Append(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	log := New(ctx)

	indexes, err := log.Append(ctx, nil)
	assert.NoError(err)
	assert.Equal([]uint64{}, indexes)
	assert.Equal(uint64(1), log.locked.startIndex)
	assert.Equal(uint64(0), log.lastIndexLocked())

	index, err := log.AppendSingle(ctx, []byte("yo"))
	assert.NoError(err)
	assert.Equal(uint64(1), index)
	assert.Equal(uint64(1), log.locked.startIndex)
	assert.Equal(uint64(1), log.lastIndexLocked())

	log.SetNextAppendError(errors.New("ants in pants"))
	index, err = log.AppendSingle(ctx, []byte("feeling itchy"))
	assert.EqualError(err, "ants in pants")
	assert.Equal(uint64(0), index)
	assert.Equal(uint64(1), log.lastIndexLocked())

	indexes, err = log.Append(ctx, [][]byte{[]byte("hello"), []byte("world")})
	assert.NoError(err)
	assert.Equal([]uint64{2, 3}, indexes)
	assert.Equal(uint64(1), log.locked.startIndex)
	assert.Equal(uint64(3), log.lastIndexLocked())

	entries, err := log.Entries(1)
	assert.NoError(err)
	assert.Equal([]blog.Entry{
		{Index: 1, Data: []byte("yo")},
		{Index: 2, Data: []byte("hello")},
		{Index: 3, Data: []byte("world")},
	}, entries)
}

func Test_Read(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	log := New(ctx)

	_, err := log.Append(ctx, [][]byte{
		[]byte("one"), []byte("two"), []byte("three"), []byte("four"),
		[]byte("five"), []byte("six"), []byte("seven"), []byte("eight"),
		[]byte("nine"), []byte("ten"), []byte("eleven"), []byte("twelve"),
		[]byte("thirteen"), []byte("fourteen"), []byte("fifteen"), []byte("sixteen"),
	})
	assert.NoError(err)

	read := func(start blog.Index, limit int) ([]blog.Entry, error) {
		entriesCh := make(chan []blog.Entry)
		entries := make([]blog.Entry, 0, limit)
		ctx, cancel := context.WithCancel(ctx)
		canceled := false
		wait := parallel.Go(func() {
			for chunk := range entriesCh {
				for _, entry := range chunk {
					entries = append(entries, entry)
					if len(entries) == limit {
						canceled = true
						cancel()
						return
					}
				}
			}
		})
		err := log.Read(ctx, start, entriesCh)
		wait()
		if err != nil {
			if err == context.Canceled && canceled {
				return entries, nil
			}
			return nil, err
		}
		return nil, errors.New("log.Read quit unexpectedly")
	}

	entries, err := log.Entries(1)
	assert.NoError(err)
	if assert.Len(entries, 16) {
		assert.Equal(uint64(1), entries[0].Index)
		assert.Equal([]byte("one"), entries[0].Data)
		assert.Equal(uint64(16), entries[15].Index)
		assert.Equal([]byte("sixteen"), entries[15].Data)
	}
	readEntries, err := read(1, 16)
	assert.NoError(err)
	assert.Equal(entries, readEntries)

	entries, err = log.Entries(9)
	assert.NoError(err)
	if assert.Len(entries, 8) {
		assert.Equal(uint64(9), entries[0].Index)
		assert.Equal([]byte("nine"), entries[0].Data)
		assert.Equal(uint64(16), entries[7].Index)
		assert.Equal([]byte("sixteen"), entries[7].Data)
	}
	readEntries, err = read(9, 8)
	assert.NoError(err)
	assert.Equal(entries, readEntries)
}

func Test_Read_pastEnd(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	log := New(ctx)

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()
	entriesCh := make(chan []blog.Entry, 4)
	err := log.Read(ctx, 1, entriesCh)
	assert.Equal(context.DeadlineExceeded, err)
	assert.Empty(entriesCh)

	entries, err := log.Entries(1)
	assert.NoError(err)
	assert.Empty(entries)
	entries, err = log.Entries(2)
	assert.NoError(err)
	assert.Empty(entries)
}

func Test_Read_discarded(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	log := New(ctx)
	err := log.Discard(ctx, 11)
	assert.NoError(err)
	entriesCh := make(chan []blog.Entry, 4)
	err = log.Read(ctx, 10, entriesCh)
	assert.Error(&blog.TruncatedError{Requested: 10}, err)
	_, err = log.Entries(1)
	assert.Error(&blog.TruncatedError{Requested: 10}, err)
}

func Test_Discard(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	log := New(ctx)

	err := log.Discard(ctx, 11)
	assert.NoError(err)
	err = log.Discard(ctx, 8)
	assert.NoError(err)
	assert.Equal(uint64(11), log.locked.startIndex)
	assert.Equal(uint64(10), log.lastIndexLocked())

	_, err = log.Append(ctx, [][]byte{[]byte("hello"), []byte("world")})
	assert.NoError(err)
	assert.Equal(uint64(11), log.locked.startIndex)
	assert.Equal(uint64(12), log.lastIndexLocked())
	assert.Equal("hello", string(log.locked.entries[0].Data))

	err = log.Discard(ctx, 12)
	assert.NoError(err)
	assert.Equal(uint64(12), log.locked.startIndex)
	assert.Equal(uint64(12), log.lastIndexLocked())
	assert.Equal("world", string(log.locked.entries[0].Data))

	err = log.Discard(ctx, 13)
	assert.NoError(err)
	assert.Equal(uint64(13), log.locked.startIndex)
	assert.Equal(uint64(12), log.lastIndexLocked())

	err = log.Discard(ctx, 500)
	assert.NoError(err)
	assert.Equal(uint64(500), log.locked.startIndex)
	assert.Equal(uint64(499), log.lastIndexLocked())
}

func Test_Info(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	log := New(ctx)

	err := log.Discard(ctx, 20)
	assert.NoError(err)
	_, err = log.Append(ctx, [][]byte{[]byte("hello"), []byte("world")})
	assert.NoError(err)
	res, err := log.Info(ctx)
	assert.NoError(err)
	assert.Equal(uint64(20), res.FirstIndex)
	assert.Equal(uint64(21), res.LastIndex)
	assert.Equal(uint64(20), res.BytesUsed)
	assert.Equal(uint64(40), res.BytesTotal)
}

func Test_InfoStream(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	log := New(ctx)

	infoCh := make(chan *blog.Info)
	infoCtx, cancel := context.WithCancel(ctx)
	wait := parallel.GoCaptureError(func() error {
		return log.InfoStream(infoCtx, infoCh)
	})
	info := <-infoCh
	assert.Equal(uint64(1), info.FirstIndex)
	assert.Equal(uint64(0), info.LastIndex)

	_, err := log.AppendSingle(ctx, []byte("hello"))
	assert.NoError(err)
	info = <-infoCh
	assert.Equal(uint64(1), info.LastIndex)

	_, err = log.AppendSingle(ctx, []byte("world"))
	assert.NoError(err)
	info = <-infoCh
	assert.Equal(uint64(2), info.LastIndex)

	cancel()
	assert.Equal(wait(), context.Canceled)
}

// SetNextAppendError is tested in the Append tests.

// Entries is tested in the Read tests.

func Test_DecodedEntries(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	log := New(ctx)

	cmd1 := &logentry.VersionCommand{
		// TODO: Once logdecoder supports additional versions, increment this
		// and test that logic.
		MoveToVersion: logencoder.DefaultLogVersion,
	}
	cmd2 := &logentry.PingCommand{
		Writer: []byte("unit test"),
	}
	_, err := log.Append(ctx, [][]byte{
		logencoder.Encode(cmd1),
		logencoder.Encode(cmd2),
	})
	assert.NoError(err)

	startPos := logencoder.LogInitialPosition()
	entries, nextPos, err := log.DecodedEntries(startPos)
	assert.NoError(err)
	assert.Equal([]logencoder.Entry{
		{
			Position: rpc.LogPosition{Version: 1, Index: 1},
			Command:  cmd1,
		},
		{
			Position: rpc.LogPosition{Version: 1, Index: 2},
			Command:  cmd2,
		},
	}, entries)
	assert.Equal(rpc.LogPosition{Version: 1, Index: 3}, nextPos)
	ok := log.AssertCommands(t, startPos,
		[]logencoder.ProtobufCommand{cmd1, cmd2})
	assert.True(ok)

	startPos = rpc.LogPosition{Version: logencoder.DefaultLogVersion, Index: 2}
	entries, nextPos, err = log.DecodedEntries(startPos)
	assert.NoError(err)
	assert.Equal([]logencoder.Entry{
		{
			Position: rpc.LogPosition{Version: 1, Index: 2},
			Command:  cmd2,
		},
	}, entries)
	assert.Equal(rpc.LogPosition{Version: 1, Index: 3}, nextPos)
	ok = log.AssertCommands(t, startPos,
		[]logencoder.ProtobufCommand{cmd2})
	assert.True(ok)

	captureAssert := new(mockAssert)
	ok = log.AssertCommands(captureAssert, startPos,
		[]logencoder.ProtobufCommand{})
	assert.False(ok)
	assert.Contains(captureAssert.String(), "number of log entries")
}

// AssertCommands is tested with DecodedEntries

// mockAssert implements assert.TestingT.
type mockAssert struct {
	strings.Builder
}

func (t *mockAssert) Errorf(format string, args ...interface{}) {
	fmt.Fprintf(t, format, args...)
	t.WriteByte('\n')
}
