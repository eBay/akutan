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

package txtimer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/logentry"
	"github.com/ebay/akutan/logentry/logencoder"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/clocks"
	"github.com/ebay/akutan/util/parallel"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// A AkutanLog that panics on all operations.
type panicLog struct{}

func (panicLog) Append(ctx context.Context, data [][]byte) ([]blog.Index, error) {
	panic("panicLog.Append: not implemented")
}
func (panicLog) AppendSingle(ctx context.Context, data []byte) (blog.Index, error) {
	panic("panicLog.AppendSingle: not implemented")
}
func (panicLog) Discard(ctx context.Context, firstIndex blog.Index) error {
	panic("panicLog.Discard: not implemented")
}
func (panicLog) Read(ctx context.Context, nextIndex blog.Index, entriesCh chan<- []blog.Entry) error {
	panic("panicLog.Read: not implemented")
}
func (panicLog) Info(ctx context.Context) (*blog.Info, error) {
	panic("panicLog.Info: not implemented")
}
func (panicLog) InfoStream(ctx context.Context, infoCh chan<- *blog.Info) error {
	panic("panicLog.InfoStream: not implemented")
}

// Used in Test_Run.
type readableLog struct {
	panicLog
	read func(ctx context.Context, nextIndex blog.Index, entriesCh chan<- []blog.Entry) error
}

func (aLog *readableLog) Read(ctx context.Context, nextIndex blog.Index, entriesCh chan<- []blog.Entry) error {
	return aLog.read(ctx, nextIndex, entriesCh)
}

// Used in Test_mainLoop and Test_abortTransactions.
type appendableLog struct {
	panicLog
	append func(ctx context.Context, data [][]byte) ([]blog.Index, error)
}

func (aLog *appendableLog) Append(ctx context.Context, data [][]byte) ([]blog.Index, error) {
	return aLog.append(ctx, data)
}

// Tests that Run calls getReplayIndex, then Read repeatedly.
func Test_Run(t *testing.T) {
	assert := assert.New(t)
	logrus.SetLevel(logrus.DebugLevel)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replayCount := 0
	getReplayPosition := func(ctx context.Context) (rpc.LogPosition, error) {
		replayCount++
		switch replayCount {
		case 1:
			return rpc.LogPosition{Index: 50, Version: logencoder.DefaultLogVersion}, nil
		case 2:
			return rpc.LogPosition{Index: 1000, Version: logencoder.DefaultLogVersion}, nil
		case 3:
			return rpc.LogPosition{Index: 1000, Version: logencoder.DefaultLogVersion}, nil
		}
		assert.Fail("Called getReplayIndex too much", "replayCount=%v", replayCount)
		return rpc.LogPosition{}, errors.New("fail")
	}
	aLog := new(readableLog)
	txTimer := New(ctx, aLog, getReplayPosition, Options{})
	readCount := 0
	aLog.read = func(ctx context.Context, nextIndex blog.Index, entriesCh chan<- []blog.Entry) error {
		defer close(entriesCh)
		readCount++
		switch readCount {
		case 1:
			assert.EqualValues(49, txTimer.LastApplied())
			assert.EqualValues(50, nextIndex)
			assert.Equal(0, txTimer.pending.Len())
			return blog.TruncatedError{Requested: nextIndex}
		case 2:
			assert.EqualValues(999, txTimer.LastApplied())
			assert.EqualValues(1000, nextIndex)
			return errors.New("some error")
		case 3:
			assert.EqualValues(999, txTimer.LastApplied())
			assert.EqualValues(1000, nextIndex)
			return blog.ClosedError{}
		}
		assert.Fail("Called Read too much", "readCount=%v", readCount)
		return blog.ClosedError{}
	}
	txTimer.pending.ReplaceOrInsert(txItem{index: 12})
	txTimer.Run()
	cancel()
	assert.Equal(3, replayCount)
	assert.Equal(3, readCount)
}

// Tests the basic flow through mainLoop, including that draining entriesCh
// happens before timing out more transactions.
func Test_mainLoop(t *testing.T) {
	assert := assert.New(t)
	mockClock := clocks.NewMock()
	clock = mockClock
	defer func() {
		clock = clocks.Wall
	}()
	entriesCh := make(chan []blog.Entry, 16)
	log := &appendableLog{}
	txTimer := New(context.Background(), log, nil, Options{})
	appendCount := 0
	log.append = func(ctx context.Context, data [][]byte) ([]blog.Index, error) {
		appendCount++
		logrus.WithFields(logrus.Fields{
			"call": appendCount,
		}).Info("log append")
		switch appendCount {
		case 1:
			if assert.Len(data, 1) {
				entry, _, err := logencoder.Decode(logencoder.DefaultLogVersion,
					blog.Entry{Data: data[0]})
				assert.NoError(err)
				cmd := entry.Command.(*logentry.TxDecisionCommand)
				assert.False(cmd.Commit)
			}
			assert.EqualValues(1325, txTimer.LastApplied())
			assert.EqualValues(1, txTimer.PendingTxs())
			// Now queue up a bunch of other entries. These must be applied before the next abort.
			for idx := blog.Index(1326); idx < 1336; idx++ {
				entriesCh <- []blog.Entry{
					{
						Index: idx,
						Data:  logencoder.Encode(&logentry.InsertTxCommand{}),
					},
				}
			}
			return nil, errors.New("some transient error that really shouldn't happen")
		case 2:
			assert.EqualValues(1335, txTimer.LastApplied())
			assert.EqualValues(11, txTimer.PendingTxs())
			close(entriesCh)
			return nil, errors.New("shutdown now")
		}
		return nil, errors.New("append called too much")
	}
	wait := parallel.Go(func() {
		txTimer.mainLoop(rpc.LogPosition{Index: 1325, Version: logencoder.DefaultLogVersion},
			entriesCh)
	})

	// Apply a transaction start entry.
	entriesCh <- []blog.Entry{
		{
			Index: 1325,
			Data:  logencoder.Encode(&logentry.InsertTxCommand{}),
		},
	}
	// When the last send completes, the previous apply (1325) must have completed.
	// This takes cap(entries) to fill the buffer, plus one more to make sure the
	// 1325 entry has not only been received but has also been applied.
	for i := 0; i <= cap(entriesCh); i++ {
		entriesCh <- []blog.Entry{}
	}

	// Advance the time so that the transaction is aborted.
	logrus.WithFields(logrus.Fields{
		"to": clock.Now().Add(txTimer.abortTxAfter + 3*time.Nanosecond),
	}).Info("Advancing time")
	mockClock.Advance(txTimer.abortTxAfter + 3*time.Nanosecond)
	// Now append should be called.
	wait()
	assert.Equal(2, appendCount)
}

func Test_apply(t *testing.T) {
	assert := assert.New(t)
	mockClock := clocks.NewMock()
	clock = mockClock
	defer func() {
		clock = clocks.Wall
	}()
	txTimer := New(context.Background(), nil, nil, Options{})
	nextPos := rpc.LogPosition{Index: 10, Version: logencoder.DefaultLogVersion}
	nextPos = txTimer.apply(nextPos, []blog.Entry{})

	mockClock.Advance(time.Hour)
	nextPos = txTimer.apply(nextPos, []blog.Entry{{
		Index: 10,
		Data:  logencoder.Encode(&logentry.InsertTxCommand{}),
	}})
	if assert.Equal(1, txTimer.pending.Len()) {
		item := txTimer.pending.Min().(txItem)
		assert.EqualValues(10, item.index)
		assert.Equal(clock.Now(), item.start)
	}

	nextPos = txTimer.apply(nextPos, []blog.Entry{{
		Index: 11,
		Data: logencoder.Encode(&logentry.TxDecisionCommand{
			Tx:     9,
			Commit: true,
		}),
	}})
	assert.Equal(1, txTimer.pending.Len())
	assert.EqualValues(1, txTimer.PendingTxs())

	nextPos = txTimer.apply(nextPos, []blog.Entry{{
		Index: 12,
		Data: logencoder.Encode(&logentry.TxDecisionCommand{
			Tx:     10,
			Commit: false,
		}),
	}})
	assert.Equal(0, txTimer.pending.Len())
	assert.EqualValues(0, txTimer.PendingTxs())

	nextPos = txTimer.apply(nextPos, []blog.Entry{
		{
			Index: 13,
			Data:  logencoder.Encode(&logentry.InsertTxCommand{}),
		},
		{
			Index: 14,
			Data:  logencoder.Encode(&logentry.WipeCommand{}),
		}})
	assert.Equal(0, txTimer.pending.Len())

	assert.NotPanics(func() {
		nextPos = txTimer.apply(nextPos, []blog.Entry{
			{
				Index: 15,
				Data:  logencoder.Encode(&logentry.PingCommand{}),
			},
		})
	})
	assert.EqualValues(15, txTimer.LastApplied())

	assert.Panics(func() {
		nextPos = txTimer.apply(nextPos, []blog.Entry{{
			Index: 50,
			Data:  []byte("a pile of garbage that won't decode into something valid"),
		}})
	})
}

func Test_abortTransactions(t *testing.T) {
	assert := assert.New(t)
	mockClock := clocks.NewMock()
	clock = mockClock
	defer func() {
		clock = clocks.Wall
	}()

	log := &appendableLog{}
	txTimer := New(context.Background(), log, nil, Options{})

	// Test that empty abort doesn't crash.
	log.append = func(ctx context.Context, data [][]byte) ([]blog.Index, error) {
		assert.Len(data, 0)
		return []blog.Index{}, nil
	}
	txTimer.abortTransactions()

	// Test that aborting only aborts old transactions (10 and 11, not 12 or 13).
	txTimer.pending.ReplaceOrInsert(txItem{index: 10, start: mockClock.Now().Add(-txTimer.abortTxAfter - time.Second)})
	txTimer.pending.ReplaceOrInsert(txItem{index: 11, start: mockClock.Now().Add(-txTimer.abortTxAfter - time.Nanosecond)})
	txTimer.pending.ReplaceOrInsert(txItem{index: 12, start: mockClock.Now().Add(-txTimer.abortTxAfter)})
	txTimer.pending.ReplaceOrInsert(txItem{index: 13, start: mockClock.Now().Add(-txTimer.abortTxAfter + time.Second)})
	log.append = func(ctx context.Context, data [][]byte) ([]blog.Index, error) {
		if assert.Len(data, 2) {
			entry, _, err := logencoder.Decode(logencoder.DefaultLogVersion,
				blog.Entry{Data: data[0]})
			assert.NoError(err)
			cmd := entry.Command.(*logentry.TxDecisionCommand)
			assert.EqualValues(10, cmd.Tx)
			assert.False(cmd.Commit)

			entry, _, err = logencoder.Decode(logencoder.DefaultLogVersion,
				blog.Entry{Data: data[1]})
			assert.NoError(err)
			cmd = entry.Command.(*logentry.TxDecisionCommand)
			assert.EqualValues(11, cmd.Tx)
			assert.False(cmd.Commit)
		}
		return []blog.Index{14, 15}, nil
	}
	txTimer.abortTransactions()
	assert.Equal(2, txTimer.pending.Len())
	assert.True(txTimer.pending.Has(txItem{index: 12}))
	assert.True(txTimer.pending.Has(txItem{index: 13}))

	// Test that a failure to abort leaves the transactions pending.
	mockClock.Advance(txTimer.abortTxAfter * 3)
	log.append = func(ctx context.Context, data [][]byte) ([]blog.Index, error) {
		assert.Len(data, 2)
		return nil, errors.New("some append failure")
	}
	txTimer.abortTransactions()
	assert.Equal(2, txTimer.pending.Len())
	assert.True(txTimer.pending.Has(txItem{index: 12}))
	assert.True(txTimer.pending.Has(txItem{index: 13}))
}
