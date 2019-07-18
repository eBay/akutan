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

package logping

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
	"github.com/stretchr/testify/assert"
)

type testMocks struct {
	ctx         context.Context
	cancel      func()
	clock       *clocks.Mock
	pinger      *Pinger
	logAppender *mockLogAppender
	logReader   *mockLogReader
	state       pingState
}

// mockPinger sets up a bunch of mock objects for unit testing a Pinger. It
// returns these, as well as a teardown function that callers must call to clean
// up state. The teardown function may safely be called more than once.
func mockPinger(t *testing.T) (*testMocks, func()) {
	assert := assert.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	mocks := &testMocks{
		ctx:         ctx,
		cancel:      cancel,
		clock:       clocks.NewMock(),
		logAppender: new(mockLogAppender),
		logReader:   new(mockLogReader),
		state: pingState{
			nextSeq: 1,
		},
	}
	var err error
	mocks.pinger, err = New(ctx, nil, nil, Options{
		Appender:             mocks.logAppender,
		logReader:            mocks.logReader,
		PingsUntilDisconnect: 10,
	})
	assert.NoError(err)
	assert.Len(mocks.pinger.localID, 16)
	assert.NotEqual(make([]byte, 16), mocks.pinger.localID)
	clock = mocks.clock
	return mocks, func() {
		cancel()
		clock = clocks.Wall
	}
}

// Tests that Run calls Info, then Read repeatedly.
func Test_Run(t *testing.T) {
	assert := assert.New(t)
	mocks, cancel := mockPinger(t)
	defer cancel()
	mocks.pinger.getReplayPosition = func(ctx context.Context) (rpc.LogPosition, error) {
		return rpc.LogPosition{}, context.Canceled
	}
	mocks.pinger.Run() // returns immediately

	replayCount := 0
	mocks.pinger.getReplayPosition = func(ctx context.Context) (rpc.LogPosition, error) {
		replayCount++
		return rpc.LogPosition{
			Index:   blog.Index(31 + 10*replayCount),
			Version: logencoder.DefaultLogVersion,
		}, nil
	}
	readCount := 0
	mocks.logReader.read = func(nextIndex blog.Index, entriesCh chan<- []blog.Entry) error {
		defer close(entriesCh)
		readCount++
		switch readCount {
		case 1:
			assert.EqualValues(40, mocks.pinger.LastApplied())
			assert.EqualValues(41, nextIndex)
			return blog.TruncatedError{Requested: nextIndex}
		case 2:
			assert.EqualValues(50, mocks.pinger.LastApplied())
			assert.EqualValues(51, nextIndex)
			return errors.New("some error")
		case 3:
			assert.EqualValues(60, mocks.pinger.LastApplied())
			assert.EqualValues(61, nextIndex)
			return blog.ClosedError{}
		}
		return errors.New("called too much")
	}
	mocks.pinger.Run()
	assert.Equal(3, replayCount)
	assert.Equal(3, readCount)
}

// This tests the log ping flow through the main loop.
// Most tests for the ping functionality are done separately below.
func Test_mainLoop(t *testing.T) {
	assert := assert.New(t)
	mocks, cancel := mockPinger(t)
	defer cancel()
	entryCh := make(chan []blog.Entry)
	wait := parallel.Go(func() {
		mocks.pinger.mainLoop(logencoder.LogInitialPosition(), entryCh)
	})

	appendCh := make(chan blog.Entry)
	nextIndex := blog.Index(1)
	mocks.logAppender.appendSingle = func(data []byte) (blog.Index, error) {
		index := nextIndex
		nextIndex++
		appendCh <- blog.Entry{Index: index, Data: data}
		if index <= 2 {
			return index, nil
		}
		return 0, errors.New("ants in pants")
	}

	// Use entryCh to make sure the event loop initialized (it has no buffer)
	entryCh <- []blog.Entry{}

	// Trigger the next ping.
	mocks.clock.Advance(mocks.pinger.pingInterval + 3*time.Nanosecond)

	// Find it in the log, and feed it back to mainLoop.
	entry := <-appendCh
	dec, _, err := logencoder.Decode(logencoder.DefaultLogVersion, entry)
	assert.NoError(err)
	ping := dec.Command.(*logentry.PingCommand)
	assert.Equal(uint64(1), ping.Seq)
	entryCh <- []blog.Entry{entry}
	entryCh <- []blog.Entry{} // wait

	// Trigger the next ping.
	mocks.clock.Advance(mocks.pinger.pingInterval + 3*time.Nanosecond)
	// Find it in the log, but don't feedback to the entryCh to abort the
	// current ping.
	<-appendCh

	// Trigger the next ping, which will fail to append.
	mocks.clock.Advance(mocks.pinger.pingInterval + 3*time.Nanosecond)
	entry = <-appendCh
	dec, _, err = logencoder.Decode(logencoder.DefaultLogVersion, entry)
	assert.NoError(err)
	ping = dec.Command.(*logentry.PingCommand)
	assert.Equal(uint64(3), ping.Seq)

	// We need to wait for pinger.finish() to complete before another ping will be triggered.
	i := 0
	for {
		// Trigger the next ping.
		mocks.clock.Advance(mocks.pinger.pingInterval + 3*time.Nanosecond)
		select {
		default:
			i++
			if i < 100 {
				time.Sleep(time.Millisecond)
				continue
			}
			assert.Fail("pinger.finish() took too long")
		case <-appendCh:
		}
		break
	}
	cancel()
	wait()
}

// Tests that disconnects happen after the right number of pings.
func Test_mainLoop_disconnect(t *testing.T) {
	assert := assert.New(t)
	mocks, cancel := mockPinger(t)
	defer cancel()
	entryCh := make(chan []blog.Entry)
	wait := parallel.Go(func() {
		mocks.pinger.mainLoop(logencoder.LogInitialPosition(), entryCh)
	})
	// Use entryCh to make sure the event loop initialized (it has no buffer)
	entryCh <- []blog.Entry{}

	// Returns the expected number of disconnects after appending ping 'idx'.
	exp := func(idx blog.Index) int {
		return int(idx) / 10
	}
	assert.Equal(0, exp(9))
	assert.Equal(1, exp(10))
	assert.Equal(1, exp(19))
	assert.Equal(2, exp(20))

	appendCh := make(chan blog.Entry)
	nextIndex := blog.Index(1)
	mocks.logAppender.appendSingle = func(data []byte) (blog.Index, error) {
		index := nextIndex
		nextIndex++
		appendCh <- blog.Entry{Index: index, Data: data}
		return index, nil
	}

	for idx := blog.Index(1); idx <= 25; idx++ {
		mocks.clock.Advance(mocks.pinger.pingInterval + 3*time.Nanosecond)
		entry := <-appendCh
		entryCh <- []blog.Entry{entry}
		entryCh <- []blog.Entry{} // wait
		assert.Equal(exp(idx), mocks.logReader.NumDisconnects(),
			"index=%v", idx)
	}
	cancel()
	wait()
}

// Asserts that state.ping has an outstanding, well-formed ping request.
func pingActive(assert *assert.Assertions, mocks *testMocks) {
	assert.True(mocks.state.currentSeq > 0)
	assert.Equal(mocks.state.nextSeq-1, mocks.state.currentSeq)
	assert.Equal(clock.Now().Add(mocks.pinger.pingInterval), mocks.state.nextAt)
	assert.NotNil(mocks.state.span)
}

// Asserts that state.ping has no outstanding, well-formed ping request.
func pingInactive(assert *assert.Assertions, mocks *testMocks) {
	assert.Equal(uint64(0), mocks.state.currentSeq)
	assert.True(mocks.state.nextSeq > 0)
	assert.Equal(clock.Now().Add(mocks.pinger.pingInterval), mocks.state.nextAt)
	assert.Nil(mocks.state.span)
}

// Normal ping scenario: append completes before read.
func Test_startPing_apply_normal(t *testing.T) {
	assert := assert.New(t)
	mocks, cancel := mockPinger(t)
	defer cancel()
	appendCh := make(chan blog.Entry)
	mocks.logAppender.appendSingle = func(data []byte) (blog.Index, error) {
		appendCh <- blog.Entry{Index: 1, Data: data}
		return 1, nil
	}
	mocks.pinger.startPing(&mocks.state, nil)
	pingActive(assert, mocks)
	entry := <-appendCh
	mocks.pinger.apply(&mocks.state, logencoder.LogInitialPosition(), []blog.Entry{entry})
	pingInactive(assert, mocks)
}

// Inverted ping scenario: read completes before append.
func Test_startPing_apply_inverted(t *testing.T) {
	assert := assert.New(t)
	mocks, cancel := mockPinger(t)
	defer cancel()
	appendCh := make(chan blog.Entry)
	mocks.logAppender.appendSingle = func(data []byte) (blog.Index, error) {
		appendCh <- blog.Entry{Index: 1, Data: data} // blocks append
		return 1, nil
	}
	mocks.pinger.startPing(&mocks.state, nil)
	pingActive(assert, mocks)
	entry := blog.Entry{
		Index: 1,
		Data: logencoder.Encode(&logentry.PingCommand{
			Writer: mocks.pinger.localID,
			Seq:    mocks.state.currentSeq,
		}),
	}
	mocks.pinger.apply(&mocks.state, logencoder.LogInitialPosition(), []blog.Entry{entry})
	pingInactive(assert, mocks)
	actualEntry := <-appendCh // allows append to complete
	pingInactive(assert, mocks)
	assert.EqualValues(entry, actualEntry)
}

// Error ping scenario: append fails and finish cleans up.
func Test_appendPing_error(t *testing.T) {
	assert := assert.New(t)
	mocks, cancel := mockPinger(t)
	defer cancel()
	mocks.logAppender.appendSingle = func(data []byte) (blog.Index, error) {
		return 0, errors.New("ants in pants")
	}
	appendFailedCh := make(chan logentry.PingCommand, 1)
	mocks.pinger.startPing(&mocks.state, appendFailedCh)
	cmd := <-appendFailedCh
	pingActive(assert, mocks)
	assert.EqualValues(1, cmd.Seq)
	assert.EqualValues(mocks.pinger.localID, cmd.Writer)
	if mocks.pinger.isCurrent(&mocks.state, cmd) {
		mocks.pinger.finishCurrent(&mocks.state, appendFailed)
	}
	pingInactive(assert, mocks)
}
