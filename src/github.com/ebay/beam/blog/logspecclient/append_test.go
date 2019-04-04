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

package logspecclient

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/ebay/beam/blog"
	"github.com/ebay/beam/util/parallel"
	"github.com/ebay/beam/util/random"
	"github.com/stretchr/testify/assert"
)

func TestAppendSingle_success(t *testing.T) {
	test := func(mode string) func(t *testing.T) {
		return func(t *testing.T) {
			assert := assert.New(t)
			server, log, cleanup := setup(t)
			defer cleanup()
			go log.batchAppends()
			go log.runAppends()
			switch mode {
			case "ok":
			case "redirect":
				server.startClock()
				server.next.redirect = 3
			case "unknown":
				server.startClock()
				server.next.unknown = true
			case "err":
				server.startClock()
				server.next.err = errors.New("some RPC error")
			case "full":
				server.startClock()
				server.next.full = true
			}
			index, err := log.AppendSingle(context.Background(), []byte("hello"))
			assert.NoError(err)
			assert.Equal(blog.Index(1), index)
			assert.Equal(0, server.next.redirect)
			assert.False(server.next.unknown)
			assert.NoError(server.next.err)
			assert.False(server.next.full)
		}
	}
	t.Run("ok", test("ok"))
	t.Run("redirect", test("redirect"))
	t.Run("unknown", test("unknown"))
	t.Run("err", test("err"))
	t.Run("full", test("full"))
}

func TestAppendSingle_closedSendingToUnbatched(t *testing.T) {
	assert := assert.New(t)
	_, log, cleanup := setup(t)
	defer cleanup()
	go log.batchAppends()
	go log.runAppends()
	cleanup()
	_, err := log.AppendSingle(context.Background(), []byte("hello"))
	assert.True(blog.IsClosedError(err))
}

func TestAppendSingle_closedReadingFromFuture(t *testing.T) {
	assert := assert.New(t)
	_, log, cleanup := setup(t)
	defer cleanup()
	go func() {
		req := <-log.unbatchedAppends
		log.batchedAppends <- appendBatch{futures: []appendFuture{req}}
		cleanup()
	}()
	go log.runAppends()
	_, err := log.AppendSingle(context.Background(), []byte("hello"))
	assert.True(blog.IsClosedError(err))
}

func TestAppendSingle_expired(t *testing.T) {
	assert := assert.New(t)
	_, log, cleanup := setup(t)
	defer cleanup()
	go log.batchAppends()
	go log.runAppends()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := log.AppendSingle(ctx, []byte("hello"))
	assert.Equal(ctx.Err(), err)
}

func TestAppend_ok(t *testing.T) {
	assert := assert.New(t)
	server, log, cleanup := setup(t)
	defer cleanup()
	go log.batchAppends()
	go log.runAppends()
	indexes, err := log.Append(context.Background(), [][]byte{
		[]byte("hello"), []byte("world"),
	})
	assert.NoError(err)
	assert.Equal([]uint64{1, 2}, indexes)
	assert.Equal([]byte("hello"), server.get(indexes[0]).Data)
	assert.Equal([]byte("world"), server.get(indexes[1]).Data)
}

// This checks that the top-level Append method splits the data into
// nicely-sized futures.
func TestAppend_splitting(t *testing.T) {
	assert := assert.New(t)
	_, log, cleanup := setup(t)
	defer cleanup()

	var futures []appendFuture
	wait := parallel.Go(func() {
		nextIndex := blog.Index(1)
		for {
			select {
			case <-log.ctx.Done():
				return
			case future := <-log.unbatchedAppends:
				indexes := make([]blog.Index, len(future.data))
				for i := range future.data {
					indexes[i] = nextIndex
					nextIndex++
				}
				future.resCh <- indexes
				futures = append(futures, future)
			}
		}
	})

	oneohone := []byte("!" +
		"0123456789" +
		"1123456789" +
		"2123456789" +
		"3123456789" +
		"4123456789" +
		"5123456789" +
		"6123456789" +
		"7123456789" +
		"8123456789" +
		"9123456789")
	data := [][]byte{
		[]byte("0123456789"),
		[]byte("1123456789"),
		[]byte("2123456789"),
		[]byte("3123456789"),
		[]byte("4123456789"),
		[]byte("5123456789"),
		[]byte("6123456789"),
		[]byte("7123456789"),
		[]byte("8123456789"),
		[]byte("9123456789"),
		[]byte("0123456789"),
		oneohone,
		[]byte("0123456789"),
	}
	indexes, err := log.Append(context.Background(), data)
	assert.NoError(err)
	cleanup()
	wait()
	if assert.Len(indexes, len(data)) {
		for i := range indexes {
			assert.Equal(blog.Index(i+1), indexes[i])
		}
	}
	assert.Len(futures, 4)
	totalSizeActual := 0
	for _, future := range futures {
		assert.True(len(future.data) > 0)
		size := 0
		for i := range future.data {
			size += bytesLen(future.data[i])
		}
		assert.True(size <= 100 || len(future.data) == 1)
		totalSizeActual += size
	}
	totalSizeExp := 0
	for i := range data {
		totalSizeExp += bytesLen(data[i])
	}
	assert.Equal(totalSizeExp, totalSizeActual)
}

func Test_batchAppends(t *testing.T) {
	random.SeedMath()
	assert := assert.New(t)
	_, log, cleanup := setup(t)
	defer cleanup()
	go log.batchAppends()

	makeFuture := func(size int) appendFuture {
		var future appendFuture
		for size > 0 {
			s := rand.Intn(size) + 1
			size -= s
			future.data = append(future.data, make([]byte, s))
		}
		return future
	}
	batchSize := func(batch appendBatch) int {
		return batch.bytes()
	}

	log.unbatchedAppends <- makeFuture(100)
	assert.Equal(100, batchSize(<-log.batchedAppends))
	log.unbatchedAppends <- makeFuture(80)
	log.unbatchedAppends <- makeFuture(20)
	assert.Equal(100, batchSize(<-log.batchedAppends))
	log.unbatchedAppends <- makeFuture(60)
	assert.Equal(60, batchSize(<-log.batchedAppends))
	log.unbatchedAppends <- makeFuture(80)
	log.unbatchedAppends <- makeFuture(20)
	log.unbatchedAppends <- makeFuture(3)
	select {
	case log.unbatchedAppends <- makeFuture(2):
		assert.Fail("shouldn't be able to send more until draining")
	default:
	}
	assert.Equal(100, batchSize(<-log.batchedAppends))
	assert.Equal(3, batchSize(<-log.batchedAppends))
}

// batchAppends used to send even empty batches onto the batchedAppends channel,
// causing 100% CPU usage. This tests that it doesn't.
func Test_batchAppends_masksSend(t *testing.T) {
	assert := assert.New(t)
	_, log, cleanup := setup(t)
	defer cleanup()
	go log.batchAppends()
	// This test is only effective on a fast enough machine, but 1ms seems to be
	// enough to fail most of the time on a MacBook Pro when the if-condition under
	// test is commented out.
	time.Sleep(time.Millisecond)
	select {
	case <-log.batchedAppends:
		assert.Fail("Shouldn't recv")
	default:
	}
}
