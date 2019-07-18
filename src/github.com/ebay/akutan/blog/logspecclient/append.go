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
	"time"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/logspec"
	"github.com/sirupsen/logrus"
)

// An appendFuture represents a top-level call to Log.Append.
type appendFuture struct {
	// The context passed into Log.Append. Used to avoid doing work on behalf of
	// this request if the context has already expired. In that event, resCh may
	// never receive a reply.
	ctx context.Context
	// Data to append.
	data [][]byte
	// The resulting indexes are sent on this channel. It has a buffer of size 1 so
	// that sending the results will never block, even if the Log.Append call has
	// already returned.
	resCh chan []uint64
}

// An appendBatch contains a set of futures. The total size of data in an
// appendBatch ideally does not exceed appendBatchSize. Large append bodies can
// cause the appendBatch to be larger than appendBatchSize.
type appendBatch struct {
	// A set of futures making up the batch, in no particular order.
	futures []appendFuture
	// The last time the batch was sent in an AppendRequest, or 0.
	sent time.Time
}

// filterExpired removes the futures whose context has been canceled or expired.
func (batch *appendBatch) filterExpired() {
	valid := batch.futures[:0]
	for _, future := range batch.futures {
		if future.ctx.Err() == nil {
			valid = append(valid, future)
		}
	}
	batch.futures = valid
}

// flattenedData returns a slice of all the proposals from all of the batch's
// futures.
func (batch *appendBatch) flattenedData() [][]byte {
	data := make([][]byte, 0, len(batch.futures))
	for _, future := range batch.futures {
		data = append(data, future.data...)
	}
	return data
}

// bytes returns the total size of all the proposals from all of the batch's
// futures.
func (batch *appendBatch) bytes() int {
	size := 0
	for _, future := range batch.futures {
		for i := range future.data {
			size += bytesLen(future.data[i])
		}
	}
	return size
}

// AppendSingle implements the method defined by blog.AkutanLog.
func (aLog *Log) AppendSingle(ctx context.Context, data []byte) (blog.Index, error) {
	indexes, err := aLog.Append(ctx, [][]byte{data})
	if err != nil {
		return 0, err
	}
	return indexes[0], err
}

// Append implements the method defined by blog.AkutanLog.
func (aLog *Log) Append(ctx context.Context, data [][]byte) ([]blog.Index, error) {
	// futures contains the data as split into different requests. Each future is
	// <= appendBatchSize in total bytes (or consists of a single larger entry).
	futures := make([]appendFuture, 0, 1)

	// addFuture appends a new future to futures, consisting of data from the last
	// invocation up to and excluding 'end'.
	start := 0
	addFuture := func(end int) {
		if start == end {
			return
		}
		futures = append(futures, appendFuture{
			ctx:   ctx,
			data:  data[start:end],
			resCh: make(chan []blog.Index, 1),
		})
		start = end
	}

	// Fill in futures.
	totalSize := 0 // total bytes of data
	size := 0      // total bytes from data[start:i]
	for i := range data {
		totalSize += bytesLen(data[i])
		switch {
		// i is a single large entry
		case bytesLen(data[i]) > aLog.appendBatchSize:
			addFuture(i)
			size = bytesLen(data[i])

		// [start:i] is full
		case size+bytesLen(data[i]) > aLog.appendBatchSize:
			addFuture(i)
			size = bytesLen(data[i])

		// [start:i+1] is still open
		default:
			size += bytesLen(data[i])
		}
	}
	addFuture(len(data))
	metrics.appendCallBytes.Observe(float64(totalSize))

	// Kick off each request.
	for _, future := range futures {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-aLog.ctx.Done():
			return nil, blog.ClosedError{}
		case aLog.unbatchedAppends <- future:
		}
	}

	// Collect the indexes.
	indexes := make([]uint64, 0, len(data))
	for _, future := range futures {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-aLog.ctx.Done():
			return nil, blog.ClosedError{}
		case rangeIndexes := <-future.resCh:
			indexes = append(indexes, rangeIndexes...)
		}
	}
	return indexes, nil
}

// batchAppends is a long-running goroutine that collects append futures from
// the unbatchedAppends channel, combines small ones together, and sends the
// combined batch to the batchedAppends channel.
func (aLog *Log) batchAppends() {
	// The current batch that futures are accumulated into.
	var batch []appendFuture
	batchBytes := 0
	// If batch is full, a single unwanted future might be here.
	var overflow []appendFuture
	overflowBytes := 0
	for {
		// The select clause below needs some of its cases enabled/disabled by various
		// conditions. The channels are set to nil to disable the corresponding cases.
		var masked struct {
			unbatchedAppends chan appendFuture
			batchedAppends   chan appendBatch
		}
		if len(overflow) == 0 {
			masked.unbatchedAppends = aLog.unbatchedAppends
		}
		if len(batch) > 0 {
			masked.batchedAppends = aLog.batchedAppends
		}

		select {
		// Got a new future: add it to batch or stash it in overflow.
		case future := <-masked.unbatchedAppends:
			futureBytes := 0
			for _, data := range future.data {
				futureBytes += bytesLen(data)
			}
			if batchBytes+futureBytes <= aLog.appendBatchSize {
				batch = append(batch, future)
				batchBytes += futureBytes
			} else {
				overflow = append(overflow, future)
				overflowBytes += futureBytes
			}

		// Sent out batch.
		case masked.batchedAppends <- appendBatch{futures: batch}:
			batch = nil
			batchBytes = 0
			if len(overflow) > 0 {
				batch, overflow = overflow, nil
				batchBytes, overflowBytes = overflowBytes, 0
			}

		// Log is exiting.
		case <-aLog.ctx.Done():
			return
		}
	}
}

// runAppends is a long-running goroutine that is the main event loop for
// driving appends. It coordinates opening streams, sending requests, and
// receiving replies.
func (aLog *Log) runAppends() {
	// An open stream or nil.
	var stream *appendStream

	// The sequence number of the last reply received from 'stream', or 0 if
	// none. If stream is nil, lastRecvd is 0.
	lastRecvd := uint64(0)
	// The request batches already sent on 'stream' that are waiting for
	// replies. They correspond to the sequence numbers from lastRecvd + 1
	// through and including the last sent sequence number. If stream is nil,
	// pending is empty. Whenever pending changes,
	// metrics.outstandingAppendRequests should be updated.
	var pending []*appendBatch

	// A queue of request batches that have not yet been sent.
	var queue []*appendBatch

	defer func() {
		if stream != nil {
			stream.cancel()
		}
	}()

	// reset throws out the stream.
	reset := func() {
		if stream == nil {
			return
		}
		stream.cancel()
		stream = nil
		lastRecvd = 0
		// Move pending to the front of the queue. Note: this avoids sharing the
		// underlying slice memory.
		queue = append(pending, queue...)
		pending = nil
		metrics.outstandingAppendRequests.Set(float64(len(pending)))
	}

	// The main event loop.
	for aLog.ctx.Err() == nil {
		// Remove the expired futures from the first batch in the queue.
		for len(queue) > 0 {
			queue[0].filterExpired()
			if len(queue[0].futures) > 0 {
				break
			}
			queue = queue[1:]
		}

		// Open a stream if we need one. This is delayed so that clients that never
		// append will never open a stream.
		if len(queue) > 0 && stream == nil {
			var err error
			stream, err = aLog.newAppendStream()
			if err != nil { // ErrClosed or aLog.ctx.Err()
				return
			}
		}

		var logger *logrus.Entry
		if stream == nil {
			logger = aLog.logger.WithField("RPC", "Append")
		} else {
			logger = stream.logger
		}

		// The select clause below needs several of its cases enabled/disabled by
		// various conditions. The channels are set to nil to disable the corresponding
		// cases.
		var masked struct {
			batchedAppends chan appendBatch
			streamCtxDone  <-chan struct{}
			streamReqCh    chan<- *appendBatch
			streamReplyCh  <-chan *logspec.AppendReply
			streamErrCh    <-chan error
		}
		// 'first' is only used if masked.streamReqCh != nil. It avoids a out-of-bounds
		// panic when using queue[0] directly.
		var first *appendBatch
		if len(queue) == 0 {
			masked.batchedAppends = aLog.batchedAppends
		}
		if stream != nil {
			masked.streamCtxDone = stream.ctx.Done()
			masked.streamReplyCh = stream.replyCh
			masked.streamErrCh = stream.errCh
			if len(queue) > 0 {
				masked.streamReqCh = stream.reqCh
				first = queue[0]
			}
		}

		// Wait for the next event.
		select {
		// The entire Log is shutting down.
		case <-aLog.ctx.Done():
			return

		// The stream is closed: clean up.
		case <-masked.streamCtxDone:
			reset()

		// Got another request batch to send: enqueue it.
		case batch := <-masked.batchedAppends:
			queue = append(queue, &batch)

		// Sent the first queued batch on the stream.
		case masked.streamReqCh <- first:
			metrics.appendRequestBytes.Observe(float64(first.bytes()))
			queue = queue[1:]
			pending = append(pending, first)
			metrics.outstandingAppendRequests.Set(float64(len(pending)))

		// Got a reply from the server.
		case res := <-masked.streamReplyCh:
			switch res := res.Reply.(type) {
			case *logspec.AppendReply_Ok:
				lastRecvd++
				if res.Ok.Sequence != lastRecvd {
					logger.WithFields(logrus.Fields{
						"expected": lastRecvd,
						"got":      res.Ok.Sequence,
					}).Errorf("Bad sequence number in reply")
					reset()
					continue
				}
				batch := pending[0]
				pending = pending[1:]
				metrics.outstandingAppendRequests.Set(float64(len(pending)))

				metrics.appendRTTSeconds.Observe(time.Since(batch.sent).Seconds())
				indexes := res.Ok.Indexes
				for _, future := range batch.futures {
					// This send won't block because the channel is buffered and we only send to it once.
					future.resCh <- indexes[:len(future.data)]
					indexes = indexes[len(future.data):]
				}

			case *logspec.AppendReply_Redirect:
				stream.cancel()
				_ = aLog.handleRedirect(stream.client, res.Redirect)
				logger.WithFields(logrus.Fields{
					"from": stream.client.server(),
					"to":   res.Redirect.Host,
				}).Info("Received redirect")
				reset()

			case *logspec.AppendReply_Full:
				stream.cancel()
				logger.Errorf("Blocked: log capacity is full")
				aLog.disconnectFrom(stream.client)
				_ = aLog.clock.SleepUntil(aLog.ctx,
					aLog.clock.Now().Add(fullBackoff))
				reset()

			default: // unknown non-OK reply
				stream.cancel()
				logger.Warnf("Got unknown non-OK reply")
				aLog.handleUnknownError(aLog.ctx, stream.client)
				reset()
			}

		// Got an error when sending to or receiving from the stream.
		case err := <-masked.streamErrCh:
			stream.cancel()
			logger.WithError(err).Warnf("Got error when receiving")
			aLog.handleRPCError(aLog.ctx, stream.client, err)
			reset()
		}
	}
}

// An appendStream is a bi-directional stream for Append requests and replies.
// It provides a channel-based interface, whereas gRPC only has blocking calls.
type appendStream struct {
	logger *logrus.Entry
	// A child of aLog.ctx. The stream.stream.Context() is a child of this one.
	ctx context.Context
	// Aborts 'ctx'.
	cancel  func()
	client  *clientConn
	stream  logspec.Log_AppendClient
	reqCh   chan *appendBatch
	replyCh chan *logspec.AppendReply
	errCh   chan error
}

// newAppendStream opens an appendStream with a server. The only error it
// returns is ErrClosed.
func (aLog *Log) newAppendStream() (*appendStream, error) {
	for {
		logger := aLog.logger.WithField("RPC", "Append")
		ctx, cancel := context.WithCancel(aLog.ctx)
		client, err := aLog.getConnection()
		if err != nil { // blog.ErrClosed
			cancel()
			return nil, err
		}
		logger = logger.WithFields(logrus.Fields{
			"server": client.server(),
		})
		logger.Debugf("Opening stream")
		grpcStream, err := client.service.Append(ctx)
		if err != nil {
			cancel()
			logger.WithFields(logrus.Fields{
				"error": err,
			}).Warnf("Opening stream failed")
			aLog.handleRPCError(ctx, client, err)
			continue
		}
		stream := &appendStream{
			logger:  logger,
			ctx:     ctx,
			cancel:  cancel,
			client:  client,
			stream:  grpcStream,
			reqCh:   make(chan *appendBatch),
			replyCh: make(chan *logspec.AppendReply),
			errCh:   make(chan error),
		}
		go aLog.sendAppendRequests(stream)
		go aLog.recvAppendReplies(stream)
		return stream, nil
	}
}

// sendAppendRequests sends request on a single append stream. This goroutine
// has the same lifetime as the stream, and there is one per stream. It cancels
// the stream and returns upon encountering any errors.
func (aLog *Log) sendAppendRequests(stream *appendStream) {
	defer stream.cancel()
	seq := uint64(0)
	for {
		seq++
		select {
		case <-stream.ctx.Done():
			return
		case batch := <-stream.reqCh:
			batch.sent = time.Now()
			req := logspec.AppendRequest{
				Sequence:  seq,
				Proposals: batch.flattenedData(),
			}
			err := stream.stream.Send(&req)
			if err != nil {
				select {
				case <-stream.ctx.Done():
				case stream.errCh <- err:
				}
				return
			}
		}
	}
}

// recvAppendReplies receives replies on a single append stream and forwards
// them to the stream's replyCh. This goroutine has the same lifetime as the
// stream, and there is one per stream. It cancels the stream, sends on
// stream.errCh, and returns upon encountering any errors.
func (aLog *Log) recvAppendReplies(stream *appendStream) {
	defer stream.cancel()
	for {
		res, err := stream.stream.Recv()
		if err != nil {
			select {
			case <-stream.ctx.Done():
			case stream.errCh <- err:
			}
			return
		}
		select {
		case <-stream.ctx.Done():
			return
		case stream.replyCh <- res:
		}
	}
}

// bytesLen is like len() but only applies to a byte slice. It provides a little
// extra safety when dealing with byte slices within other things that len()
// applies to.
func bytesLen(bytes []byte) int {
	return len(bytes)
}
