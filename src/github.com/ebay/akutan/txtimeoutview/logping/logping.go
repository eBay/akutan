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

// Package logping measures the latency of Akutan's log by appending to it and
// reading from it. The purpose of this package is to provide continuous
// run-time metrics (not just benchmarks) about the log's health and
// performance.
//
package logping

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/config"
	"github.com/ebay/akutan/logentry"
	"github.com/ebay/akutan/logentry/logencoder"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/clocks"
	"github.com/ebay/akutan/util/parallel"
	"github.com/ebay/akutan/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
)

// An alias for the normal clock. This is swapped out for some unit tests.
var clock = clocks.Wall

// A Pinger measures the log's latency. It must be constructed with New.
type Pinger struct {
	// Used to find where to start reading from the log.
	getReplayPosition GetReplayPosition
	// A background context for all of the Pinger's operations, as well as any
	// AkutanLog instances it creates.
	ctx context.Context
	// Used to append to the log.
	logAppender LogAppender
	// Used to read from the log.
	logReader logReader
	// A constant value defined in Options. Never zero here.
	pingInterval time.Duration
	// A constant value defined in Options. Never zero here, but may be
	// math.MaxUint64--be careful with overflow.
	pingsUntilDisconnect uint64
	// A random number placed into the PingCommand that's used to identify this
	// Pinger when reading. Together with the sequence numbers, this is needed to
	// accurately time the read in the case where the read completes before the
	// Append returns the command's index.
	localID []byte
	// Protects locked.
	lock sync.Mutex
	// Protected by lock.
	locked struct {
		// The last log index that this view has applied.
		lastApplied blog.Index
	}
}

// Options define optional arguments to New. The zero value of Options is usable.
type Options struct {
	// If set, the Pinger will append PingCommand entries to the log using this.
	// Otherwise, it will open its own AkutanLog with blog.New.
	Appender LogAppender
	// Once one ping command is read from the log, how long to wait before writing
	// the next one to the log. The zero value defaults to 1s.
	PingInterval time.Duration
	// After this many pings, the Pinger will disconnect from the server it's
	// reading from and immediately re-connect to some log server. This is used to
	// measure latency to different servers, in case some are faster than others.
	//
	// This feature is not available with every AkutanLog instance (it requires an
	// optional Disconnect method). If the AkutanLog instance doesn't support it or
	// this is zero, the Pinger will never artificially disconnect.
	PingsUntilDisconnect uint64
	// This is used for unit testing to pass in a mock log reader, but it's normally
	// nil.
	logReader logReader
}

// LogAppender defines a subset of the blog.AkutanLog interface for appending.
// The Pinger uses a different AkutanLog for reads.
type LogAppender interface {
	AppendSingle(ctx context.Context, data []byte) (blog.Index, error)
}

// logReader defines a subset of the blog.AkutanLog interface for reading.
type logReader interface {
	Read(ctx context.Context, nextIndex blog.Index, entriesCh chan<- []blog.Entry) error
}

// GetReplayPosition returns the latest known log position (index and version).
// This is where reading the log will begin. It continues retrying until it
// succeeds; the only error it returns is a context error.
type GetReplayPosition func(context.Context) (rpc.LogPosition, error)

// New constructs a new Pinger. The Pinger will not do anything until Run is called.
// The given context is for the background activity of the Pinger, not just for
// the call to New. The cfg is used to construct AkutanLog instances.
func New(
	backgroundCtx context.Context,
	cfg *config.Akutan, getReplayPosition GetReplayPosition,
	options Options,
) (*Pinger, error) {
	ctx, abort := context.WithCancel(backgroundCtx)
	if options.PingInterval == 0 {
		options.PingInterval = time.Second
	}
	if options.PingsUntilDisconnect == 0 {
		options.PingsUntilDisconnect = math.MaxUint64
	}
	if options.Appender == nil {
		aLog, err := blog.New(ctx, cfg)
		if err != nil {
			abort()
			return nil, err
		}
		options.Appender = aLog
	}
	if options.logReader == nil {
		// Note: This is intentionally distinct from the appender because we want the
		// two to be redirected/disconnected independently.
		aLog, err := blog.New(ctx, cfg)
		if err != nil {
			abort()
			return nil, err
		}
		options.logReader = aLog
	}

	localID := make([]byte, 16)
	_, err := cryptorand.Read(localID)
	if err != nil {
		abort()
		return nil, fmt.Errorf("log pinger: failed to generate random local ID: %v", err)
	}

	// This is a workaround to silence go vet. ctx should only be canceled on the
	// failure paths above, since the ctx has the same lifetime as the Pinger. go vet
	// wants it to be canceled even on the success path. Assigning anything to
	// 'abort' suppresses the warning.
	abort = nil
	return &Pinger{
		getReplayPosition:    getReplayPosition,
		ctx:                  ctx,
		logAppender:          options.Appender,
		logReader:            options.logReader,
		pingInterval:         options.PingInterval,
		pingsUntilDisconnect: options.PingsUntilDisconnect,
		localID:              localID,
	}, nil
}

// pingState tracks the state of 0 or 1 active log pings, which measure the
// log's latency. This state is local to mainLoop and must not be accessed from
// other goroutines.
type pingState struct {
	// A counter containing the sequence number that will be assigned to the next
	// ping. Initialized at 1.
	nextSeq uint64
	// If nonzero, a ping has been started with this sequence number, but its
	// command has not yet been read from the log. If zero, no ping is currently
	// taking place.
	currentSeq uint64
	// If currentSeq is nonzero, the current ping has been started at this time.
	// Otherwise, this field is zero value.
	currentStartedAt time.Time
	// If currentSeq is zero, the next ping will be started shortly after this
	// time. Otherwise, the current ping will be aborted after this time (if the
	// command has not been read from the log by then).
	nextAt time.Time
	// This is used to report the log latency encompassing reads. If currentSeq is
	// nonzero, this is an open span that started just before the append began and
	// will end when the read completes. Otherwise, this field is nil.
	span opentracing.Span
	// After this sequence number completes, disconnect from the logReader.
	disconnectAfter uint64
}

// Run is a long-running goroutine that measures log latency. Run returns once
// the context given to New is canceled. Run may only be called once per Pinger.
func (pinger *Pinger) Run() {
	for {
		nextPosition, err := pinger.getReplayPosition(pinger.ctx)
		if err != nil { // context error
			log.WithError(err).Warn("Fetching log replay position failed. Exiting")
			return
		}
		pinger.setLastApplied(nextPosition.Index - 1)
		entriesCh := make(chan []blog.Entry, 16)
		wait := parallel.Go(func() {
			pinger.mainLoop(nextPosition, entriesCh)
		})
		err = pinger.logReader.Read(pinger.ctx, nextPosition.Index, entriesCh)
		wait()
		switch {
		case err == pinger.ctx.Err():
			return // normal exit
		case blog.IsClosedError(err):
			return // normal exit
		case blog.IsTruncatedError(err):
			log.WithError(err).Warn("Pinger fell behind in reading log. Restarting")
			metrics.restarts.Inc()
			continue
		default:
			// TODO: this should be a Panicf, but the Kafka client might still return
			// transient errors.
			log.WithError(err).Error("Pinger: Unexpected error reading log. Restarting")
			metrics.restarts.Inc()
		}
	}
}

// mainLoop applies consecutive entries from the log, appends pings, and measures
// their latency.
//
// We wanted to be able to process reads from the log while in the middle of a
// log append. This helps break down the latency into its distinct components,
// but it's trickier in terms of concurrency. As a result, this function is
// written in the style of a single-threaded event loop. It keeps local state
// ('pingState') and only blocks in one place--a select statement that waits on
// various channels and timers. Blocking work happens in other goroutines, which
// do not access the 'pingState'.
func (pinger *Pinger) mainLoop(nextPosition rpc.LogPosition, entriesCh <-chan []blog.Entry) {
	state := pingState{
		nextSeq:         1,
		nextAt:          clock.Now().Add(pinger.pingInterval),
		disconnectAfter: pinger.pingsUntilDisconnect,
	}

	// This is used to report when appending a PingCommand to the log fails. In such
	// a case, the reader may never find the ping command in the log and needs to
	// clean up its state.
	appendFailedCh := make(chan logentry.PingCommand)

	// Used to sleep until it's time to write a new PingCommand to time the log.
	pingAlarm := clock.NewAlarm()
	defer pingAlarm.Stop()

	for {
		// Set up ping alarm.
		pingAlarm.Set(state.nextAt)

		// This is the main, blocking select statement.
		select {
		case <-pinger.ctx.Done():
			return

		case entries, ok := <-entriesCh:
			if !ok {
				return
			}
			nextPosition = pinger.apply(&state, nextPosition, entries)

		case <-pingAlarm.WaitCh():
			if state.currentSeq > 0 {
				// Ping timed out.
				pinger.finishCurrent(&state, aborted)
			}
			pinger.startPing(&state, appendFailedCh)

		case cmd := <-appendFailedCh:
			if pinger.isCurrent(&state, cmd) {
				pinger.finishCurrent(&state, appendFailed)
			}
		}
	}
}

// apply updates the local state based on the commands read from the log.
func (pinger *Pinger) apply(state *pingState, nextPosition rpc.LogPosition, entries []blog.Entry) rpc.LogPosition {
	for _, entry := range entries {
		if entry.Index != nextPosition.Index {
			log.WithFields(log.Fields{
				"expected": nextPosition.Index,
				"got":      entry.Index,
			}).Panic("Apply got unexpected log entry")
		}
		cmd, nextPos, err := logencoder.Decode(nextPosition.Version, entry)
		if err != nil {
			log.WithFields(log.Fields{
				"index": entry.Index,
			}).Panic("Could not decode log entry")
		}
		nextPosition = nextPos
		switch body := cmd.Command.(type) {
		case *logentry.PingCommand:
			if pinger.isCurrent(state, *body) {
				pinger.finishCurrent(state, succeeded)
			}
		default:
			// not interested in other types
		}
		pinger.setLastApplied(entry.Index)
	}
	return nextPosition
}

// isCurrent returns true if the supplied cmd is for the current ping.
func (pinger *Pinger) isCurrent(state *pingState, cmd logentry.PingCommand) bool {
	return state.currentSeq > 0 &&
		bytes.Equal(pinger.localID, cmd.Writer) &&
		state.currentSeq == cmd.Seq
}

const succeeded = "succeeded"
const appendFailed = "append_failed"
const aborted = "aborted"

// finishCurrent is called to update the local state to end the current ping.
func (pinger *Pinger) finishCurrent(state *pingState, status string) {
	if state.currentSeq == 0 {
		return
	}
	switch status {
	case succeeded:
		metrics.pingsSucceeded.Inc()
		metrics.pingLatencySeconds.Observe(
			time.Since(state.currentStartedAt).Seconds())
	case appendFailed:
		// No need to increment the pingAppendsFailed metric as it's already
		// done in appendPing.
	case aborted:
		metrics.pingsAborted.Inc()
	}

	log.WithFields(log.Fields{
		"seq":    state.currentSeq,
		"status": status,
	}).Debugf("Log ping %v", status)
	state.span.SetTag("status", status)

	if state.currentSeq >= state.disconnectAfter {
		// Disconnect from logReader if supported.
		state.disconnectAfter += pinger.pingsUntilDisconnect
		reader, ok := pinger.logReader.(blog.Disconnector)
		if ok {
			reader.Disconnect()
			metrics.disconnects.Inc()
		}
	}
	state.span.Finish()
	state.span = nil
	state.currentSeq = 0
	state.currentStartedAt = time.Time{}
}

// startPing begins measuring the log's latency. This function does not block; the I/O
// is done in a separate goroutine, which does not access 'state'.
func (pinger *Pinger) startPing(state *pingState, appendFailedCh chan<- logentry.PingCommand) {
	// This avoids using opentracing.StartSpanFromContext because this isn't
	// logically a child span of pinger.ctx.
	span := opentracing.StartSpan("log ping")
	appendCtx := opentracing.ContextWithSpan(pinger.ctx, span)
	span.SetTag("startIndex", pinger.LastApplied())
	state.currentSeq = state.nextSeq
	state.nextSeq++
	state.span = span
	now := clock.Now()
	state.nextAt = now.Add(pinger.pingInterval)
	state.currentStartedAt = now
	metrics.pingsStarted.Inc()
	cmd := logentry.PingCommand{
		Writer: pinger.localID,
		Seq:    uint64(state.currentSeq),
	}
	log.WithFields(log.Fields{
		"seq": state.currentSeq,
	}).Debug("Starting log ping")
	go pinger.appendPing(appendCtx, cmd, appendFailedCh)
}

// appendPing appends a PingCommand to the log. It's blocking, so it runs in a
// separate goroutine from the event loop and has no access to pingState.
func (pinger *Pinger) appendPing(ctx context.Context, cmd logentry.PingCommand, appendFailedCh chan<- logentry.PingCommand) {
	// Note that it's possible for this span to extend beyond the pingRequest's span.
	// That would indicate the log server is slow to acknowledge appends but fast to
	// serve the entries to readers.
	span, ctx := opentracing.StartSpanFromContext(ctx, "append ping")
	tracing.UpdateMetric(span, metrics.pingAppendLatencySeconds)
	defer span.Finish()
	index, err := pinger.logAppender.AppendSingle(ctx, logencoder.Encode(&cmd))
	if err != nil { // context error or log closed
		log.WithFields(log.Fields{
			"seq":   cmd.Seq,
			"error": err,
		}).Warn("Error appending PingCommand to log")
		metrics.pingAppendsFailed.Inc()
		span.SetTag("error", true)
		appendFailedCh <- cmd
		return
	}
	log.WithFields(log.Fields{
		"seq":   cmd.Seq,
		"index": index,
	}).Debug("Successfully appended PingCommand to log")
	metrics.pingAppendsSucceeded.Inc()
	span.SetTag("index", index)
}

// LastApplied returns the last applied log index.
// This method is thread safe.
func (pinger *Pinger) LastApplied() blog.Index {
	pinger.lock.Lock()
	v := pinger.locked.lastApplied
	pinger.lock.Unlock()
	return v
}

func (pinger *Pinger) setLastApplied(index blog.Index) {
	pinger.lock.Lock()
	pinger.locked.lastApplied = index
	pinger.lock.Unlock()
	metrics.lastApplied.Set(float64(index))
}
