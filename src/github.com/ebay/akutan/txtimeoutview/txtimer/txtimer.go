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

// Package txtimer watches for slow transactions and aborts them.
package txtimer

import (
	"context"
	"sync"
	"time"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/logentry"
	"github.com/ebay/akutan/logentry/logencoder"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/clocks"
	"github.com/ebay/akutan/util/parallel"
	"github.com/google/btree"
	log "github.com/sirupsen/logrus"
)

// An alias for the normal clock. This is swapped out for some unit tests.
var clock = clocks.Wall

// TxTimer watches for slow transactions and aborts them. A TxTimer must be
// constructed with New.
type TxTimer struct {
	// Used to find where to start reading from the log.
	getReplayPosition GetReplayPosition
	// The client used for log reads and log appends.
	aLog blog.AkutanLog
	// A background context used for all the TxTimer's activity.
	ctx context.Context
	// A constant value defined in Options. Never zero here.
	abortTxAfter time.Duration
	// Tracks all transactions that have started before 'nextIndex' but have not
	// yet completed. Each item in the btree has type txItem. May only be accessed
	// from the Run/mainLoop goroutine.
	pending *btree.BTree
	// Protects locked.
	lock sync.Mutex
	// These fields are protected by lock.
	locked struct {
		// The last log index that this view has applied.
		lastApplied blog.Index
		// The number of current pending transactions, equal to len(pending).
		numPendingTxns int
	}
}

// txItem values are stored in the pending btree.
type txItem struct {
	// The key in the btree is the log index of the begin transaction command.
	index blog.Index
	// The value in the btree is the wall time at which this TxTimer read and
	// applied the entry given by 'index'.
	start time.Time
}

// Less is needed to order the btree. This has two important properties. First,
// the oldest pending transaction is the first item in the tree. Second,
// deleting by txItem{index: idx} with a zero start time works correctly.
func (item txItem) Less(other btree.Item) bool {
	return item.index < other.(txItem).index
}

// GetReplayPosition returns the earliest position (log index and version) that
// might have a pending transaction. This is where reading the log will begin.
// It continues retrying until it succeeds; the only error it returns is a
// context error.
type GetReplayPosition func(context.Context) (rpc.LogPosition, error)

// Options define optional arguments to New. The zero value of Options is usable.
type Options struct {
	// The minimum time after seeing a start transaction command before appending a
	// corresponding abort decision. The zero value defaults to 3s.
	AbortTxAfter time.Duration
}

// New constructs a new TxTimer. The TxTimer will not do anything until Run is
// called. The given context is for the background activity of the TxTimer, not
// just for the call to New. The TxTimer will read from and append to aLog.
func New(
	backgroundCtx context.Context,
	aLog blog.AkutanLog,
	getReplayPosition GetReplayPosition,
	options Options,
) *TxTimer {
	if options.AbortTxAfter == 0 {
		options.AbortTxAfter = time.Second * 3
	}
	return &TxTimer{
		getReplayPosition: getReplayPosition,
		aLog:              aLog,
		ctx:               backgroundCtx,
		abortTxAfter:      options.AbortTxAfter,
		pending:           btree.New(16),
	}
}

// Run is a blocking call that watches for slow transactions and aborts them.
// Run returns once the context given to New is canceled or the AkutanLog given to
// New is closed. Run may only be called once per TxTimer.
func (txTimer *TxTimer) Run() {
	for {
		// Find the new replay position and reinitialize state.
		nextPosition, err := txTimer.getReplayPosition(txTimer.ctx)
		if err != nil { // context error
			return
		}
		txTimer.setLastApplied(nextPosition.Index - 1)
		txTimer.pending.Clear(false)
		txTimer.setPendingTxs(txTimer.pending.Len())

		// Read from the log.
		entriesCh := make(chan []blog.Entry, 16)
		wait := parallel.Go(func() {
			txTimer.mainLoop(nextPosition, entriesCh)
		})
		err = txTimer.aLog.Read(txTimer.ctx, nextPosition.Index, entriesCh)
		wait()
		switch {
		case err == txTimer.ctx.Err():
			return // normal exit
		case blog.IsClosedError(err):
			return // normal exit
		case blog.IsTruncatedError(err):
			log.WithError(err).Warnf("TxTimer fell behind in reading log. Restarting")
			metrics.restarts.Inc()
			continue
		default:
			// TODO: this should be a Panicf, but the Kafka client might still return
			// transient errors.
			log.WithError(err).Warnf("TxTimer: Unexpected error reading log")
			metrics.restarts.Inc()
		}
	}
}

// mainLoop applies consecutive entries from the log, runs timers, and aborts
// transactions.
func (txTimer *TxTimer) mainLoop(nextPosition rpc.LogPosition, entriesCh <-chan []blog.Entry) {
	// Used to sleep until it's time to abort the earliest pending transaction.
	alarm := clock.NewAlarm()
	defer alarm.Stop()

	for {
		// Logically, applying log entries should take priority over aborting
		// transactions, since those log entries might well complete the transactions.
		// However, Go select statements don't allow priorities (they're always
		// randomized). As a workaround, this loop applies all the ready log entries
		// without blocking first.
		for {
			select {
			case <-txTimer.ctx.Done():
				return
			case entries, ok := <-entriesCh:
				if !ok {
					return
				}
				nextPosition = txTimer.apply(nextPosition, entries)
			default:
				goto doneApplying
			}
		}
	doneApplying:

		// Set up the alarm.
		if txTimer.pending.Len() == 0 {
			alarm.Stop()
		} else {
			start := txTimer.pending.Min().(txItem).start
			alarm.Set(start.Add(txTimer.abortTxAfter + time.Nanosecond))
		}

		// Wait for new log entries or the alarm.
		select {
		case <-txTimer.ctx.Done():
			return
		case entries, ok := <-entriesCh:
			if !ok {
				return
			}
			nextPosition = txTimer.apply(nextPosition, entries)
		case <-alarm.WaitCh():
			// Note: this blocks while appending to the log.
			txTimer.abortTransactions()
		}
	}
}

// apply updates the local state based on the commands read from the log.
func (txTimer *TxTimer) apply(nextPosition rpc.LogPosition, entries []blog.Entry) rpc.LogPosition {
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
		case *logentry.InsertTxCommand:
			metrics.startsApplied.Inc()
			txTimer.pending.ReplaceOrInsert(txItem{
				index: entry.Index,
				start: clock.Now(),
			})

		case *logentry.TxDecisionCommand:
			if body.Commit {
				metrics.commitsApplied.Inc()
			} else {
				metrics.abortsApplied.Inc()
			}
			txTimer.pending.Delete(txItem{
				index: body.Tx,
			})

		case *logentry.WipeCommand:
			log.WithFields(log.Fields{
				"index": entry.Index,
			}).Warn("Applying wipe")
			metrics.wipesApplied.Inc()
			txTimer.pending.Clear(false)

		default:
			// not interested in other types
		}

		txTimer.setLastApplied(entry.Index)
		txTimer.setPendingTxs(txTimer.pending.Len())
	}
	return nextPosition
}

// abortTransactions aborts the pending transactions that have been running
// longer than the abortTxAfter timeout.
func (txTimer *TxTimer) abortTransactions() {
	now := clock.Now()
	var commands []logentry.TxDecisionCommand
	txTimer.pending.Ascend(func(btreeItem btree.Item) bool {
		item := btreeItem.(txItem)
		if now.Sub(item.start) <= txTimer.abortTxAfter {
			return false // break iteration
		}
		log.WithFields(log.Fields{
			"txIndex":   item.index,
			"startTime": item.start,
			"elapsed":   now.Sub(item.start),
			"now":       now,
		}).Info("Aborting transaction")
		commands = append(commands, logentry.TxDecisionCommand{
			Tx:     item.index,
			Commit: false,
		})
		return true // continue iteration
	})

	metrics.abortsStartedTotal.Add(float64(len(commands)))
	enc := make([][]byte, len(commands))
	for i := range commands {
		enc[i] = logencoder.Encode(&commands[i])
	}
	_, err := txTimer.aLog.Append(txTimer.ctx, enc)
	if err != nil { // context canceled or log closed
		log.WithFields(log.Fields{
			"count": len(commands),
			"error": err,
		}).Warn("Error appending abort decisions to log")
		metrics.abortsFailedTotal.Add(float64(len(commands)))
		return
	}
	log.WithFields(log.Fields{
		"count": len(commands),
	}).Debug("Successfully appended abort decisions to log")
	metrics.abortsSucceededTotal.Add(float64(len(commands)))
	// Clear out these pending transactions right away instead of waiting for
	// apply() to get to these entries. Otherwise, if reading from the log is even
	// a bit behind, the main loop will call right back into this function and abort
	// these same transactions again.
	for _, cmd := range commands {
		txTimer.pending.Delete(txItem{
			index: cmd.Tx,
		})
	}
	txTimer.setPendingTxs(txTimer.pending.Len())
}

// PendingTxs returns the number of pending transactions.
// This method is thread safe.
func (txTimer *TxTimer) PendingTxs() int {
	txTimer.lock.Lock()
	v := txTimer.locked.numPendingTxns
	txTimer.lock.Unlock()
	return v
}

func (txTimer *TxTimer) setPendingTxs(count int) {
	txTimer.lock.Lock()
	txTimer.locked.numPendingTxns = count
	txTimer.lock.Unlock()
	metrics.pendingTxns.Set(float64(count))
}

// LastApplied returns the last applied log index.
// This method is thread safe.
func (txTimer *TxTimer) LastApplied() blog.Index {
	txTimer.lock.Lock()
	v := txTimer.locked.lastApplied
	txTimer.lock.Unlock()
	return v
}

func (txTimer *TxTimer) setLastApplied(index blog.Index) {
	txTimer.lock.Lock()
	txTimer.locked.lastApplied = index
	txTimer.lock.Unlock()
	metrics.lastApplied.Set(float64(index))
}
