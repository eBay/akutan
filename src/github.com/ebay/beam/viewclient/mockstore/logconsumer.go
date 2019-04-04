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

package mockstore

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/ebay/beam/blog"
	"github.com/ebay/beam/logentry"
	"github.com/ebay/beam/logentry/logencoder"
	"github.com/ebay/beam/logentry/logread"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/parallel"
	"github.com/ebay/beam/viewclient/lookups"
	"github.com/sirupsen/logrus"
)

// A LogConsumer reads from a Beam log and applies commands to its internal
// database of facts. It is safe for concurrent access.
type LogConsumer struct {
	lock   sync.Mutex
	locked struct {
		facts     []rpc.Fact
		nextPos   rpc.LogPosition
		txns      map[blog.Index]transaction
		changedCh chan struct{}
	}
}

type transaction struct {
	insertFacts []rpc.Fact
}

// NewLogConsumer constructs an LogConsumer that contains only the well-known
// facts.
func NewLogConsumer() *LogConsumer {
	consumer := &LogConsumer{}
	consumer.locked.nextPos = logencoder.LogInitialPosition()
	consumer.locked.txns = make(map[blog.Index]transaction)
	consumer.locked.changedCh = make(chan struct{})
	return consumer
}

// Lookups returns an object that can perform lookup operations against the
// database.
//
// Each of the lookups takes a log index. It waits until the log consumer has
// reached this log index and until all pending transactions have been resolved
// before proceeding.
func (consumer *LogConsumer) Lookups() lookups.All {
	return storeLookups{snapshot: consumer.Snapshot}
}

// LastApplied returns the index of the last entry consumed. It returns 0 if no
// entries have been consumed.
func (consumer *LogConsumer) LastApplied() blog.Index {
	consumer.lock.Lock()
	nextIndex := consumer.locked.nextPos.Index
	consumer.lock.Unlock()
	return nextIndex - 1
}

// Current returns a snapshot of the current database state, excluding all
// pending transactions.
func (consumer *LogConsumer) Current() *Snapshot {
	consumer.lock.Lock()
	snap := NewSnapshot(consumer.locked.facts)
	consumer.lock.Unlock()
	return snap
}

// Snapshot waits until the consumer has applied the given index and all
// transactions up to that index have been resolved, then returns a snapshot
// with any facts above the given index filtered out. If the context expires
// first, it returns nil and a context error.
func (consumer *LogConsumer) Snapshot(ctx context.Context, index blog.Index) (*Snapshot, error) {
	for {
		consumer.lock.Lock()
		nextIndex := consumer.locked.nextPos.Index
		oldest := consumer.oldestPendingTxnLocked()
		snap := NewSnapshot(consumer.locked.facts)
		changedCh := consumer.locked.changedCh
		consumer.lock.Unlock()

		if nextIndex > index && oldest > index {
			return snap.Filtered(index), nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changedCh:
		}
	}
}

// oldestPendingTxnLocked returns the index of the earliest unresolved
// transaction, or math.MaxUint64 if no transactions are pending. It must be
// called with the lock held.
func (consumer *LogConsumer) oldestPendingTxnLocked() blog.Index {
	oldest := blog.Index(math.MaxUint64)
	for txn := range consumer.locked.txns {
		if oldest > txn {
			oldest = txn
		}
	}
	return oldest
}

// BlogReader is the subset of blog.BeamLog needed by LogConsumer.
type BlogReader interface {
	Read(ctx context.Context, nextIndex blog.Index, entriesCh chan<- []blog.Entry) error
}

// Consume is a blocking call that reads from the log and applies entries. It
// returns errors from beamLog.
func (consumer *LogConsumer) Consume(ctx context.Context, beamLog BlogReader) error {
	apply := func(lastPos, nextPos rpc.LogPosition, cmd logencoder.Entry) {
		consumer.lock.Lock()
		defer consumer.lock.Unlock()
		// This if-statement allows concurrent Consume() calls to safely
		// race each other. It's not the expected usage, but it's easier
		// to check for than to explain in the docs.
		if consumer.locked.nextPos.Index != lastPos.Index {
			return
		}
		consumer.applyLocked(cmd)
		consumer.locked.nextPos = nextPos
		close(consumer.locked.changedCh)
		consumer.locked.changedCh = make(chan struct{})
	}

	consumer.lock.Lock()
	nextPos := consumer.locked.nextPos
	consumer.lock.Unlock()

	entriesCh := make(chan []blog.Entry, 4)
	wait := parallel.Go(func() {
		for entries := range entriesCh {
			for _, entry := range entries {
				lastPos := nextPos
				cmd, p, err := logencoder.Decode(lastPos.Version, entry)
				if err != nil {
					panic(fmt.Sprintf("Cannot parse entry at index %v", entry.Index))
				}
				nextPos = p
				apply(lastPos, nextPos, cmd)
			}
		}
	})
	err := beamLog.Read(ctx, nextPos.Index, entriesCh)
	wait()
	return err
}

// applyLocked updates the state based on the given log command. It must be
// called with the lock held.
func (consumer *LogConsumer) applyLocked(entry logencoder.Entry) {
	switch cmd := entry.Command.(type) {
	case *logentry.InsertTxCommand:
		tx := transaction{
			insertFacts: make([]rpc.Fact, len(cmd.Facts)),
		}
		for i := range cmd.Facts {
			tx.insertFacts[i] = logread.ToRPCFact(entry.Position.Index, &cmd.Facts[i])
		}
		consumer.locked.txns[entry.Position.Index] = tx

	case *logentry.TxDecisionCommand:
		tx, ok := consumer.locked.txns[cmd.Tx]
		if ok {
			if cmd.Commit {
				consumer.locked.facts = append(consumer.locked.facts, tx.insertFacts...)
			}
			delete(consumer.locked.txns, cmd.Tx)
		} else {
			if cmd.Commit {
				logrus.Warnf("Can't commit missing tx at index %d. "+
					"It could have been previously aborted.", cmd.Tx)
			} else {
				logrus.Warnf("Aborting missing tx at index %d", cmd.Tx)
			}
		}

	case *logentry.SkippedCommand:
		// nothing to do
	case *logentry.PingCommand:
		// nothing to do
	case *logentry.VersionCommand:
		// the Decode() call that generated this Entry applied the versioning rules.
	default: // includes *logentry.WipeCommand
		panic(fmt.Sprintf("Unexpected Command Type read from log: %T %v",
			entry.Command, entry.Command))
	}
}
