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

package diskview

import (
	"fmt"

	"github.com/ebay/beam/blog"
	"github.com/ebay/beam/diskview/database"
	"github.com/ebay/beam/diskview/keys"
	"github.com/ebay/beam/logentry"
	"github.com/ebay/beam/logentry/logencoder"
	"github.com/ebay/beam/logentry/logread"
	"github.com/ebay/beam/rpc"
	log "github.com/sirupsen/logrus"
)

// applyEffects captures delayed actions from apply().
type applyEffects struct {
	dbWriter database.BulkWriter
	// A list of channels to close once dbWriter has been flushed.
	closeAfterWriting []chan struct{}
	changedMetadata   bool
}

// Apply the supplied message to the current state by means of 'effects'.
// Assumes the write lock has already been attained.
func (view *DiskView) apply(cmdEntry logencoder.Entry, effects *applyEffects) {
	switch cmd := cmdEntry.Command.(type) {
	case *logentry.InsertTxCommand:
		view.applyTransaction(cmdEntry.Position, cmd, effects)
	case *logentry.WipeCommand:
		view.applyWipe(cmdEntry.Position, cmd, effects)
	case *logentry.TxDecisionCommand:
		view.applyDecision(cmd, effects)
	case *logentry.SkippedCommand:
		// nothing to do
	case *logentry.PingCommand:
		// nothing to do
	case *logentry.VersionCommand:
		// the Decode() call that generated this Entry applied the versioning rules.
	default:
		log.Panicf("Unexpected Command Type read from log: %T %v", cmdEntry.Command, cmdEntry.Command)
	}
}

func (view *DiskView) applyTransaction(position rpc.LogPosition, body *logentry.InsertTxCommand, effects *applyEffects) {
	log.Debugf("Begin fact tx %v: %+v", position, body)
	tx := &transaction{
		position: position,
		decided:  make(chan struct{}),
	}
	view.txns[position.Index] = tx
	for i := range body.Facts {
		f := logread.ToRPCFact(position.Index, &body.Facts[i])
		if view.partition.HasFact(&f) {
			key := keys.FactKey{Fact: &f, Encoding: view.partition.Encoding()}.Bytes()
			tx.keys = append(tx.keys, key)
			view.pending.ReplaceOrInsert(pendingItem{key: key, tx: tx})
		}
	}
}

func (view *DiskView) applyInsertFactsAt(index blog.Index, insertCmd *logentry.InsertTxCommand, effects *applyEffects) {
	facts := make([]rpc.Fact, len(insertCmd.Facts))
	for i := range insertCmd.Facts {
		facts[i] = logread.ToRPCFact(index, &insertCmd.Facts[i])
	}
	view.insertFactsForOurPartition(facts, effects)
}

func (view *DiskView) insertFactsForOurPartition(facts []rpc.Fact, effects *applyEffects) {
	for _, fact := range facts {
		if view.partition.HasFact(&fact) {
			log.Debugf("Adding fact %v = %v @ %d", fact.Id, fact, fact.Index)
			key := keys.FactKey{Fact: &fact, Encoding: view.partition.Encoding()}.Bytes()
			if err := effects.dbWriter.Buffer(key, nil); err != nil {
				log.Panicf("Unable to apply fact: %v: %v", fact, err)
			}
		}
	}
}

func (view *DiskView) applyDecision(body *logentry.TxDecisionCommand, effects *applyEffects) {
	log.Debugf("Apply decision: %+v\n", body)
	tx, ok := view.txns[body.Tx]
	if !ok {
		if body.Commit {
			log.Debugf("Couldn't commit tx %v because not found in pending transactions. this is normal if someone else timed out the transaction before the client committed", body.Tx)
		} else {
			log.Debugf("couldn't abort tx %v because not found in pending transactions. this is normal if someone else timed out the transaction first", body.Tx)
		}
		return
	}
	if body.Commit {
		for _, key := range tx.keys {
			if err := effects.dbWriter.Buffer(key, nil); err != nil {
				panic(fmt.Sprintf("Unable to write key: %v", err))
			}
		}
	}
	for _, key := range tx.keys {
		view.pending.Delete(pendingItem{key: key, tx: tx})
	}
	delete(view.txns, uint64(body.Tx))
	effects.closeAfterWriting = append(effects.closeAfterWriting, tx.decided)
}
