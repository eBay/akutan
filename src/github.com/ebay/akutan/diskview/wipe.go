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
	"time"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/diskview/database"
	"github.com/ebay/akutan/diskview/keys"
	"github.com/ebay/akutan/logentry"
	"github.com/ebay/akutan/rpc"
	log "github.com/sirupsen/logrus"
)

type wipeKeyIterator interface {
	EnumerateKeys(startKey, endKey []byte, emit func(key []byte) error) error
}

type wipeRemoveBuffer interface {
	Remove([]byte) error
	Close() error
}

type wipeStats struct {
	Duration     time.Duration
	FirstIndex   blog.Index
	LastIndex    blog.Index
	AtIndex      blog.Index
	KeyCount     int64
	RemovedCount int64
	CloseCount   int64
}

// applyWipe will find keys with a valid index and remove them from the disk view
func (view *DiskView) applyWipe(pos rpc.LogPosition, cmd *logentry.WipeCommand, effects *applyEffects) {
	effects.changedMetadata = true
	if !view.cfg.DiskView.EnableWipe {
		log.Warnf("Wipe ignored: %v", pos)
		return
	}
	view.txns = make(map[blog.Index]*transaction)
	view.pending.Clear(false)
	// we need to close any pending changes or they won't be seen in the snapshot
	err := effects.dbWriter.Close()
	if err != nil {
		log.Fatalf("Cannot close buffer: %v", err)
	}
	snap := view.db.Snapshot()
	defer snap.Close()
	ws, err := applyWipeAt(pos.Index, effects.dbWriter, snap)
	if err != nil {
		log.Fatal(err)
	}
	view.db.Compact()
	view.ensureBaseFactsStored()
	view.graphCounts.update()
	log.Infof("Wipe applied @ %d %+v", ws.AtIndex, ws)
}

// applyWipeAt implements removing messages with a valid index and remove them
func applyWipeAt(msgIdx blog.Index, rb wipeRemoveBuffer, ke wipeKeyIterator) (wipeStats, error) {
	ws := wipeStats{
		Duration:     0,
		AtIndex:      msgIdx,
		KeyCount:     0,
		RemovedCount: 0,
		CloseCount:   0,
	}
	start := time.Now()
	buffer := func(key []byte) error {
		log.Debugf("Wiping @ %d '%s'", msgIdx, string(key))
		err := rb.Remove(key)
		if err != nil {
			return err
		}
		ws.RemovedCount++
		if 0 == ws.RemovedCount%100 {
			err = rb.Close()
			if err != nil {
				return err
			}
			ws.CloseCount++
		}
		return nil
	}
	defer rb.Close()
	var eErr error
	err := ke.EnumerateKeys(nil, []byte{0xFF}, func(key []byte) error {
		keyIdx := keys.ParseIndex(key)
		ws.KeyCount++
		// todo should we use a prefix here versus key < 1 checks
		if keyIdx < 1 {
			return nil
		}
		log.Debugf("Wiping @ %d: %d", msgIdx, keyIdx)
		if ws.FirstIndex == 0 {
			ws.FirstIndex = keyIdx
		}
		ws.LastIndex = keyIdx
		err := buffer(key)
		if err != nil {
			eErr = fmt.Errorf("cannot remove key @ %d (%v)", keyIdx, err)
			return database.ErrHalt
		}
		return nil
	})
	ws.Duration = time.Since(start)
	if err != nil {
		return ws, fmt.Errorf("cannot wipe @ %d: %v", msgIdx, err)
	}
	if eErr != nil {
		return ws, eErr
	}
	return ws, nil
}
