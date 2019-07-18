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
	"testing"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/logentry"
	"github.com/ebay/akutan/logentry/logencoder"
	"github.com/ebay/akutan/logentry/logwrite"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/space"
	"github.com/google/btree"
	"github.com/stretchr/testify/assert"
)

type testPartition struct{}

func (*testPartition) HasFact(fact *rpc.Fact) bool {
	return fact.Subject < 1000
}

func (*testPartition) Encoding() rpc.FactKeyEncoding {
	return rpc.KeyEncodingSPO
}

func (*testPartition) HashRange() space.Range {
	panic("not implemented")
}

type testBulkWriter struct {
	keys []string
}

func (w *testBulkWriter) Buffer(key []byte, value []byte) error {
	w.keys = append(w.keys, string(key))
	return nil
}

func (*testBulkWriter) Remove(key []byte) error {
	panic("not implemented")
}

func (*testBulkWriter) Close() error {
	panic("not implemented")
}

func logPos(index blog.Index, version logencoder.Version) rpc.LogPosition {
	return rpc.LogPosition{
		Index:   index,
		Version: version,
	}
}

func Test_applyTransaction(t *testing.T) {
	assert := assert.New(t)
	view := &DiskView{
		txns:      make(map[blog.Index]*transaction),
		pending:   btree.New(4),
		partition: new(testPartition),
	}
	effects := applyEffects{}
	body := &logentry.InsertTxCommand{
		Facts: []logentry.InsertFact{
			new(logwrite.InsertFactBuilder).SID(10).PID(20).OString("turtle", 0).Fact(),
			// not on this partition:
			new(logwrite.InsertFactBuilder).SID(5000).PID(20).OKID(30).Fact(),
		},
	}

	view.applyTransaction(logPos(500, 1), body, &effects)
	assert.Len(view.txns, 1)
	tx, ok := view.txns[500]
	if assert.True(ok) {
		assert.Equal(logPos(500, 1), tx.position)
		if assert.Len(tx.keys, 1) {
			assert.Contains(string(tx.keys[0]), "turtle")
		}
	}
	if assert.Equal(1, view.pending.Len()) {
		item := view.pending.Min().(pendingItem)
		assert.Contains(string(item.key), "turtle")
		assert.Equal(tx, item.tx)
		assert.Equal(logPos(500, 1), item.tx.position)
	}
}

func Test_applyDecision_notFound(t *testing.T) {
	assert := assert.New(t)
	view := &DiskView{}
	for _, commit := range []bool{false, true} {
		body := &logentry.TxDecisionCommand{
			Tx:     100,
			Commit: commit,
		}
		effects := applyEffects{}
		view.applyDecision(body, &effects)
		assert.Len(effects.closeAfterWriting, 0)
	}
}

func Test_applyDecision_commit(t *testing.T) {
	assert := assert.New(t)
	tx := &transaction{
		position: logPos(100, 1),
		decided:  make(chan struct{}),
		keys: [][]byte{
			[]byte("green"),
			[]byte("red"),
			[]byte("blue"),
		},
	}
	view := &DiskView{
		pending: btree.New(4),
		txns: map[blog.Index]*transaction{
			100: tx,
		},
	}
	for _, key := range tx.keys {
		view.pending.ReplaceOrInsert(pendingItem{key: key, tx: tx})
	}
	view.pending.ReplaceOrInsert(pendingItem{key: []byte("monster"), tx: nil})
	body := &logentry.TxDecisionCommand{
		Tx:     100,
		Commit: true,
	}
	dbWriter := new(testBulkWriter)
	effects := applyEffects{
		dbWriter: dbWriter,
		closeAfterWriting: []chan struct{}{
			make(chan struct{}),
		},
	}
	assert.Contains(view.txns, blog.Index(100))
	view.applyDecision(body, &effects)
	if assert.Len(effects.closeAfterWriting, 2) {
		assert.Equal(tx.decided, effects.closeAfterWriting[1])
	}
	assert.NotContains(view.txns, blog.Index(100))
	assert.Equal([]string{"green", "red", "blue"}, dbWriter.keys)
	if assert.Equal(1, view.pending.Len()) {
		assert.Equal("monster", string(view.pending.Min().(pendingItem).key))
	}
}

func Test_applyDecision_abort(t *testing.T) {
	assert := assert.New(t)
	tx := &transaction{
		position: logPos(100, 1),
		decided:  make(chan struct{}),
		keys: [][]byte{
			[]byte("green"),
			[]byte("red"),
			[]byte("blue"),
		},
	}
	view := &DiskView{
		pending: btree.New(4),
		txns: map[blog.Index]*transaction{
			100: tx,
		},
	}
	for _, key := range tx.keys {
		view.pending.ReplaceOrInsert(pendingItem{key: key, tx: tx})
	}
	view.pending.ReplaceOrInsert(pendingItem{key: []byte("monster"), tx: nil})
	body := &logentry.TxDecisionCommand{
		Tx:     100,
		Commit: false,
	}
	dbWriter := new(testBulkWriter)
	effects := applyEffects{
		dbWriter:          dbWriter,
		closeAfterWriting: nil,
	}
	assert.Contains(view.txns, blog.Index(100))
	view.applyDecision(body, &effects)
	assert.Len(effects.closeAfterWriting, 1)
	assert.NotContains(view.txns, blog.Index(100))
	assert.Len(dbWriter.keys, 0)
	if assert.Equal(1, view.pending.Len()) {
		assert.Equal("monster", string(view.pending.Min().(pendingItem).key))
	}
}
