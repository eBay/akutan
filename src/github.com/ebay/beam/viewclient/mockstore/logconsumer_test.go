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
	"testing"
	"time"

	"github.com/ebay/beam/blog"
	"github.com/ebay/beam/blog/mockblog"
	"github.com/ebay/beam/logentry"
	"github.com/ebay/beam/logentry/logencoder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type logConsumerTest struct {
	t        *testing.T
	ctx      context.Context
	consumer *LogConsumer
	log      *mockblog.Log
}

func setupLogConsumerTest(t *testing.T) (*logConsumerTest, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	test := &logConsumerTest{
		t:        t,
		ctx:      ctx,
		consumer: NewLogConsumer(),
		log:      mockblog.New(ctx),
	}
	go test.consumer.Consume(test.ctx, test.log)
	go test.consumer.Consume(test.ctx, test.log) // race the first one
	return test, cancel
}

func (test *logConsumerTest) append(cmds []logencoder.ProtobufCommand) {
	proposals := make([][]byte, len(cmds))
	for i := range cmds {
		proposals[i] = logencoder.Encode(cmds[i])
	}
	_, err := test.log.Append(test.ctx, proposals)
	require.NoError(test.t, err)
}

func (test *logConsumerTest) anInsertCmd() *logentry.InsertTxCommand {
	return &logentry.InsertTxCommand{
		Facts: []logentry.InsertFact{
			{
				FactIDOffset: 1,
				Subject:      logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 5003}},
				Predicate:    logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 1003}},
				Object:       logentry.KGObject{Value: &logentry.KGObject_AKID{AKID: 1004}},
			}, {
				FactIDOffset: 2,
				Subject:      logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Offset{Offset: 1}},
				Predicate:    logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 1003}},
				Object:       logentry.KGObject{Value: &logentry.KGObject_AKID{AKID: 1005}},
			},
		},
	}
}

func Test_LogConsumer_skip(t *testing.T) {
	test, cancel := setupLogConsumerTest(t)
	defer cancel()
	assert.Equal(t, blog.Index(0), test.consumer.LastApplied())
	cmds := []logencoder.ProtobufCommand{
		// These are ignored.
		&logentry.SkippedCommand{},
		&logentry.PingCommand{},
		&logentry.VersionCommand{},
	}
	test.append(cmds)
	index := blog.Index(len(cmds))
	snap, err := test.consumer.Snapshot(test.ctx, index)
	require.NoError(t, err)
	assert.Empty(t, snap.StoredFacts())
	assert.Equal(t, index, test.consumer.LastApplied())
}

func Test_LogConsumer_txCommit(t *testing.T) {
	test, cancel := setupLogConsumerTest(t)
	defer cancel()
	cmds := []logencoder.ProtobufCommand{
		test.anInsertCmd(),
		&logentry.TxDecisionCommand{
			Tx:     1,
			Commit: true,
		},
	}
	test.append(cmds)
	snap, err := test.consumer.Snapshot(test.ctx, 2)
	require.NoError(t, err)
	facts := snap.StoredFacts()
	require.Len(t, facts, 2)
	assert.Equal(t, "{idx:1 id:1001 s:5003 p:1003 o:#1004}", facts[0].String())
	assert.Equal(t, "{idx:1 id:1002 s:1001 p:1003 o:#1005}", facts[1].String())
}

func Test_LogConsumer_txAbort(t *testing.T) {
	test, cancel := setupLogConsumerTest(t)
	defer cancel()
	cmds := []logencoder.ProtobufCommand{
		test.anInsertCmd(),
		&logentry.TxDecisionCommand{
			Tx:     1,
			Commit: false,
		},
	}
	test.append(cmds)
	snap, err := test.consumer.Snapshot(test.ctx, 2)
	require.NoError(t, err)
	facts := snap.StoredFacts()
	require.Len(t, facts, 0)
}

func Test_LogConsumer_txPending(t *testing.T) {
	test, cancel := setupLogConsumerTest(t)
	defer cancel()
	cmds := []logencoder.ProtobufCommand{
		test.anInsertCmd(),
	}
	test.append(cmds)
	shortCtx, shortCancel := context.WithTimeout(test.ctx, time.Millisecond)
	defer shortCancel()
	_, err := test.consumer.Snapshot(shortCtx, 1)
	assert.Equal(t, context.DeadlineExceeded, err)
	assert.Empty(t, test.consumer.Current().StoredFacts())
}
