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

package logencoder

import (
	"bytes"
	"errors"
	"testing"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/logentry"
	"github.com/ebay/akutan/logentry/logwrite"
	"github.com/stretchr/testify/assert"
)

func Test_EncodeDecode(t *testing.T) {
	// round trips a Command through encode / decode and verifies that the results matches the original input
	fb := new(logwrite.InsertFactBuilder)
	oldMax := maxVersion
	maxVersion = 4
	defer func() {
		maxVersion = oldMax
	}()
	tests := []ProtobufCommand{
		&logentry.InsertTxCommand{Facts: []logentry.InsertFact{fb.SOffset(2).PID(3).OString("Bob", 2).Fact()}},
		&logentry.WipeCommand{},
		&logentry.TxDecisionCommand{Tx: 42, Commit: true},
		&logentry.SkippedCommand{},
		&logentry.VersionCommand{MoveToVersion: 4},
		&logentry.PingCommand{Writer: []byte("hello world"), Seq: 325},
	}
	types := []byte{byte(insertTx), byte(txDecision), byte(wipe), byte(skipped), byte(version), byte(ping)}
	for _, test := range tests {
		enc := Encode(test)
		assert.True(t, bytes.IndexByte(types, enc[0]) != -1)
		logEntry := blog.Entry{Index: 11, Data: enc}
		dec, nextPos, err := Decode(3, logEntry)
		assert.NoError(t, err)
		assert.Equal(t, test, dec.Command)
		assert.Equal(t, uint64(11), dec.Position.Index)
		assert.Equal(t, int32(3), dec.Position.Version)
		switch dec.Command.(type) {
		case *logentry.VersionCommand:
			assert.Equal(t, logPos(12, 4), nextPos)
		default:
			assert.Equal(t, logPos(12, 3), nextPos)
		}
	}
}

func Test_UnexpectedType(t *testing.T) {
	f := logentry.InsertFact{}
	assert.Panics(t, func() { Encode(&f) })
}

func Test_MarshalFails(t *testing.T) {
	cmd := &failedPb{}
	assert.Panics(t, func() { Encode(cmd) })
}

// failedPb implements the ProtobufCommand interface, and will always return an
// error for MarshalTo
type failedPb struct {
}

func (f *failedPb) Size() int {
	return 5
}
func (f *failedPb) ProtoMessage() {
}
func (f *failedPb) MarshalTo([]byte) (int, error) {
	return 0, errors.New("Failed to Marshal")
}
func (f *failedPb) Unmarshal([]byte) error {
	return nil
}
