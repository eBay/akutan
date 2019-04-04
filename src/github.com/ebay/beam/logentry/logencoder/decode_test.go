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
	"fmt"
	"testing"

	"github.com/ebay/beam/blog"
	"github.com/ebay/beam/logentry"
	"github.com/ebay/beam/rpc"
	"github.com/stretchr/testify/assert"
)

func Test_Skipped(t *testing.T) {
	dec, nextPos, err := Decode(1, blog.Entry{Index: 4, Skip: true})
	assert.NoError(t, err)
	assert.Equal(t, logPos(5, 1), nextPos)
	assert.Equal(t, uint64(4), dec.Position.Index)
	assert.Equal(t, int32(1), dec.Position.Version)
	assert.Equal(t, &logentry.SkippedCommand{}, dec.Command)
}

func logPos(index blog.Index, ver Version) rpc.LogPosition {
	return rpc.LogPosition{
		Index:   index,
		Version: ver,
	}
}

func Test_LogInitialPosition(t *testing.T) {
	assert.Equal(t, logPos(1, 1), LogInitialPosition())
	p := LogInitialPosition()
	p.Index = 4
	assert.Equal(t, logPos(1, 1), LogInitialPosition())
}

func Test_Empty(t *testing.T) {
	_, _, err := Decode(1, blog.Entry{Index: 4})
	assert.EqualError(t, err, "invalid command: data has zero length at LogIndex:4")
}

func Test_DecodeUnknownType(t *testing.T) {
	e := blog.Entry{Index: 1, Data: []byte{'a'}}
	_, _, err := Decode(1, e)
	assert.EqualError(t, err, "unexpected command type of 0x61 at LogIndex:1")
}

func Test_DecodePbUnmarshalError(t *testing.T) {
	e := blog.Entry{Index: 1, Data: []byte{byte(txDecision), 0xFF, 0xFF}}
	_, _, err := Decode(1, e)
	assert.EqualError(t, err, "unexpected EOF")
}

func Test_VerifyVersionChangeValid(t *testing.T) {
	oldMax := maxVersion
	maxVersion = 3
	defer func() {
		maxVersion = oldMax
	}()
	assertValid := func(index blog.Index, currentVer, newVer int32) {
		cmd := logentry.VersionCommand{MoveToVersion: newVer}
		be := blog.Entry{Index: index, Data: Encode(&cmd)}
		dec, nextPos, err := Decode(currentVer, be)
		assert.NoError(t, err)
		assert.Equal(t, logPos(index, currentVer), dec.Position)
		assert.Equal(t, logPos(index+1, newVer), nextPos)
		assert.Equal(t, &cmd, dec.Command)
	}
	// version changes can go forward upto & including maxVersion, they also don't need to be single steps
	assertValid(4, 1, 1)
	assertValid(4, 1, 2)
	assertValid(4, 1, 3)
	assertValid(4, 2, 3)

	cf := newMockFatal()
	defer cf.close()
	e := blog.Entry{Index: 2, Data: Encode(&logentry.VersionCommand{MoveToVersion: 4})}
	Decode(3, e)
	assert.Equal(t, 1, cf.count)
	assert.Equal(t, []string{"VersionCommand moveTo:4 from log @2, but maxVersion for this process is 3, and can't support the requested version"}, cf.msgs)

	// attempt to change to an earlier version that is currently active should be ignored.
	cf.reset()
	moveTo1 := logentry.VersionCommand{MoveToVersion: 1}
	e = blog.Entry{Index: 12, Data: Encode(&moveTo1)}
	cmd, nextPos, err := Decode(3, e)
	assert.NoError(t, err)
	assert.Equal(t, &moveTo1, cmd.Command)
	assert.Equal(t, logPos(12, 3), cmd.Position)
	assert.Equal(t, logPos(13, 3), nextPos)
}

func Test_DecodeSetsVersion(t *testing.T) {
	e := blog.Entry{Index: 201, Data: Encode(&logentry.SkippedCommand{})}
	cmd, nextPos, err := Decode(3, e)
	assert.NoError(t, err)
	assert.Equal(t, logPos(202, 3), nextPos)
	assert.Equal(t, int32(3), cmd.Position.Version)
	assert.Equal(t, uint64(201), cmd.Position.Index)
}

type mockFatal struct {
	count      int
	msgs       []string
	originalFn func(string, ...interface{})
}

func newMockFatal() *mockFatal {
	f := &mockFatal{
		originalFn: fatalf,
	}
	fatalf = f.fatalf
	return f
}

func (f *mockFatal) reset() {
	f.count = 0
	f.msgs = f.msgs[:0]
}

func (f *mockFatal) close() {
	fatalf = f.originalFn
}

func (f *mockFatal) fatalf(msg string, args ...interface{}) {
	f.count++
	f.msgs = append(f.msgs, fmt.Sprintf(msg, args...))
}
