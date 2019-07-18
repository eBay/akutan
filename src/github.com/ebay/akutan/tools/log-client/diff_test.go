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

package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os/exec"
	"strings"
	"testing"

	"github.com/ebay/akutan/logspec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_diffLog_NoDiffs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	data := make([][]byte, 200)
	for i := range data {
		data[i] = []byte{byte(i), byte(i + 1)}
	}

	planks := make([]string, 3)
	for i := range planks {
		planks[i] = startPlank(ctx, t)
		s := logServer{}
		s.init(ctx, planks[i])
		plankAppend(ctx, t, s.client, data)
	}

	opts, err := parseArgs(append([]string{"diff"}, planks...))
	assert.NoError(t, err)
	assert.NotNil(t, opts)
	out := bytes.Buffer{}
	err = diffLog(ctx, opts, &out)
	assert.NoError(t, err)
	assert.True(t, strings.Contains(out.String(), "No diffs"), out.String())
}

func Test_diffLog_Diffs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	data := make([][]byte, 200)
	for i := range data {
		data[i] = []byte{byte(i), byte(i + 1)}
	}
	badLog := make([][]byte, 200)
	copy(badLog, data)
	badLog[175] = []byte("boom")

	planks := make([]string, 3)
	for i := range planks {
		planks[i] = startPlank(ctx, t)
		s := logServer{}
		s.init(ctx, planks[i])
		if i == 0 {
			plankAppend(ctx, t, s.client, badLog)
		} else {
			plankAppend(ctx, t, s.client, data)
		}
	}

	opts, err := parseArgs(append([]string{"diff"}, planks...))
	assert.NoError(t, err)
	assert.NotNil(t, opts)
	out := bytes.Buffer{}
	err = diffLog(ctx, opts, &out)
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "different values at index 176"), err)
	assert.True(t, strings.Contains(out.String(), "different values at index 176"), out.String())
}

func Test_logServerRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr := startPlank(ctx, t)

	ls := new(logServer)
	ls.init(ctx, addr)
	data := make([][]byte, 200)
	for i := range data {
		data[i] = []byte{byte(i)}
	}
	indexes := plankAppend(ctx, t, ls.client, data)

	// read the entire set of log entries just written
	entriesCh := make(chan logspec.Entry, len(data))
	stopCh := make(chan struct{})
	inc := mockIncrementer{}
	err := ls.read(&inc, indexes[0], indexes[len(indexes)-1], entriesCh, stopCh)
	assert.NoError(t, err)
	dataIndex := 0
	for e := range entriesCh {
		assert.True(t, bytes.Equal(data[dataIndex], e.Data))
		assert.EqualValues(t, e.Index, indexes[dataIndex])
		dataIndex++
	}
	assert.EqualValues(t, 200, inc.pos)

	// test reading of a subset of the log
	entriesCh = make(chan logspec.Entry, len(data))
	stopCh = make(chan struct{})
	inc.pos = 0
	err = ls.read(&inc, indexes[10], indexes[20], entriesCh, stopCh)
	assert.NoError(t, err)
	dataIndex = 10
	for e := range entriesCh {
		assert.True(t, bytes.Equal(data[dataIndex], e.Data))
		assert.EqualValues(t, e.Index, indexes[dataIndex])
		dataIndex++
	}
	assert.EqualValues(t, 20-10+1, inc.pos)
}

type mockIncrementer struct {
	pos int
}

func (m *mockIncrementer) Increment() int {
	m.pos++
	return m.pos
}

// startPlank starts the plank server on a random port, and returns the
// host:port that it was successfully started on. When the function returns the
// plank server is alive and ready to accept requests. If after a number of
// attempts it fails to start the plank server, it'll log a fatal test failure
// on the supplied 't' and return "". The started server will be kill when the
// supplied context is cancelled.
func startPlank(ctx context.Context, t *testing.T) string {
	plank := "../../../../../../bin/plank"
	try := func() string {
		addr := fmt.Sprintf("localhost:%d", rand.Int31n(5000)+21000)
		cmd := exec.CommandContext(ctx, plank, "-limit", "0", "-address", addr)
		out, err := cmd.StderrPipe()
		require.NoError(t, err)
		require.NoError(t, cmd.Start())
		// plank reports either 'Could not start sever ...' or 'plank server started...'
		r := bufio.NewScanner(out)
		for r.Scan() {
			line := r.Text()
			if strings.Contains(line, "Could not start sever") {
				return ""
			}
			if strings.Contains(line, "plank server started") {
				return addr
			}
		}
		t.Fatalf("plank process unexpectedly died")
		return ""
	}
	for tries := 0; tries < 5; tries++ {
		addr := try()
		if addr == "" {
			continue
		}
		return addr
	}
	t.Fatalf("Exhausted attempts to start plank server")
	return ""
}

func plankAppend(ctx context.Context, t *testing.T, c logspec.LogClient, proposals [][]byte) []uint64 {
	appender, err := c.Append(ctx)
	require.NoError(t, err)
	req := logspec.AppendRequest{
		Sequence:  1,
		Proposals: proposals,
	}
	require.NoError(t, appender.Send(&req))
	res, err := appender.Recv()
	require.NoError(t, err)
	indexes := res.GetOk().Indexes
	assert.Len(t, indexes, len(proposals))
	return indexes
}
