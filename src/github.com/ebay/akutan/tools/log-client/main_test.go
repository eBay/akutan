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
	"bytes"
	"context"
	"errors"
	"testing"

	docopt "github.com/docopt/docopt-go"
	"github.com/ebay/akutan/blog/mockblog"
	"github.com/ebay/akutan/discovery"
	"github.com/ebay/akutan/util/clocks"
	"github.com/stretchr/testify/assert"
)

func Test_parseArgs(t *testing.T) {
	var tests = []struct {
		name         string
		inputArgv    []string
		expValidArgs bool
		expOpts      options
	}{
		{
			name:         "info",
			inputArgv:    []string{"info"},
			expValidArgs: true,
			expOpts: options{
				Servers: []string{"localhost:20011"},
				endpoints: []*discovery.Endpoint{
					{Network: "tcp", Host: "localhost", Port: "20011"},
				},
				Info:       true,
				StartIndex: 1,
				MaxErrors:  9999,
			},
		}, {
			name:         "info_with_server",
			inputArgv:    []string{"info", "log:20000"},
			expValidArgs: true,
			expOpts: options{
				Servers: []string{"log:20000"},
				endpoints: []*discovery.Endpoint{
					{Network: "tcp", Host: "log", Port: "20000"},
				},
				Info:       true,
				StartIndex: 1,
				MaxErrors:  9999,
			},
		}, {
			name:         "info_with_servers",
			inputArgv:    []string{"info", "log01:20010", "log02:20020", "log03:20030"},
			expValidArgs: true,
			expOpts: options{
				Servers: []string{"log01:20010", "log02:20020", "log03:20030"},
				endpoints: []*discovery.Endpoint{
					{Network: "tcp", Host: "log01", Port: "20010"},
					{Network: "tcp", Host: "log02", Port: "20020"},
					{Network: "tcp", Host: "log03", Port: "20030"},
				},
				Info:       true,
				StartIndex: 1,
				MaxErrors:  9999,
			},
		}, {
			name:         "append_version",
			inputArgv:    []string{"appendVersion", "--force", "1"},
			expValidArgs: true,
			expOpts: options{
				Servers: []string{"localhost:20011"},
				endpoints: []*discovery.Endpoint{
					{Network: "tcp", Host: "localhost", Port: "20011"},
				},
				AppendVersion: true,
				Force:         true,
				Version:       1,
				StartIndex:    1,
				MaxErrors:     9999,
			},
		},
		{
			name:         "append_version_no_force",
			inputArgv:    []string{"appendVersion", "1"},
			expValidArgs: false,
		},
		{
			name:         "append_version_no_version",
			inputArgv:    []string{"appendVersion", "--force"},
			expValidArgs: false,
		},
		{
			name:         "discard_prefix",
			inputArgv:    []string{"discardPrefix", "--force", "100"},
			expValidArgs: true,
			expOpts: options{
				Servers: []string{"localhost:20011"},
				endpoints: []*discovery.Endpoint{
					{Network: "tcp", Host: "localhost", Port: "20011"},
				},
				DiscardPrefix:    true,
				Force:            true,
				FirstIndexString: "100",
				FirstIndex:       uint64(100),
				StartIndex:       1,
				MaxErrors:        9999,
			},
		},
		{
			name:         "discard_prefix_no_force",
			inputArgv:    []string{"discardPrefix", "100"},
			expValidArgs: false,
		},
		{
			name:         "discard_prefix_no_index",
			inputArgv:    []string{"discardPrefix", "--force"},
			expValidArgs: false,
		}, {
			name:         "diff",
			inputArgv:    []string{"diff", ":20011", ":20021"},
			expValidArgs: true,
			expOpts: options{
				Servers: []string{":20011", ":20021"},
				endpoints: []*discovery.Endpoint{
					{Network: "tcp", Host: "", Port: "20011"},
					{Network: "tcp", Host: "", Port: "20021"},
				},
				Diff:       true,
				StartIndex: 1,
				MaxErrors:  9999,
			},
		}, {
			name:         "diff_start_end",
			inputArgv:    []string{"diff", "-s", "10", "-e", "1000", ":20011", ":20021"},
			expValidArgs: true,
			expOpts: options{
				Servers: []string{":20011", ":20021"},
				endpoints: []*discovery.Endpoint{
					{Network: "tcp", Host: "", Port: "20011"},
					{Network: "tcp", Host: "", Port: "20021"},
				},
				Diff:             true,
				StartIndexString: "10",
				StartIndex:       10,
				EndIndexString:   "1000",
				EndIndex:         1000,
				MaxErrors:        9999,
			},
		}, {
			name:         "diff_max_errors",
			inputArgv:    []string{"diff", "-m", "3", ":20011", ":20021"},
			expValidArgs: true,
			expOpts: options{
				Servers: []string{":20011", ":20021"},
				endpoints: []*discovery.Endpoint{
					{Network: "tcp", Host: "", Port: "20011"},
					{Network: "tcp", Host: "", Port: "20021"},
				},
				Diff:       true,
				StartIndex: 1,
				MaxErrors:  3,
			},
		}, {
			name:         "unknown_command",
			inputArgv:    []string{"unknown"},
			expValidArgs: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var parseErr error
			docopt.DefaultParser.HelpHandler = func(err error, usage string) {
				parseErr = err
			}
			opts, err := parseArgs(test.inputArgv)
			if !test.expValidArgs {
				assert.Error(t, err)
				assert.Error(t, parseErr)
				return
			}
			assert.NoError(t, err)
			assert.NoError(t, parseErr)
			if assert.NotNil(t, opts) {
				assert.Equal(t, test.expOpts, *opts)
			}
		})
	}
}

func Test_info(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	log := mockblog.New(ctx)
	clock = clocks.NewMock()

	b := &bytes.Buffer{}
	err := info(ctx, log, b)
	assert.NoError(err)
	assert.Equal(`
Invoking Info

Got Info Reply
FirstIndex:          1
LastIndex:           0
BytesUsed:          0B
BytesTotal:         0B

Info took 0s
`, b.String())

	log.Append(ctx, [][]byte{[]byte("hello"), []byte("world")})
	b = &bytes.Buffer{}
	info(ctx, log, b)
	assert.NoError(err)
	assert.Equal(`
Invoking Info

Got Info Reply
FirstIndex:          1
LastIndex:           2
BytesUsed:         20B
BytesTotal:        40B

Info took 0s
`, b.String())

	log.Discard(ctx, 1000000000)
	b = &bytes.Buffer{}
	info(ctx, log, b)
	assert.NoError(err)
	assert.Equal(`
Invoking Info

Got Info Reply
FirstIndex: 1000000000
LastIndex:   999999999
BytesUsed:          0B
BytesTotal:         0B

Info took 0s
`, b.String())

	clock = clocks.Wall
}

func Test_formatBytes(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("0B", formatBytes(0))
	assert.Equal("128B", formatBytes(128))
	assert.Equal("1536B", formatBytes(1024+512))
	assert.Equal("11.5KB", formatBytes(10240+1024+512))
	assert.Equal("15.0MB", formatBytes(1024*1024*15))
	assert.Equal("16.0GB", formatBytes(1024*1024*1024*16))
	assert.Equal("1024.0GB", formatBytes(1024*1024*1024*1024))
	assert.Equal("11.0TB", formatBytes(1024*1024*1024*1024*11))
}

func Test_appendVersion(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	log := mockblog.New(ctx)
	clock = clocks.NewMock()

	b := &bytes.Buffer{}
	err := appendVersion(ctx, log, int32(5), b)
	assert.NoError(err)
	assert.Equal(`
Invoking Append (VersionCommand with MoveToVersion 5)
Appended VersionCommand with version 5 at index 1
AppendVersion took 0s
`, b.String())

	res, _ := log.Info(ctx)
	assert.Equal(uint64(1), res.LastIndex)

	b = &bytes.Buffer{}
	log.SetNextAppendError(errors.New("artificial error"))
	err = appendVersion(ctx, log, int32(6), b)
	assert.EqualError(err, "artificial error")
	clock = clocks.Wall
}

func Test_discardPrefix(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	log := mockblog.New(ctx)
	clock = clocks.NewMock()

	b := &bytes.Buffer{}
	err := discardPrefix(ctx, log, uint64(10), b)
	assert.NoError(err)
	assert.Equal(`
Invoking Discard with firstIndex 10
Discard completed
Discard took 0s
`, b.String())

	res, _ := log.Info(ctx)
	assert.Equal(uint64(10), res.FirstIndex)
	assert.Equal(uint64(9), res.LastIndex)
	clock = clocks.Wall
}
