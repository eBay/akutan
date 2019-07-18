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

package debuglog

import (
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_Configure(t *testing.T) {
	tests := []struct {
		name     string
		options  Options
		contains []string
	}{
		{
			name:    "default",
			options: Options{},
			contains: []string{
				" level=info ",
				` msg="Initialized Logrus"`,
				" forceColors=false",
			},
		},
		{
			name:    "default/UTC_timestamp",
			options: Options{},
			contains: []string{
				` UTC"`,
			},
		},
		{
			name:    "default/relative_filenames",
			options: Options{},
			contains: []string{
				` file="src/github.com/ebay/akutan/util/debuglog/setup.go:`,
			},
		},
		{
			name: "forceColors",
			options: Options{
				ForceColors: true,
			},
			contains: []string{
				"\x1b[36mINFO\x1b[0m",
				"\x1b[36mforceColors\x1b[0m=true",
			},
		},
	}

	// Ensure CLICOLOR_FORCE isn't set, as it would cause the test to fail.
	value, isSet := os.LookupEnv("CLICOLOR_FORCE")
	if isSet {
		assert.NoError(t, os.Unsetenv("CLICOLOR_FORCE"))
		defer func() {
			assert.NoError(t, os.Setenv("CLICOLOR_FORCE", value))
		}()
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			logger := logrus.New()
			var buf strings.Builder
			logger.Out = &buf
			options := test.options
			options.Logger = logger
			Configure(options)
			output := buf.String()
			for _, needle := range test.contains {
				assert.Contains(output, needle, `
Go output: %#v
hex output: %#v
decimal output: %v
Go needle: %#v
hex needle: %#v
decimal needle: %v`,
					output, []byte(output), []byte(output),
					needle, []byte(needle), []byte(needle))
			}
		})
	}
}

func Test_filenameHook(t *testing.T) {
	assert := assert.New(t)
	_, thisFile, _, _ := runtime.Caller(0)
	tests := []struct {
		in       string
		expected string
	}{
		{
			in:       thisFile,
			expected: "src/github.com/ebay/akutan/util/debuglog/setup_test.go",
		},
		{
			in:       "/some/other/path",
			expected: "/some/other/path",
		},
	}
	hook := newFilenameHook()
	logger := logrus.New()
	logger.SetReportCaller(true)
	for _, test := range tests {
		entry := logrus.Entry{
			Logger: logger,
			Caller: &runtime.Frame{
				File: test.in,
			},
		}
		assert.True(entry.HasCaller(), "test should set up entry.Caller")
		assert.NoError(hook.Fire(&entry))
		assert.Equal(test.expected, entry.Caller.File, "input: %v", test.in)
	}
}
