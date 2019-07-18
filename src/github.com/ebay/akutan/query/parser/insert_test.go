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

package parser

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ParseInsert(t *testing.T) {
	filepath.Walk("testdata/insert", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		switch filepath.Ext(path) {
		case ".tsv":
			// path includes the leading testdata directory, so strip that off for the test name
			t.Run(strings.TrimPrefix(path, "testdata/insert/"), func(t *testing.T) {
				runInsertFileTest(t, path, InsertFormatTSV)
			})
		}
		return nil
	})
}

func runInsertFileTest(t *testing.T, inputFile string, format string) {
	input, err := ioutil.ReadFile(inputFile)
	require.NoError(t, err)
	var actual strings.Builder
	ast, err := ParseInsert(format, string(input))
	if err == nil {
		actual.WriteString("Parsed:\n")
		actual.WriteString(ast.String())
		actual.WriteString("\n\nParsed Details:\n")
		actual.WriteString(dump(ast))
	} else {
		actual.WriteString(err.Error())
	}

	// Compare actual to expected,
	resultsFile := strings.TrimSuffix(inputFile, filepath.Ext(inputFile)) + ".res"
	exp, err := ioutil.ReadFile(resultsFile)
	assert.NoError(t, err)
	if assert.Equal(t, string(exp), actual.String()) {
		return
	}

	// Update the file if it's out of date, so you'll see a git diff if you've
	// affected the parsing.
	t.Errorf("Results file missing or changed, writing new output to '%s'", resultsFile)
	err = ioutil.WriteFile(resultsFile, []byte(actual.String()), 0644)
	assert.NoError(t, err)
}

func Test_ParseInsert_badFormat(t *testing.T) {
	_, err := ParseInsert("qqq", "<a> <b> <c>")
	assert.EqualError(t, err, "parser: unsupported input format: qqq")
}
