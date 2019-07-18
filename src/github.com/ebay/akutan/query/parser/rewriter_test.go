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
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/ebay/akutan/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test_ParseRewriteResults runs parse / rewrite against test queries from the
// testdata directory. You can add new test cases by creating a new file in the
// testdata tree with a .bql extension and containing the query to parse. The
// test will run the query through parse and rewrite creating a dump of the AST
// after each step. Then it'll compare this against a results file, if the file
// is missing or the output is different the test will fail, and it'll write the
// updated data to the file. The results file have matching name as the bql file
// except with a .res extension.
//
func Test_ParseRewriteResults(t *testing.T) {
	filepath.Walk("testdata", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && info.Name() == "insert" {
			return filepath.SkipDir
		}
		if !info.IsDir() && filepath.Ext(path) == ".bql" {
			// path includes the leading testdata directory, so strip that off for the test name
			t.Run(strings.TrimPrefix(path, "testdata/"), func(t *testing.T) {
				runQueryFileTest(t, path)
			})
		}
		return nil
	})
}

func runQueryFileTest(t *testing.T, queryFile string) {
	queryText, err := ioutil.ReadFile(queryFile)
	require.NoError(t, err)

	actual := strings.Builder{}
	fmt.Fprintf(&actual, "Query:\n%s\nParsed:\n", queryText)

	// run parse on the test input, and if that succeeds, run rewrite on it as
	// well
	format := QuerySparql
	if strings.HasPrefix(queryFile, "testdata/legacy/") {
		format = QueryFactPattern
	}
	ast, err := Parse(format, string(queryText))
	if err != nil {
		fmt.Fprintf(&actual, "Error: %s\n", err)
	} else {
		parsedDetails := dump(ast)
		actual.WriteString(ast.String())
		actual.WriteString("\n\nRewritten:\n")

		rewriter := &rewriter{in: ast, lookup: &testLookups{}}
		err = rewriter.rewrite(context.Background())
		rewrittenDetails := ""
		if err != nil {
			fmt.Fprintf(&actual, "Error: %s\n", err)
		} else {
			rewrittenDetails = dump(ast)
			if rewrittenDetails == parsedDetails {
				actual.WriteString("Unchanged from parsed version\n")
				rewrittenDetails = ""
			} else {
				actual.WriteString(ast.String())
				actual.WriteByte('\n')
			}
		}
		actual.WriteString("\nParsed Details:\n")
		actual.WriteString(parsedDetails)
		if rewrittenDetails != "" {
			actual.WriteString("\nRewritten Details:\n")
			actual.WriteString(rewrittenDetails)
		}
		actual.WriteByte('\n')
	}

	// Compare actual to expected.
	resultsFile := strings.TrimSuffix(queryFile, ".bql") + ".res"
	exp, err := ioutil.ReadFile(resultsFile)
	assert.NoError(t, err)
	if assert.Equal(t, string(exp), actual.String()) {
		return
	}

	// Update the file if it's out of date, so you'll see a git diff if you've
	// affected the parsing/rewriting.
	t.Errorf("Results file missing or changed, writing new output to '%s'", resultsFile)
	err = ioutil.WriteFile(resultsFile, []byte(actual.String()), 0644)
	assert.NoError(t, err)
}

// dump uses reflection to serialize a deep structure as a human-readable string.
// This is used in Test_ParseRewriteResults to compare ASTs.
func dump(v interface{}) string {
	config := spew.NewDefaultConfig()
	config.DisableMethods = true
	config.DisablePointerMethods = true
	config.DisablePointerAddresses = true
	config.DisableCapacities = true
	config.SortKeys = true
	config.Indent = "  "
	return config.Sdump(v)
}

type testLookups struct{}

func (tl *testLookups) LookupPO(_ context.Context, req *rpc.LookupPORequest, ch chan *rpc.LookupChunk) error {
	defer close(ch)
	emit := func(offset int, fact *rpc.Fact) {
		if fact == nil {
			ch <- new(rpc.LookupChunk)
			return
		}
		res := &rpc.LookupChunk{
			Facts: []rpc.LookupChunk_Fact{
				{
					Lookup: uint32(offset),
					Fact:   *fact,
				},
			},
		}
		ch <- res
	}
	for i, reqItem := range req.Lookups {
		switch reqItem.Object.ValString() {
		case "brand":
			emit(i, &rpc.Fact{Id: 1, Subject: 2})
		case "color":
			emit(i, &rpc.Fact{Id: 13559708068, Subject: 13559708021})
		case "de":
			emit(i, &rpc.Fact{Id: 728989058, Subject: 728989018})
		case "products:bolts":
			emit(i, &rpc.Fact{Id: 13, Subject: 14})
		case "products:lugnuts":
			emit(i, &rpc.Fact{Id: 11, Subject: 12})
		case "products:rims":
			emit(i, &rpc.Fact{Id: 7, Subject: 8})
		case "products:tires":
			emit(i, &rpc.Fact{Id: 5, Subject: 6})
		case "products:vehicle":
			emit(i, &rpc.Fact{Id: 3, Subject: 4})
		case "en":
			emit(i, &rpc.Fact{Id: 728987058, Subject: 728987017})
		case "jpn":
			emit(i, &rpc.Fact{Id: 728987032, Subject: 728987004})
		case "m":
			emit(i, &rpc.Fact{Id: 15, Subject: 16})
		case "bob001_guid_f44862d7-3f37-4a31-b8fa-4d9e14edb771":
			emit(i, &rpc.Fact{Id: 502, Subject: 503})
		case "Samsung":
			emit(i, &rpc.Fact{Id: 10574672213, Subject: 10574672077})
		case "Samsung_(Complex)":
			emit(i, &rpc.Fact{Id: 10574672214, Subject: 10574672078})
		case "size":
			emit(i, &rpc.Fact{Id: 9, Subject: 10})
		case "rdfs:label":
			emit(i, &rpc.Fact{Id: 13542198001, Subject: 13542198000})
		case "rdfs:label_(complicated)":
			emit(i, &rpc.Fact{Id: 13542191001, Subject: 13542191000})
		case "rdf:type":
			emit(i, &rpc.Fact{Id: 13542198002, Subject: 13542198003})
		case "white":
			emit(i, &rpc.Fact{Id: 10863788237, Subject: 10863788102})
		case "xsd:date":
			emit(i, &rpc.Fact{Id: 809287001, Subject: 809287000})
		case "xsd:decimal":
			emit(i, &rpc.Fact{Id: 4045557001, Subject: 4045557000})
		case "xsd:integer":
			emit(i, &rpc.Fact{Id: 4045560001, Subject: 4045560000})
		case "Beyonc\u00e9":
			emit(i, &rpc.Fact{Id: 5045560001, Subject: 5045560000})
		default:
			emit(i, nil)
		}
	}
	return nil
}
