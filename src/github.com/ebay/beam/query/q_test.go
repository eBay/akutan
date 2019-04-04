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

package query

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/ebay/beam/blog"
	"github.com/ebay/beam/logentry"
	"github.com/ebay/beam/logentry/logread"
	wellknown "github.com/ebay/beam/msg/facts"
	"github.com/ebay/beam/query/exec"
	"github.com/ebay/beam/query/parser"
	"github.com/ebay/beam/query/planner"
	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/clocks"
	"github.com/ebay/beam/util/cmp"
	"github.com/ebay/beam/util/parallel"
	"github.com/ebay/beam/viewclient/mockstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test_QueryEngine is an integration test for the query engine. It executes
// queries end to end (parse, plan, execute), and verifies the results. Queries
// are executed against an in memory view. The tests are driven from files in
// the testdata directory. You can add additional tests by creating more sets of
// files there.
//
// Each test should be in its own directory, you can nest directories as needed.
// A test consists of 4 files, query, input, results & debug
//
// input.facts contains a list of facts to execute the query against. Each line
// in the file should be a fact triple using string identifiers.
//
// query.bql contains the text of the query to execute. It expects the exact
// same format as the query API. (i.e. as parsed by BeamQL).
//
// The expected results are contained in a file named results.<type>. Type can
// be either error, table or facts.
//
// results.error contains the expected error reported when the query is executed.
//
// results.table contains the expected results, it should be a whitespace
// separated set of columns with the first row containing the column names. The
// column order should match the query output.
//
// results.facts contains the expected results. This should contain triples in
// the same format as input.facts with line breaks signifying each FactSet
// grouping. Order in the results files does not matter, both the order of the
// FactSets, and the order of facts in each FactSet. This supports the legacy
// query results format.
//
// debug.out contains the query report output for this query execution. If the
// debug report does not match this, the test will fail, and an updated version
// of the report written to the file. You can then review these diffs as part of
// the commit.
//
// Each test case is executed both with the query debug enabled, and disabled.
func Test_QueryEngine(t *testing.T) {
	err := filepath.Walk("testdata", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, "query.bql") {
			// path includes the leading testdata directory, so strip that off for the test name
			t.Run(strings.TrimSuffix(strings.TrimPrefix(path, "testdata/"), "/query.bql"), func(t *testing.T) {
				runQueryTest(t, filepath.Dir(path))
			})
		}
		return nil
	})
	assert.NoError(t, err)
}

func runQueryTest(t *testing.T, testDir string) {
	queryText, err := ioutil.ReadFile(filepath.Join(testDir, "query.bql"))
	require.NoError(t, err)
	srcDB, err := loadTestData(filepath.Join(testDir, "input.facts"))
	require.NoError(t, err)
	srcData := srcDB.Current()
	expResults, err := loadErrorResult(filepath.Join(testDir, "results.error"))
	if os.IsNotExist(err) {
		expResults, err = loadTableTestResults(filepath.Join(testDir, "results.table"), srcData)
		if os.IsNotExist(err) {
			expResults, err = loadLegacyTestResults(filepath.Join(testDir, "results.facts"), srcData)
		}
	}
	require.NoError(t, err)

	queryFormat := parser.QuerySparql
	if strings.Contains(testDir, "/legacy/") {
		queryFormat = parser.QueryFactPattern
	}

	t.Run("Debug", func(t *testing.T) {
		report := strings.Builder{}
		opts := Options{
			Debug:    true,
			DebugOut: &report,
			Clock:    clocks.NewMock(),
			Format:   queryFormat,
		}
		assertQueryEqual(t, string(queryText), srcData, opts, expResults)
		// We ignore err here because we want the test to generate the
		// .debug file if its missing to make it easier to add new test
		// cases.
		expDebug, _ := ioutil.ReadFile(filepath.Join(testDir, "debug.out"))
		if !assert.Equal(t, string(expDebug), report.String()) {
			err := ioutil.WriteFile(filepath.Join(testDir, "debug.out"), []byte(report.String()), 0644)
			assert.NoError(t, err)
		}
	})
	t.Run("NoDebug", func(t *testing.T) {
		opts := Options{
			Format: queryFormat,
		}
		assertQueryEqual(t, string(queryText), srcData, opts, expResults)
	})

}

func assertQueryEqual(t *testing.T, query string, srcData *mockstore.Snapshot, opts Options, verifier resultsVerifier) {
	statsProvider := func(context.Context) (planner.Stats, error) {
		return srcData.Stats(), nil
	}
	engine := New(statsProvider, srcData.Lookups())
	queryRes := ResultChunk{}
	resCh := make(chan ResultChunk, 4)
	wait := parallel.Go(func() {
		for c := range resCh {
			if queryRes.Columns == nil {
				// update our local ResultChunk that we're accumulating all the
				// result into with the column list.
				queryRes.Columns = c.Columns
			} else {
				// resultChunks are supposed to get the same columns in the same
				// order for any single query invocation.
				assert.Equal(t, queryRes.Columns, c.Columns)
			}
			queryRes.Values = append(queryRes.Values, c.Values...)
			queryRes.Facts = append(queryRes.Facts, c.Facts...)
		}
	})
	err := engine.Query(context.Background(), blog.Index(123456), query, opts, resCh)
	wait()
	verifier(t, err, queryRes)
}

// loadTestData loads in a set of facts from the supplied file. The file
// contents should consist of 3 columns whitespace separated. The columns are
// string names for subject, predicate, object. The Object should use the same
// KGObject format used by the query parser. Comment lines start with #. e.g.
//
//      motors:F150 <make>     <Ford>
//      motors:F150 <engine_l> 6.2
//
// InstanceOf & ExternalID facts will be automatically created as needed.
func loadTestData(filename string) (*mockstore.DB, error) {
	input, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	parsed, err := parser.ParseInsert(parser.InsertFormatTSV, string(input))
	if err != nil {
		return nil, err
	}
	db := mockstore.NewDB()
	nextKID := db.SkipIndex()*logread.MaxOffset + 1
	resolveOrCreateXID := func(xid string) uint64 {
		kid := db.Current().ResolveXID(xid)
		if kid != 0 {
			return kid
		}
		kid = nextKID
		nextKID++
		db.AddSPO(kid, wellknown.HasExternalID, rpc.AString(xid, 0))
		return kid
	}
	for _, line := range parsed.Facts {
		spo, err := makeFact(line, resolveOrCreateXID)
		if err != nil {
			return nil, fmt.Errorf("Invalid test data line %s: %v", line, err)
		}
		db.AddSPO(spo.Subject, spo.Predicate, spo.Object)
	}
	return db, nil
}

// resultsVerifier is called by assertQueryEqual and is passed the results of
// executing the query. It should assert that the results are the expected
// results.
type resultsVerifier func(t *testing.T, queryError error, results ResultChunk)

// makeFact converts the supplied Quad into a Fact. It uses the given resolveXID
// function to get KIDs for any QName or Entity; resolveXID function must never
// return 0.
func makeFact(line *parser.Quad, resolveXID func(string) uint64) (rpc.Fact, error) {
	xidOf := func(node interface{}, field string) (string, error) {
		switch n := node.(type) {
		case *parser.Entity:
			return n.Value, nil
		case *parser.QName:
			return n.Value, nil
		default:
			return "", fmt.Errorf("value %s not a valid type for field %s", node, field)
		}
	}
	var res rpc.Fact
	subjectXID, err := xidOf(line.Subject, "Subject")
	if err != nil {
		return rpc.Fact{}, err
	}
	res.Subject = resolveXID(subjectXID)
	predicateXID, err := xidOf(line.Predicate, "Predicate")
	if err != nil {
		return rpc.Fact{}, err
	}
	res.Predicate = resolveXID(predicateXID)

	resolveUnitLanguageID := func(xid string) uint64 {
		if len(xid) > 0 {
			return resolveXID(xid)
		}
		return 0
	}
	switch t := line.Object.(type) {
	case *parser.LiteralBool:
		res.Object = rpc.ABool(t.Value, resolveUnitLanguageID(t.Unit.Value))
	case *parser.LiteralFloat:
		res.Object = rpc.AFloat64(t.Value, resolveUnitLanguageID(t.Unit.Value))
	case *parser.LiteralInt:
		res.Object = rpc.AInt64(t.Value, resolveUnitLanguageID(t.Unit.Value))
	case *parser.LiteralTime:
		res.Object = rpc.ATimestamp(t.Value,
			logentry.TimestampPrecision(t.Precision),
			resolveUnitLanguageID(t.Unit.Value))
	case *parser.LiteralString:
		res.Object = rpc.AString(t.Value, resolveUnitLanguageID(t.Language.Value))
	default:
		objectXID, err := xidOf(line.Object, "Object")
		if err != nil {
			return rpc.Fact{}, err
		}
		res.Object = rpc.AKID(resolveXID(objectXID))
	}
	return res, nil
}

// loadErrorResult returns a resultsVerifier that asserts that the query
// returned an error, and that the error message matches the contents of the
// file.
func loadErrorResult(filename string) (resultsVerifier, error) {
	msg, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return func(t *testing.T, err error, res ResultChunk) {
		assert.EqualError(t, err, strings.TrimSpace(string(msg)))
	}, nil
}

// loadTableTestResults reads the supplied file and builds the expected results
// from it. Entity, QName & Literal values should be specified the same as in
// the query language. The file should contain a set of whitespace separated
// columns, where the first row is the column names. e.g.
//
//  postcard    country
//  <postcard1> <england>
//  <postcard2> <paris>
//
// Comment lines can be included by having a # as the first character in the
// line, and blank lines will be ignored.
func loadTableTestResults(filename string, srcData *mockstore.Snapshot) (resultsVerifier, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	s := bufio.NewScanner(bufio.NewReader(f))
	row := 0
	exp := ResultChunk{
		Columns: make(exec.Columns, 0, 4),
	}
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		row++
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		if len(exp.Columns) == 0 {
			// column headers. The Fields func splits the input on unicode.isSpace boundaries
			for _, c := range strings.Fields(line) {
				exp.Columns = append(exp.Columns, &plandef.Variable{Name: c})
			}
			continue
		}
		cells := strings.Fields(line)
		if len(cells) != len(exp.Columns) {
			return nil, fmt.Errorf("row %d has %d columns, but expecting %d", row, len(cells), len(exp.Columns))
		}
		for i, cell := range cells {
			parsed, err := parser.ParseTerm(cell)
			if err != nil {
				return nil, fmt.Errorf("row %d column %d unable to parse '%s' into a valid term: %v", row, i, cell, err)
			}
			val := exec.Value{}

			resolveUnitLanguageID := func(xid string) uint64 {
				if len(xid) > 0 {
					return srcData.ResolveXID(xid)
				}
				return uint64(0)
			}
			switch t := parsed.(type) {
			case *parser.Variable:
				return nil, fmt.Errorf("row %d column %d unexpected variable %s found in results", row, i, t)
			case *parser.LiteralID:
				return nil, fmt.Errorf("row %d column %d unexpected literal ID %s found in results", row, i, t)
			case *parser.LiteralBool:
				val.KGObject = rpc.ABool(t.Value, resolveUnitLanguageID(t.Unit.Value))
				if len(t.Unit.Value) > 0 {
					val.SetUnitExtID(t.Unit.Value)
				}
			case *parser.LiteralFloat:
				val.KGObject = rpc.AFloat64(t.Value, resolveUnitLanguageID(t.Unit.Value))
				if len(t.Unit.Value) > 0 {
					val.SetUnitExtID(t.Unit.Value)
				}
			case *parser.LiteralInt:
				val.KGObject = rpc.AInt64(t.Value, resolveUnitLanguageID(t.Unit.Value))
				if len(t.Unit.Value) > 0 {
					val.SetUnitExtID(t.Unit.Value)
				}
			case *parser.LiteralTime:
				val.KGObject = rpc.ATimestamp(t.Value,
					logentry.TimestampPrecision(t.Precision),
					resolveUnitLanguageID(t.Unit.Value))
				if len(t.Unit.Value) > 0 {
					val.SetUnitExtID(t.Unit.Value)
				}
			case *parser.LiteralString:
				val.KGObject = rpc.AString(t.Value, resolveUnitLanguageID(t.Language.Value))
				val.SetLangExtID(t.Language.Value)
			case *parser.QName:
				val.KGObject = rpc.AKID(srcData.ResolveXID(t.Value))
				val.ExtID = t.String()
			case *parser.Entity:
				val.KGObject = rpc.AKID(srcData.ResolveXID(t.Value))
				val.ExtID = t.String()
			default:
				return nil, fmt.Errorf("row %d column %d unexpected value type %T %s in results", row, i, t, t)
			}
			exp.Values = append(exp.Values, val)
		}
	}
	format := func(r ResultChunk) []string {
		formatted := make([]string, r.NumRows()+1)
		formatted[0] = r.Columns.String()
		for i := 0; i < r.NumRows(); i++ {
			formatted[i+1] = fmt.Sprintf("%v", r.Row(i))
		}
		return formatted
	}
	verify := func(t *testing.T, queryError error, actual ResultChunk) {
		assert.NoError(t, queryError)
		assert.Equal(t, format(exp), format(actual))
	}
	return verify, nil
}

// loadLegacyTestResults reads the supplied file and builds the expected
// results from it. Facts are specified in the same format as the input data,
// blank line should be used to group facts into FactSets, e.g.
//
//  motors:f150 motors:make "Ford"
//  motors:f150 motors:model "F150"
//
//  motors:focus motors:make "Ford"
//  motors:focus motors:model "Focus"
//
// indicates that are 2 expected FactSets containing 2 Facts each.
//
func loadLegacyTestResults(filename string, srcData *mockstore.Snapshot) (resultsVerifier, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	s := bufio.NewScanner(bufio.NewReader(f))
	row := 0
	exp := ResultChunk{}
	current := exec.FactSet{}
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		row++
		if len(line) == 0 || line[0] == '#' {
			if len(current.Facts) > 0 {
				exp.Facts = append(exp.Facts, current)
				current = exec.FactSet{}
			}
			continue
		}
		parsed, err := parser.Parse(parser.QueryFactPattern, line)
		if err != nil {
			return nil, err
		}
		fact, err := makeFact(parsed.Where[0], srcData.MustResolveXID)
		if err != nil {
			return nil, fmt.Errorf("file %s line %d: %s", filename, row, err)
		}
		current.Facts = append(current.Facts, fact)
	}
	if len(current.Facts) > 0 {
		exp.Facts = append(exp.Facts, current)
	}
	verify := func(t *testing.T, queryError error, actual ResultChunk) {
		assert.NoError(t, queryError)
		assert.Equal(t, len(exp.Facts), len(actual.Facts), "Results contains different number of rows")
		sortResultChunk(exp)
		sortResultChunk(actual)
		for i := 0; i < cmp.MinInt(len(exp.Facts), len(actual.Facts)); i++ {
			expFacts := exp.Facts[i]
			actFacts := actual.Facts[i]
			assert.Equal(t, len(expFacts.Facts), len(actFacts.Facts),
				"Result at index %d has different number of facts", i)
		}
		assert.Equal(t, pretty(exp, srcData), pretty(actual, srcData), "Query results differ")
	}
	return verify, nil
}

// sortResultChunk sorts the Rows and the Facts in each row into a deterministic order.
func sortResultChunk(r ResultChunk) {
	for i := range r.Facts {
		fs := r.Facts[i].Facts
		sort.Slice(fs, func(i, j int) bool {
			return factLess(&fs[i], &fs[j])
		})
	}
	sort.Slice(r.Facts, func(i, j int) bool {
		return factLess(&r.Facts[i].Facts[0], &r.Facts[j].Facts[0])
	})
}

// factLess return true if a is less than b, by comparing subject, predicate,
// object and ID fields, in that order.
func factLess(a, b *rpc.Fact) bool {
	if a.Subject == b.Subject {
		if a.Predicate == b.Predicate {
			if a.Object.Equal(b.Object) {
				return a.Id < b.Id
			}
			return a.Object.Less(b.Object)
		}
		return a.Predicate < b.Predicate
	}
	return a.Subject < b.Subject
}

// pretty returns a human readable string version of the ResultChunk, using
// externalIDs where possible.
func pretty(c ResultChunk, snap *mockstore.Snapshot) string {
	formatKID := func(kid uint64) string {
		xid := snap.ExternalID(kid)
		return fmt.Sprintf("#%d:%s", kid, xid)
	}
	s := strings.Builder{}
	for rowIdx, row := range c.Facts {
		for _, f := range row.Facts {
			fmtObj := f.Object.String()
			if f.Object.IsType(rpc.KtKID) {
				fmtObj = formatKID(f.Object.ValKID())
			}
			fmt.Fprintf(&s, "row:%3d    subject:%-20s  predicate:%-20s  object:%-20s\n",
				rowIdx, formatKID(f.Subject), formatKID(f.Predicate), fmtObj)
		}
		s.WriteByte('\n')
	}
	return s.String()
}
