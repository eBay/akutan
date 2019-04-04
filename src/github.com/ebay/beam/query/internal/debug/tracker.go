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

package debug

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ebay/beam/blog"
	"github.com/ebay/beam/query/exec"
	"github.com/ebay/beam/query/parser"
	"github.com/ebay/beam/query/planner"
	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/query/planner/search"
	"github.com/ebay/beam/util/bytes"
	"github.com/ebay/beam/util/clocks"
	"github.com/ebay/beam/viewclient/lookups"
	"github.com/sirupsen/logrus"
)

// timestampFormat is used to format the timestamps written to the report.
const timestampFormat = "2006-01-02 15:04:05.000000 MST"

// Tracker defines points in the query processing sequence. The Query Engine
// will call these at the appropriate places in the processing.
type Tracker interface {
	Parsed(*parser.Query, error)
	Rewritten(*parser.Query, error)
	DecorateStats(planner.Stats) planner.Stats
	Planned(*search.Space, *plandef.Plan, error)
	DecorateLookups(lookups.All) lookups.All
	ExecEvents(plan *plandef.Plan) exec.Events
	Close()
}

// trackerID is used by New() to assign an Id to the query, via an atomic.Add.
// Nothing else should need to be reading or writing this.
var trackerID uint64

// New returns a new Tracker. The caller is expected to arrange for the various
// methods on Tracker to get called at the right time. It is unlikely that you
// need to create one of these unless you're working in the query package
// itself. If 'debug' is set the tracker will accumulate a detailed query report
// and write it to debugOut. If debugOut is nil, the report will be written to a
// file in $TMPDIR. If 'debug' is false, a no-op Tracker is returned.
func New(debug bool, debugOut io.Writer, clock clocks.Source, index blog.Index, query string) Tracker {
	if !debug {
		return noopTracker{}
	}
	if clock == nil {
		clock = clocks.Wall
	}
	t := &debugTracker{
		id:    atomic.AddUint64(&trackerID, 1),
		clock: clock,
	}
	if debugOut == nil {
		f, err := os.Create(filepath.Join(os.TempDir(), fmt.Sprintf("query_debug_%d", t.id)))
		if err != nil {
			logrus.Warnf("Unable to create query debug file: %v", err)
			return noopTracker{}
		}
		logrus.Infof("Query Debug Info %d being written to %s", t.id, f.Name())
		t.close = f
		debugOut = f
	}
	t.out = bufio.NewWriter(debugOut)
	t.started = t.clock.Now()
	fmt.Fprintf(&t.report.header, "Started at: %s\n", t.started.UTC().Format(timestampFormat))
	t.report.inputQuery = fmt.Sprintf("Query @ Index: %d\n%s", index, query)
	return t
}

// debugTracker implements the Tracker interface. It will generate a human
// readable query debug report containing diagnostic information about the query
// processing.
type debugTracker struct {
	id        uint64
	clock     clocks.Source
	started   time.Time
	parsed    time.Time
	rewritten time.Time
	planned   time.Time
	// after the rewrite is completed, the mapping of externalID to internal
	// KIDs is populated in here. The data is extracted from the beamql.Query
	externalIDs map[uint64]string
	// out is where the report will be written to.
	out *bufio.Writer
	// close if set will be closed once the report is written.
	close io.Closer
	// The created report contains the below sections, in the order you see.
	report struct {
		header     strings.Builder
		inputQuery string
		parsed     string
		rewritten  string
		planned    string
		stats      func(bytes.StringWriter)
		execution  func(bytes.StringWriter)
		lookups    func(bytes.StringWriter)
		planSpace  func(bytes.StringWriter)
	}
}

func (t *debugTracker) Parsed(q *parser.Query, err error) {
	t.parsed = t.clock.Now()
	fmt.Fprintf(&t.report.header, "Parsing   %v\n", t.parsed.Sub(t.started))
	if err != nil {
		t.report.parsed = fmt.Sprintf("Error: %v\n", err)
		return
	}
	t.report.parsed = q.String() + "\n"
}

func (t *debugTracker) Rewritten(q *parser.Query, err error) {
	t.rewritten = t.clock.Now()
	fmt.Fprintf(&t.report.header, "Rewriting %v\n", t.rewritten.Sub(t.parsed))
	if err != nil {
		t.report.rewritten = fmt.Sprintf("Error: %v\n", err)
		return
	}
	t.report.rewritten = q.String() + "\n"
	t.externalIDs = extractKIDs(q)
}

// extractKIDs will build a map of KID -> ExternalID for all fields in the query
// that have LiteralIDs. This relies on the parser filling out the Hint in the
// literalID.
func extractKIDs(query *parser.Query) map[uint64]string {
	r := make(map[uint64]string, len(query.Where))
	add := func(val interface{}) {
		if litID, ok := val.(*parser.LiteralID); ok {
			if litID.Hint != "" {
				r[litID.Value] = litID.Hint
			}
		}
	}
	for _, q := range query.Where {
		add(q.Subject)
		add(q.Predicate)
		add(q.Object)
	}
	return r
}

func (t *debugTracker) DecorateStats(s planner.Stats) planner.Stats {
	r := &plannerStats{impl: s, kids: t.externalIDs}
	t.report.stats = r.dump
	return r
}

func (t *debugTracker) Planned(space *search.Space, plan *plandef.Plan, err error) {
	t.planned = t.clock.Now()
	fmt.Fprintf(&t.report.header, "Planning  %v\n", t.planned.Sub(t.rewritten))
	if err != nil {
		t.report.planned = fmt.Sprintf("Error: %v\n", err)
		return
	}
	b := strings.Builder{}
	space.DebugCostedBest(&b)
	t.report.planned = b.String()
	t.report.planSpace = space.Debug
}

func (t *debugTracker) DecorateLookups(lookupSource lookups.All) lookups.All {
	lookupStats := queryLookupStats{impl: lookupSource, clock: t.clock}
	t.report.lookups = lookupStats.dump
	return &lookupStats
}

func (t *debugTracker) ExecEvents(plan *plandef.Plan) exec.Events {
	e := newExecEvents(plan, t.clock)
	t.report.execution = e.dump
	return e
}

func (t *debugTracker) Close() {
	end := t.clock.Now()
	t.out.WriteString(t.report.header.String())
	if !t.planned.IsZero() {
		fmt.Fprintf(t.out, "Executing %v\n", end.Sub(t.planned))
	}
	fmt.Fprintf(t.out, "Query Ended at: %s\n", end.UTC().Format(timestampFormat))
	fmt.Fprintf(t.out, "Total: %v\n\n", end.Sub(t.started))
	t.out.WriteString(t.report.inputQuery)
	t.out.WriteString("\nParsed Query:\n")
	t.out.WriteString(t.report.parsed)
	if t.report.rewritten != "" {
		t.out.WriteString("\nRewritten Query:\n")
		t.out.WriteString(t.report.rewritten)
	}
	if t.report.planned != "" {
		t.out.WriteString("\nSelected Plan:\n")
		t.out.WriteString(t.report.planned)
	}
	if t.report.stats != nil {
		t.out.WriteString("\nStatistics Used:\n")
		t.report.stats(t.out)
	}
	if t.report.execution != nil {
		t.out.WriteString("\nQuery Execution Summary:\n")
		t.report.execution(t.out)
	}
	if t.report.lookups != nil {
		t.out.WriteString("\nDiskview Lookups:\n")
		t.report.lookups(t.out)
	}
	if t.report.planSpace != nil {
		t.out.WriteString("\nPlan Space:\n")
		t.report.planSpace(t.out)
	}
	t.out.WriteByte('\n')

	flushErr := t.out.Flush()
	if flushErr != nil {
		logrus.WithFields(logrus.Fields{
			"query_id": t.id,
			"error":    flushErr,
		}).Warn("Error writing report for query")
	}
	// even if the flush failed, we should still try and close the ouput if
	// needed.
	if t.close != nil {
		closeErr := t.close.Close()
		if closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"query_id": t.id,
				"error":    closeErr,
			}).Warn("Error closing report for query")
			return
		}
	}
	if flushErr != nil {
		return
	}
	logrus.WithField("query_id", t.id).Info("Completed query debug report")
}

// noopTracker implements the Tracker interface, everything is effectively a
// no-op
type noopTracker struct {
}

func (noopTracker) Parsed(*parser.Query, error)    {}
func (noopTracker) Rewritten(*parser.Query, error) {}
func (noopTracker) DecorateStats(s planner.Stats) planner.Stats {
	return s
}
func (noopTracker) Planned(*search.Space, *plandef.Plan, error) {}
func (noopTracker) DecorateLookups(s lookups.All) lookups.All {
	return s
}
func (noopTracker) ExecEvents(plan *plandef.Plan) exec.Events {
	return nil
}
func (noopTracker) Close() {}
