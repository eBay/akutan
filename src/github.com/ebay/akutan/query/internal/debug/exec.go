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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ebay/akutan/query/exec"
	"github.com/ebay/akutan/query/planner/plandef"
	"github.com/ebay/akutan/util/bytes"
	"github.com/ebay/akutan/util/clocks"
	"github.com/ebay/akutan/util/cmp"
)

// execEvents implements qexec.Events, capturing OpCompleted events. It
// generates a version of the query plan that shows the timing/results size info
// collected from these events.
type execEvents struct {
	plan   *plandef.Plan
	clock  clocks.Source
	lock   sync.Mutex // protects locked
	locked struct {
		events []exec.OpCompletedEvent
	}
}

func newExecEvents(p *plandef.Plan, clock clocks.Source) *execEvents {
	e := execEvents{
		plan:  p,
		clock: clock,
	}
	e.locked.events = make([]exec.OpCompletedEvent, 0, 8)
	return &e
}

// OpCompleted implements the qexec.Events interface
func (e *execEvents) OpCompleted(event exec.OpCompletedEvent) {
	e.lock.Lock()
	e.locked.events = append(e.locked.events, event)
	e.lock.Unlock()
}

// Clock implements the qexec.Events interface
func (e *execEvents) Clock() clocks.Source {
	return e.clock
}

// dump writes a textual summary of the executed plan to the supplied
// StringWriter. It contains a line for each operator in the plan, with summary
// information about what that operator did.
func (e *execEvents) dump(w bytes.StringWriter) {
	e.lock.Lock()
	defer e.lock.Unlock()
	// maxLen returns the length of the longest operator description from the
	// plan, including padding.
	var maxLen func(depth int, p *plandef.Plan) int
	maxLen = func(depth int, p *plandef.Plan) int {
		l := depth*4 + len(p.Operator.String())
		for _, i := range p.Inputs {
			l = cmp.MaxInt(l, maxLen(depth+1, i))
		}
		return l
	}
	maxOpLen := maxLen(0, e.plan) + 1
	var writeOp func(depth int, p *plandef.Plan)
	writeOp = func(depth int, p *plandef.Plan) {
		fmt.Fprintf(w, "%s%v%s %s\n",
			strings.Repeat(" ", depth*4),
			p.Operator,
			strings.Repeat(" ", maxOpLen-(depth*4)-len(p.Operator.String())),
			opTotals(p.Operator, e.locked.events))

		for _, input := range p.Inputs {
			writeOp(depth+1, input)
		}
	}
	writeOp(0, e.plan)
}

// opTotals returns a string with the an aggregate summary of the events for the
// supplied operator.
func opTotals(op plandef.Operator, events []exec.OpCompletedEvent) string {
	var duration time.Duration
	var executions int
	var inputRows int
	var output exec.StreamStats

	for _, event := range events {
		if event.Operator == op {
			duration += event.EndedAt.Sub(event.StartedAt)
			executions++
			inputRows += int(event.InputBulkCount)
			output.NumChunks += event.Output.NumChunks
			output.NumFactSets += event.Output.NumFactSets
			output.FinalStatistics = event.Output.FinalStatistics
		}
	}

	if executions == 0 {
		return "[not executed]"
	}
	avg := ""
	if executions > 1 {
		avg = fmt.Sprintf(" (avg exec %v)",
			(duration / time.Duration(executions)).Round(time.Millisecond))
	}
	totalResultSize := ""
	zeroVal := exec.FinalStatistics{}
	if output.FinalStatistics != zeroVal {
		totalResultSize = fmt.Sprintf(" | total result size: %6d", output.FinalStatistics.TotalResultSize)
	}
	return fmt.Sprintf("execs:%4d | totals: | input rows:%4d | out chunks:%4d | out factsets:%6d | took %6v%s%s",
		executions, inputRows, output.NumChunks, output.NumFactSets,
		duration.Round(time.Millisecond), avg, totalResultSize)
}
