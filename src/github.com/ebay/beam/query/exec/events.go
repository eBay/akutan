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

package exec

import (
	"time"

	"github.com/ebay/beam/query/planner/plandef"
	"github.com/ebay/beam/util/clocks"
)

// Events receives callbacks about the progress of the query execution.
// Methods in the interface can be called concurrently by the execution engine,
// implementations of this interface must be concurrent safe.
type Events interface {
	// OpCompleted is called when an Operator has finished execution (even in
	// error cases). The event parameter contains a summary of information about
	// the execution.
	OpCompleted(event OpCompletedEvent)
	// Clocks will be called to obtain a time source that can be used for timing
	// the execution.
	Clock() clocks.Source
}

// OpCompletedEvent contains the collected data about a single execution of an
// operator. All these fields are populated by exec before it calls the
// OpCompleted method.
type OpCompletedEvent struct {
	// The definition of the Operator that was executed. A single query execution
	// may generate multiple OpCompleted events for the same Operator. For
	// example because it's the right side input to a loop join.
	Operator plandef.Operator
	// The number of bulk input rows that were executed.
	InputBulkCount uint32
	// When the operator started execution.
	StartedAt time.Time
	// When the operator completed execution.
	EndedAt time.Time
	// resulting output Chunk/Row counts.
	Output StreamStats
	// if set, the execution failed with an error.
	Err error
}

// StreamStats contains basic stats about a particular operator output stream
type StreamStats struct {
	NumChunks       int
	NumFactSets     int
	FinalStatistics FinalStatistics
}

// ignoreEvents is an implementation of Events that ignores the callbacks.
type ignoreEvents struct {
}

func (ignoreEvents) OpCompleted(event OpCompletedEvent) {
}

func (ignoreEvents) Clock() clocks.Source {
	return clocks.Wall
}
