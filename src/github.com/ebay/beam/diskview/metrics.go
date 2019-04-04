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

package diskview

import (
	metricsutil "github.com/ebay/beam/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type diskViewMetrics struct {
	lookupStallForLogEntriesSeconds prometheus.Summary
}

var metrics diskViewMetrics

func init() {
	mr := metricsutil.Registry{R: prometheus.DefaultRegisterer}
	metrics = diskViewMetrics{
		lookupStallForLogEntriesSeconds: mr.NewSummary(prometheus.SummaryOpts{
			Namespace: "beam",
			Subsystem: "diskview",
			Name:      "lookup_stall_for_log_entries_seconds",
			Help: `How long Lookup waits for the requested index to have been applied from the log.

This observation has two other edge cases. The first is timing out while waiting
for log to reach the requested index and the second is when the view
is already at (or after) the requested index.
`,
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
		}),
	}
}
