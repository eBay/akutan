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
	metricsutil "github.com/ebay/beam/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type queryMetrics struct {
	parseQueryDurationSeconds   prometheus.Summary
	rewriteQueryDurationSeconds prometheus.Summary
	planQueryDurationSeconds    prometheus.Summary
	executeQueryDurationSeconds prometheus.Summary
}

var metrics queryMetrics

func init() {
	mr := metricsutil.Registry{R: prometheus.DefaultRegisterer}
	metrics = queryMetrics{
		parseQueryDurationSeconds: mr.NewSummary(prometheus.SummaryOpts{
			Namespace:  "beam",
			Subsystem:  "query",
			Name:       "parse_duration_seconds",
			Help:       `The time it takes to parse a query.`,
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
		}),
		rewriteQueryDurationSeconds: mr.NewSummary(prometheus.SummaryOpts{
			Namespace: "beam",
			Subsystem: "query",
			Name:      "rewrite_duration_seconds",
			Help: `The time it takes to rewrite a query.

This step happens after parsing and before planning, and it includes fetching
numeric IDs for any external IDs used in the query.
`,
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
		}),
		planQueryDurationSeconds: mr.NewSummary(prometheus.SummaryOpts{
			Namespace: "beam",
			Subsystem: "query",
			Name:      "planning_duration_seconds",
			Help: `The time it takes to come up with a query plan.
			
Query planning is a CPU-intensive process that considers many possible execution plans and picks
the one with the lowest estimated cost.

These observations are expected to vary significantly from one query to the
next, but a major shift in the overall distributions would indicate a change in
usage or a change in the code.
`,
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
		}),
		executeQueryDurationSeconds: mr.NewSummary(prometheus.SummaryOpts{
			Namespace: "beam",
			Subsystem: "query",
			Name:      "execute_duration_seconds",
			Help: `The time it takes to execute a query.

This happens after query planning, and it involves fetching the data from the
views.
`,
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
		}),
	}
}
