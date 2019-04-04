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

package viewclient

import (
	metricsutil "github.com/ebay/beam/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type viewClientMetrics struct {
	diagnosticsStatsLatencySeconds prometheus.Summary
}

var metrics viewClientMetrics

func init() {
	mr := metricsutil.Registry{R: prometheus.DefaultRegisterer}
	metrics = viewClientMetrics{
		diagnosticsStatsLatencySeconds: mr.NewSummary(prometheus.SummaryOpts{
			Namespace:  "beam",
			Subsystem:  "viewclient",
			Name:       "diagnostics_stats_latency_seconds",
			Help:       `The time it takes to collect general service statistics from all the views.`,
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
		}),
	}
}
