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

package impl

import (
	metricsutil "github.com/ebay/beam/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type apiMetrics struct {
	resolveIndexLatencySeconds prometheus.Summary
}

var metrics apiMetrics

func init() {
	mr := metricsutil.Registry{R: prometheus.DefaultRegisterer}
	metrics = apiMetrics{
		resolveIndexLatencySeconds: mr.NewSummary(prometheus.SummaryOpts{
			Namespace:  "beam",
			Subsystem:  "api",
			Name:       "resolve_index_latency_seconds",
			Help:       `The time it takes to fetch the latest linearizable index from the log.`,
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
		}),
	}
}
