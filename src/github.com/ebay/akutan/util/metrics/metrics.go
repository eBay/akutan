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

// Package metrics aids in defining Prometheus metrics.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Registry encapsulates metrics creation and registration
type Registry struct {
	R prometheus.Registerer
}

// NewCounter returns a new created and registered Prometheus Counter
func (mr Registry) NewCounter(c prometheus.CounterOpts) prometheus.Counter {
	pm := prometheus.NewCounter(c)
	mr.R.MustRegister(pm)
	return pm
}

// NewGauge returns a new created and registered Prometheus Gauge
func (mr *Registry) NewGauge(g prometheus.GaugeOpts) prometheus.Gauge {
	pm := prometheus.NewGauge(g)
	mr.R.MustRegister(pm)
	return pm
}

// NewSummary returns a new and registered Prometheus Summary
func (mr *Registry) NewSummary(s prometheus.SummaryOpts) prometheus.Summary {
	pm := prometheus.NewSummary(s)
	mr.R.MustRegister(pm)
	return pm
}

// NewHistogram returns a new and registered Prometheus Histogram
func (mr *Registry) NewHistogram(h prometheus.HistogramOpts) prometheus.Histogram {
	pm := prometheus.NewHistogram(h)
	mr.R.MustRegister(pm)
	return pm
}
