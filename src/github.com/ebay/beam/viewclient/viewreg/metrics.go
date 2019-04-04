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

package viewreg

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// metrics contains statically-registered Prometheus metrics for the package.
var metrics = struct {
	allViews             prometheus.Gauge
	healthyViews         prometheus.Gauge
	additionsTotal       prometheus.Counter
	removalsTotal        prometheus.Counter
	changesTotal         prometheus.Counter
	lastChangedTimestamp prometheus.Gauge
}{
	allViews: promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "beam",
		Subsystem: "view_registry",
		Name:      "all_views",
		Help: `The number of view servers currently known to this registry.

This includes unhealthy servers.`,
	}),
	healthyViews: promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "beam",
		Subsystem: "view_registry",
		Name:      "healthy_views",
		Help: `The number of healthy view servers currently known to this registry.

A server is deemed healthy if the gRPC connection to that server is in the
READY state and the last Diagnostics.Stats RPC to that server was successful.`,
	}),
	additionsTotal: promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "beam",
		Subsystem: "view_registry",
		Name:      "additions_total",
		Help: `The cumulative number of views ever added to the registry.

If this counter increases quickly, it could be an indication of that the
locator to discover new views changes often.`,
	}),
	removalsTotal: promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "beam",
		Subsystem: "view_registry",
		Name:      "removals_total",
		Help: `The cumulative number of views ever removed from the registry.

If this counter increases quickly, it could be an indication of that the
locator to discover new views changes often.`,
	}),
	changesTotal: promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "beam",
		Subsystem: "view_registry",
		Name:      "changes_total",
		Help: `The cumulative number of updates to the registry.

A change is either an addition or a removal of a view or an update of a
view's endpoint, health, or other properties. If this counter increases
quickly, it could be an indication of an unstable discovery locator or an
unstable cluster.`,
	}),
	lastChangedTimestamp: promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "beam",
		Subsystem: "view_registry",
		Name:      "last_changed_timestamp",
		Help:      `Seconds since the Unix epoch when any change was made to the registry.`,
	}),
}
