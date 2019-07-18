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

package logping

import (
	metricsutil "github.com/ebay/akutan/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type logPingMetrics struct {
	lastApplied              prometheus.Gauge
	pingsStarted             prometheus.Counter
	pingsSucceeded           prometheus.Counter
	pingLatencySeconds       prometheus.Summary
	pingsAborted             prometheus.Counter
	pingAppendsSucceeded     prometheus.Counter
	pingAppendsFailed        prometheus.Counter
	pingAppendLatencySeconds prometheus.Summary
	disconnects              prometheus.Counter
	restarts                 prometheus.Counter
}

var metrics logPingMetrics

func init() {
	mr := metricsutil.Registry{R: prometheus.DefaultRegisterer}
	metrics = logPingMetrics{
		lastApplied: mr.NewGauge(prometheus.GaugeOpts{
			Namespace: "akutan",
			Subsystem: "logping",
			Name:      "last_applied",
			Help:      "The index of the last log entry this Pinger applied.",
		}),
		pingsStarted: mr.NewCounter(prometheus.CounterOpts{
			Namespace: "akutan",
			Subsystem: "logping",
			Name:      "pings_started",
			Help:      `The number of times the Pinger started a log append and timing of a read.`,
		}),
		pingsSucceeded: mr.NewCounter(prometheus.CounterOpts{
			Namespace: "akutan",
			Subsystem: "logping",
			Name:      "pings_succeeded",
			Help: `The number of times the Pinger finished timing a ping successfully.

This doesn't include the log append failures or timeouts (aborts).
`,
		}),
		pingLatencySeconds: mr.NewSummary(prometheus.SummaryOpts{
			Namespace:  "akutan",
			Subsystem:  "logping",
			Name:       "ping_latency_seconds",
			Help:       `The time it takes to complete a ping successfully.`,
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
		}),
		pingsAborted: mr.NewCounter(prometheus.CounterOpts{
			Namespace: "akutan",
			Subsystem: "logping",
			Name:      "pings_aborted",
			Help: `The number of times the Pinger timed out the current ping.

After starting a ping the Pinger waits for a particular duration (ping
interval) to read its ping command back. If the command has not been
read from the log by then it aborts the current ping and starts the
next ping immediately.
`,
		}),
		pingAppendsSucceeded: mr.NewCounter(prometheus.CounterOpts{
			Namespace: "akutan",
			Subsystem: "logping",
			Name:      "ping_appends_succeeded",
			Help:      `The number of times the Pinger's log append succeeded.`,
		}),
		pingAppendsFailed: mr.NewCounter(prometheus.CounterOpts{
			Namespace: "akutan",
			Subsystem: "logping",
			Name:      "ping_appends_failed",
			Help:      `The number of times the Pinger's log append returned an error.`,
		}),
		pingAppendLatencySeconds: mr.NewSummary(prometheus.SummaryOpts{
			Namespace:  "akutan",
			Subsystem:  "logping",
			Name:       "append_latency_seconds",
			Help:       `The time it takes to append a ping to the log and get an acknowledgment or an error.`,
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
		}),
		disconnects: mr.NewCounter(prometheus.CounterOpts{
			Namespace: "akutan",
			Subsystem: "logping",
			Name:      "disconnects",
			Help: `The number of times the Pinger artificially disconnected from the log service.

The Pinger will artificially disconnect from the log service every fixed number
of pings. It does so to connect to other log servers and sample them as well, in
case some servers exhibit different latency than others.

This feature is currently only implemented for the log spec client (not for Kafka).
`,
		}),
		restarts: mr.NewCounter(prometheus.CounterOpts{
			Namespace: "akutan",
			Subsystem: "logping",
			Name:      "restarts",
			Help:      `The number of times this Pinger failed to read the log and had to reinitialize either for falling behind in reading the log or by unexpected error.`,
		}),
	}
}
