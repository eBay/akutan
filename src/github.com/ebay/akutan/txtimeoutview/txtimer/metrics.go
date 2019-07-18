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

package txtimer

import (
	metricsutil "github.com/ebay/akutan/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type txTimerMetrics struct {
	startsApplied        prometheus.Counter
	commitsApplied       prometheus.Counter
	abortsApplied        prometheus.Counter
	wipesApplied         prometheus.Counter
	abortsStartedTotal   prometheus.Counter
	abortsSucceededTotal prometheus.Counter
	abortsFailedTotal    prometheus.Counter
	restarts             prometheus.Counter
	lastApplied          prometheus.Gauge
	pendingTxns          prometheus.Gauge
}

var metrics txTimerMetrics

func init() {
	mr := metricsutil.Registry{R: prometheus.DefaultRegisterer}
	metrics = txTimerMetrics{
		startsApplied: mr.NewCounter(prometheus.CounterOpts{
			Namespace: "akutan",
			Subsystem: "txtimer",
			Name:      "starts_applied",
			Help:      `The number of transaction start entries this TxTimer applied from the log.`,
		}),
		commitsApplied: mr.NewCounter(prometheus.CounterOpts{
			Namespace: "akutan",
			Subsystem: "txtimer",
			Name:      "commits_applied",
			Help:      `The number of transaction commit entries this TxTimer applied from the log.`,
		}),
		abortsApplied: mr.NewCounter(prometheus.CounterOpts{
			Namespace: "akutan",
			Subsystem: "txtimer",
			Name:      "aborts_applied",
			Help:      `The number of transaction abort entries this TxTimer applied from the log.`,
		}),
		wipesApplied: mr.NewCounter(prometheus.CounterOpts{
			Namespace: "akutan",
			Subsystem: "txtimer",
			Name:      "wipes_applied",
			Help:      `The number of wipe entries this TxTimer applied from the log.`,
		}),
		abortsStartedTotal: mr.NewCounter(prometheus.CounterOpts{
			Namespace: "akutan",
			Subsystem: "txtimer",
			Name:      "aborts_started_total",
			Help:      `The total number of abort decisions this TxTimer attempted to append to the log.`,
		}),
		abortsSucceededTotal: mr.NewCounter(prometheus.CounterOpts{
			Namespace: "akutan",
			Subsystem: "txtimer",
			Name:      "aborts_succeeded_total",
			Help:      `The total number of abort decisions this TxTimer successfully appended to the log.`,
		}),
		abortsFailedTotal: mr.NewCounter(prometheus.CounterOpts{
			Namespace: "akutan",
			Subsystem: "txtimer",
			Name:      "aborts_failed_total",
			Help:      `The total number of abort decisions this TxTimer attempted to append to the log but the append returned an error.`,
		}),
		restarts: mr.NewCounter(prometheus.CounterOpts{
			Namespace: "akutan",
			Subsystem: "txtimer",
			Name:      "restarts",
			Help:      `The number of times this TxTimer failed to read the log and had to reinitialize either for falling behind in reading the log or by unexpected error.`,
		}),
		lastApplied: mr.NewGauge(prometheus.GaugeOpts{
			Namespace: "akutan",
			Subsystem: "txtimer",
			Name:      "last_applied",
			Help:      `The index of the last log entry this TxTimer applied.`,
		}),
		pendingTxns: mr.NewGauge(prometheus.GaugeOpts{
			Namespace: "akutan",
			Subsystem: "txtimer",
			Name:      "pending_txns",
			Help:      `The number of started transactions that have not yet been committed or aborted, as observed by this TxTimer.`,
		}),
	}
}
