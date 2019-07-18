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

package logspecclient

import (
	metricsutil "github.com/ebay/akutan/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type clientMetrics struct {
	appendCallBytes           prometheus.Histogram
	appendRequestBytes        prometheus.Histogram
	appendRTTSeconds          prometheus.Summary
	readBytes                 prometheus.Histogram
	readEntries               prometheus.Histogram
	redirectsTotal            prometheus.Counter
	disconnectsTotal          prometheus.Counter
	outstandingAppendRequests prometheus.Gauge
}

var metrics clientMetrics

func init() {
	mr := metricsutil.Registry{R: prometheus.DefaultRegisterer}
	metrics = clientMetrics{
		appendCallBytes: mr.NewHistogram(prometheus.HistogramOpts{
			Namespace: "akutan",
			Subsystem: "logspecclient",
			Name:      "append_call_bytes",
			Help: `The number of bytes passed to the append function.

Append splits the data passed into separate requests. The logspecclient will try
to batch these bytes together from those of other concurrent requests, where
each request will contain a single large entry or a number of smaller entries.

If batching is done right, then even small sizes here would cause big network
requests (which can be identified by 'append_request_bytes'). If the sizes
coming in here look big already, then we may not need the batching at all.

This metric will not measure retried requests.
`,
			Buckets: prometheus.ExponentialBuckets(16, 10.0, 8),
		}),
		appendRequestBytes: mr.NewHistogram(prometheus.HistogramOpts{
			Namespace: "akutan",
			Subsystem: "logspecclient",
			Name:      "append_request_bytes",
			Help: `The size of a batched append request.

This metric observes the number of bytes per request. If the send fails, we end 
up counting the bytes multiple times (every send attempt). Low values indicate
that either load is low or batching isn't happening effectively.
`,
			Buckets: prometheus.ExponentialBuckets(16, 10.0, 8),
		}),
		appendRTTSeconds: mr.NewSummary(prometheus.SummaryOpts{
			Namespace: "akutan",
			Subsystem: "logspecclient",
			Name:      "append_rtt_seconds",
			Help: `The round trip time in seconds between sending an append request and receiving a confirmation response.

This metric measures RTT outside of gRPC metrics as the append function uses 
bidirectional streams so the server can apply back pressure. This metric
measures the RTT of a successful append, therefore, it won't count retries. 
For this reason the gRPC middleware-measured RTT for append may be inaccurate.
`,
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
		}),
		readBytes: mr.NewHistogram(prometheus.HistogramOpts{
			Namespace: "akutan",
			Subsystem: "logspecclient",
			Name:      "read_bytes",
			Help: `The read size in bytes

This metric observes the read 'OK' (confirmed) size in bytes. It is measured 
outside the gRPC middleware metrics as read also uses bidirectional streaming.
`,
			Buckets: prometheus.ExponentialBuckets(16, 10.0, 8),
		}),
		readEntries: mr.NewHistogram(prometheus.HistogramOpts{
			Namespace: "akutan",
			Subsystem: "logspecclient",
			Name:      "read_entries",
			Help: `The number of entries received in a read from the log

As 'read_bytes' observes the received read size, this metric observes the number
of entries each of these reads contains.
`,
			Buckets: prometheus.ExponentialBuckets(1, 10.0, 7),
		}),
		redirectsTotal: mr.NewCounter(prometheus.CounterOpts{
			Namespace: "akutan",
			Subsystem: "logspecclient",
			Name:      "redirects_total",
			Help: `The number of times the client received a redirect reply from the log.

Some redirect replies may be stale by the time they're processed. For
example, if there are 3 concurrent requests and they all return redirects, 3
get counted in this metrics but only 1 will be processed.
`,
		}),
		disconnectsTotal: mr.NewCounter(prometheus.CounterOpts{
			Namespace: "akutan",
			Subsystem: "logspecclient",
			Name:      "disconnects_total",
			Help: `The number of times the client closed its connection to the log service.

This metric observes the number of times the client disconnected the 
connection from the log service. The disconnect is initiated by the client. 
`,
		}),
		outstandingAppendRequests: mr.NewGauge(prometheus.GaugeOpts{
			Namespace: "akutan",
			Subsystem: "logspecclient",
			Name:      "outstanding_append_requests",
			Help: `The current number of append requests sent but waiting for replies from the log.

Many append requests are sent on a stream. This metric measures the number of
requests sent on the stream that are waiting for replies.
`,
		}),
	}
}
