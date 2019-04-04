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

// Package tracing assists with reporting OpenTracing traces.
package tracing

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ebay/beam/config"
	"github.com/ebay/beam/discovery/discoveryfactory"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	jaeger "github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

// A Tracer reports OpenTracing traces to a server.
type Tracer struct {
	// If not nil, called by Close.
	close func()
}

// New constructs a tracer and sets it as the global opentracing tracer.
// Call this early on from main functions to initialize Jaeger/OpenTracing. The
// locator described in beamCfg.Tracing.Locator should resolve to ports that
// accept jaeger.thrift over HTTP directly from clients. If err != nil, the
// returned tracer should be Closed to clean up resources and flush its buffer
// before program exit.
func New(serviceName string, beamCfg *config.Tracing) (*Tracer, error) {
	if beamCfg == nil {
		log.Warn("Skipping Jaeger setup: nil Tracing configuration")
		return &Tracer{}, nil
	}
	collectors, err := discoveryfactory.NewLocator(context.TODO(), &beamCfg.Locator)
	if err != nil {
		return nil, fmt.Errorf("failed to create Jaeger locator: %v", err)
	}
	cfg := jaegercfg.Configuration{
		ServiceName: serviceName,
		Sampler: &jaegercfg.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
	}
	transport := newTransport(collectors)
	reporter := jaeger.NewRemoteReporter(transport)
	logger := (*logrusAdapter)(log.WithFields(log.Fields{"component": "jaeger"}))
	tracer, closer, err := cfg.NewTracer(
		jaegercfg.Logger(logger),
		jaegercfg.Reporter(reporter),
		jaegercfg.ContribObserver(&contribObserver{}),
	)
	if err != nil {
		return nil, fmt.Errorf("could not initialize Jaeger tracer: %v", err)
	}
	opentracing.SetGlobalTracer(tracer)
	return &Tracer{
		close: func() {
			err := closer.Close()
			if err != nil {
				log.WithError(err).Warn("Error shutting down Jaeger tracer")
			}
		},
	}, nil
}

// Close stops the Tracer and cleans up resources. It is not thread-safe.
func (t *Tracer) Close() {
	if t.close != nil {
		t.close()
	}
	t.close = nil
}

type logrusAdapter log.Entry

func (_log *logrusAdapter) Error(msg string) {
	log := (*log.Entry)(_log)
	log.Error(strings.TrimSpace(msg))
}

func (_log *logrusAdapter) Infof(msg string, args ...interface{}) {
	log := (*log.Entry)(_log)
	log.Infof(strings.TrimSpace(msg), args...)
}

type contribObserver struct {
}

// OnStartSpan implements the method defined in jaeger.ContribObserver.
func (m *contribObserver) OnStartSpan(
	span opentracing.Span,
	operationName string,
	options opentracing.StartSpanOptions,
) (jaeger.ContribSpanObserver, bool) {
	return &spanObserver{
		span:          span,
		operationName: operationName,
		start:         options.StartTime,
	}, true
}

// spanObserver implements the interface jaeger.ContribSpanObserver.
type spanObserver struct {
	span          opentracing.Span
	start         time.Time
	operationName string
	// metricLock protects metric. It should rarely, if ever, be needed, as it's
	// unlikely that anyone will call UpdateMetric or finish a span
	// concurrently.
	metricLock sync.Mutex
	// If not nil, this metric will be updated with the duration of the span.
	metric Metric
}

func (o *spanObserver) OnSetOperationName(name string) {
	// Note: We think this is called without any locks held.
	// This method appears to be unused in practice, so it's not worth adding any
	// synchronization code here.
}

func (o *spanObserver) OnSetTag(key string, value interface{}) {
	// Note: We think this is called without any locks held.
	if key == "metric" {
		if metric, ok := value.(Metric); ok {
			o.metricLock.Lock()
			o.metric = metric
			o.metricLock.Unlock()
		}
	}
}

func (o *spanObserver) OnFinish(options opentracing.FinishOptions) {
	// Note: We think this is called without any locks held.
	dur := options.FinishTime.Sub(o.start)
	o.metricLock.Lock()
	if o.metric != nil {
		o.metric.Observe(dur.Seconds())
	}
	o.metricLock.Unlock()
}

// UpdateMetric arrranges for the given metric to be updated with the duration
// of the span (in seconds).
func UpdateMetric(span opentracing.Span, metric Metric) {
	span.SetTag("metric", stringableMetric{metric})
}

// Metric is satisfied by prometheus.Summary and prometheus.Histogram.
type Metric interface {
	prometheus.Metric
	Observe(float64)
}

// stringableMetric gives the Prometheus metrics a better stringer.
type stringableMetric struct {
	Metric
}

// String returns the fully-qualified name of the metric. This ends up being
// reported in the OpenTracing tag named "metric".
func (metric stringableMetric) String() string {
	// Desc doesn't seem to have a way to extract the name.
	// Its Stringer outputs like this:
	//   Desc{fqName: %q, help: %q, constLabels: {%s}, variableLabels: %v}
	s := metric.Desc().String()
	s = strings.TrimPrefix(s, `Desc{fqName: "`)
	i := strings.IndexByte(s, '"')
	if i < 0 {
		return ""
	}
	return s[:i]
}
