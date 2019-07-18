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

package tracing

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

// Used in Test_metricsObserver_UpdateMetric.
type recordingMetric struct {
	// A hack to make recordingMetric satisfy the Metric interface.
	prometheus.Metric
	// A log of Observe calls.
	values []float64
}

func (metric *recordingMetric) Observe(value float64) {
	metric.values = append(metric.values, value)
}

func Test_metricsObserver_UpdateMetric(t *testing.T) {
	assert := assert.New(t)
	cfg := jaegercfg.Configuration{
		ServiceName: t.Name(),
	}
	tracer, closer, err := cfg.NewTracer(jaegercfg.ContribObserver(&contribObserver{}))
	assert.NoError(err)
	defer func() {
		assert.NoError(closer.Close())
	}()
	metric := new(recordingMetric)
	for i := 0; i < 3; i++ {
		span := tracer.StartSpan(t.Name())
		UpdateMetric(span, metric)
		time.Sleep(time.Millisecond)
		span.Finish()
	}
	assert.Len(metric.values, 3)
	for _, value := range metric.values {
		dur := time.Nanosecond * time.Duration(value*1e9)
		assert.True(dur >= time.Millisecond, "duration: %v", dur)
		assert.True(dur <= 100*time.Millisecond, "duration: %v", dur)
	}
}

func Test_stringableMetric(t *testing.T) {
	metric := prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace:  "akutan",
		Subsystem:  "txtimer",
		Name:       "aborts_started",
		Help:       "The number of abort decisions ... the log.",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01},
	})
	stringable := stringableMetric{metric}
	assert.Equal(t, "akutan_txtimer_aborts_started", fmt.Sprint(stringable))
}
