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
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/ebay/beam/discovery"
	"github.com/ebay/beam/util/random"
	"github.com/sirupsen/logrus"
	jaeger "github.com/uber/jaeger-client-go"
	jaegerTransport "github.com/uber/jaeger-client-go/transport"
)

// placeholderHost is a dummy hostname put into the host component of a URL
// before the collector host has been determined.
const placeholderHost = "some.tracing.collector.localhost"

// newTransport returns a jaeger.Transport that sends traces to some collector
// from the given locator. It connects to new collectors as needed when the
// current collector isn't working.
func newTransport(collectors discovery.Locator) jaeger.Transport {
	roundTripper := &collectorProxy{
		collectors:    collectors,
		baseTransport: http.DefaultTransport.RoundTrip,
	}
	url := "http://" + placeholderHost + "/api/traces?format=jaeger.thrift"
	return jaegerTransport.NewHTTPTransport(url,
		jaegerTransport.HTTPRoundTripper(roundTripper))
}

// A collectorProxy is an http.RoundTripper that allows a client to connect to
// different servers over time.
type collectorProxy struct {
	// A locator that returns server endpoints.
	collectors discovery.Locator
	// Set to http.DefaultTransport.RoundTrip, except for testing.
	baseTransport func(req *http.Request) (*http.Response, error)
	// Protects locked.
	lock sync.Mutex
	// Protected by lock.
	locked struct {
		// The host:port of the current server in use, or "" if none.
		hostPort string
		// If non-nil, the current server's last request failed with this error.
		lastErr error
		// When the last successful request completed. If there hasn't been one,
		// when the connection was started.
		lastSuccess time.Time
	}
}

// RoundTrip implements the method defined in http.RoundTripper.
func (proxy *collectorProxy) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Host != placeholderHost {
		_ = req.Body.Close()
		return nil, fmt.Errorf("tracing: Jaeger client tried to reach unexpected host: %v",
			req.URL.Host)
	}
	hostPort, err := proxy.getHostPort()
	if err != nil {
		_ = req.Body.Close()
		return nil, err
	}
	// Make a copy to update the URL, since RoundTripper may not modify the given
	// 'req'.
	url2 := *req.URL
	url2.Host = hostPort
	req2 := req
	req2.URL = &url2
	resp, err := proxy.baseTransport(req2)
	if err == nil {
		if resp.StatusCode >= 400 && resp.StatusCode <= 599 {
			proxy.report(hostPort, errors.New(resp.Status))
		} else {
			proxy.report(hostPort, nil)
		}
	} else {
		proxy.report(hostPort, err)
	}
	return resp, err
}

func init() {
	// math/rand is used in getHostPort()
	random.SeedMath()
}

// getHostPort returns the best server to send a request to, or an error if no
// servers are known.
func (proxy *collectorProxy) getHostPort() (string, error) {
	proxy.lock.Lock()
	defer proxy.lock.Unlock()

	if proxy.locked.hostPort != "" {
		if proxy.locked.lastErr == nil ||
			time.Since(proxy.locked.lastSuccess) < 10*time.Second {
			return proxy.locked.hostPort, nil
		}
	}

	// Find other endpoints.
	endpoints := proxy.collectors.Cached().Endpoints
	others := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		ehp := endpoint.HostPort()
		if ehp != proxy.locked.hostPort {
			others = append(others, ehp)
		}
	}

	if len(others) == 0 {
		if proxy.locked.hostPort == "" {
			// Haven't found any endpoints yet.
			return "", errNoServer
		}
		// Stick with current endpoint.
		return proxy.locked.hostPort, nil
	}

	// Choose a new random endpoint.
	newHostPort := others[rand.Intn(len(others))]
	now := time.Now()
	logrus.WithFields(logrus.Fields{
		"lastError":                     proxy.locked.lastErr,
		"timeSinceLastConnectOrSuccess": now.Sub(proxy.locked.lastSuccess),
		"oldCollector":                  proxy.locked.hostPort,
		"newCollector":                  newHostPort,
	}).Info("Switching Jaeger servers")
	proxy.locked.hostPort = newHostPort
	proxy.locked.lastErr = nil
	proxy.locked.lastSuccess = now
	return newHostPort, nil
}

var errNoServer = errors.New("have not discovered a Jaeger collector yet")

// report updates the state based on a response from hostPort. Pass a nil error
// if successful or a non-nil error otherwise.
func (proxy *collectorProxy) report(hostPort string, err error) {
	proxy.lock.Lock()
	defer proxy.lock.Unlock()
	if proxy.locked.hostPort == hostPort {
		if err == nil {
			proxy.locked.lastSuccess = time.Now()
		}
		proxy.locked.lastErr = err
	}
}
