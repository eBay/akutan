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
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ebay/akutan/discovery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// See proxyTests.expLastSuccess.
type lastSuccessExpectation int

const (
	notUpdated lastSuccessExpectation = iota
	updatedBeforeRequest
	updatedAfterRequest
)

// This table describes tests that are run by Test_collectorProxy_RoundTrip.
// They each invoke the RoundTrip function once under different conditions.
var proxyTests = []struct {
	// A very brief description of the test.
	name string

	// Inputs:

	// Returned by collectors Locator.
	hosts []*discovery.Endpoint
	// Initial value for collectorProxy.locked.hostPort.
	initialHostPort string
	// Initial value for collectorProxy.locked.lastErr.
	initialLastErr string
	// If true, collectorProxy.locked.lastSuccess is set to the wall time before
	// running the request.
	setLastSuccessOnStart bool
	// If empty, the baseTransport returns a response. If non-empty, it returns
	// this error.
	baseTransportErr string
	// The HTTP status code of the response returned by the baseTransport.
	statusCode int

	// Assertions:

	// The host:port that the baseTransport should see in the request.
	expTarget string
	// The expected final value for collectorProxy.locked.hostPort.
	expHostPort string
	// If empty, collectorProxy.locked.lastError should end up nil. Otherwise,
	// the expected error message for collectorProxy.locked.lastErr.
	expLastErr string
	// Describes the expected final value for collectorProxy.locked.lastSuccess.
	expLastSuccess lastSuccessExpectation
	// If empty, the RoundTrip function should return a nil error. Otherwise,
	// the expected error message.
	expErr string
}{
	{
		// No servers available for initial connection.
		name:      "from empty, no servers",
		hosts:     nil,
		expTarget: "example.org:8080",
		expErr:    errNoServer.Error(),
	},
	{
		// Initial connection to a server succeeds.
		name: "from empty, OK",
		hosts: []*discovery.Endpoint{
			{Host: "example.org", Port: "8080"},
		},
		statusCode:     201,
		expTarget:      "example.org:8080",
		expHostPort:    "example.org:8080",
		expLastSuccess: updatedAfterRequest,
	},
	{
		// Initial connection to a server results in a 500 error.
		name: "from empty, 500",
		hosts: []*discovery.Endpoint{
			{Host: "example.org", Port: "8080"},
		},
		statusCode:     500,
		expTarget:      "example.org:8080",
		expHostPort:    "example.org:8080",
		expLastErr:     "500 Description Here",
		expLastSuccess: updatedBeforeRequest,
	},
	{
		// Initial connection to a server results in a RoundTripper error.
		name: "from empty, error",
		hosts: []*discovery.Endpoint{
			{Host: "example.org", Port: "8080"},
		},
		baseTransportErr: "ants in pants",
		expErr:           "ants in pants",
		expTarget:        "example.org:8080",
		expHostPort:      "example.org:8080",
		expLastErr:       "ants in pants",
		expLastSuccess:   updatedBeforeRequest,
	},
	{
		// Once happily connected, stick with that server.
		name: "from connected, OK",
		hosts: []*discovery.Endpoint{
			{Host: "example.org", Port: "8080"},
			{Host: "example.org", Port: "8081"},
			{Host: "example.org", Port: "8082"},
			{Host: "example.org", Port: "8083"},
		},
		initialHostPort: "example.org:8080",
		statusCode:      201,
		expTarget:       "example.org:8080",
		expHostPort:     "example.org:8080",
		expLastSuccess:  updatedAfterRequest,
	},
	{
		// Once happily connected, get a 500 error.
		name: "from connected, error",
		hosts: []*discovery.Endpoint{
			{Host: "example.org", Port: "8080"},
		},
		initialHostPort: "example.org:8080",
		statusCode:      500,
		expTarget:       "example.org:8080",
		expHostPort:     "example.org:8080",
		expLastErr:      "500 Description Here",
		expLastSuccess:  notUpdated,
	},
	{
		// Successful request after seeing a transient error.
		name: "from error, OK",
		hosts: []*discovery.Endpoint{
			{Host: "example.org", Port: "8080"},
		},
		initialHostPort: "example.org:8080",
		initialLastErr:  "ants in pants",
		statusCode:      201,
		expTarget:       "example.org:8080",
		expHostPort:     "example.org:8080",
		expLastSuccess:  updatedAfterRequest,
	},
	{
		// Got an error with this server, but stick with it for now (10s haven't
		// elapsed).
		name: "from error, stick with it",
		hosts: []*discovery.Endpoint{
			{Host: "example.org", Port: "8080"},
			{Host: "example.org", Port: "8081"},
		},
		initialHostPort:       "example.org:8080",
		initialLastErr:        "ants in pants",
		setLastSuccessOnStart: true,
		statusCode:            500,
		expTarget:             "example.org:8080",
		expHostPort:           "example.org:8080",
		expLastErr:            "500 Description Here",
		expLastSuccess:        notUpdated,
	},
	{
		// Got an error with this server, move on (10s have elapsed).
		name: "from error, pick other",
		hosts: []*discovery.Endpoint{
			{Host: "example.org", Port: "8080"},
			{Host: "example.org", Port: "8081"},
		},
		initialHostPort: "example.org:8081",
		initialLastErr:  "ants in pants",
		statusCode:      201,
		expTarget:       "example.org:8080",
		expHostPort:     "example.org:8080",
		expLastSuccess:  updatedAfterRequest,
	},
}

// Runs proxyTests.
func Test_collectorProxy_RoundTrip(t *testing.T) {
	for _, test := range proxyTests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)

			// Set up the proxy.
			proxy := &collectorProxy{
				collectors: discovery.NewStaticLocator(test.hosts),
			}
			proxy.locked.hostPort = test.initialHostPort
			if test.initialLastErr != "" {
				proxy.locked.lastErr = errors.New(test.initialLastErr)
			}
			var startAt time.Time
			if test.setLastSuccessOnStart {
				startAt = time.Now()
			}
			proxy.locked.lastSuccess = startAt

			// Set up the mock base transport.
			var reqAt time.Time
			proxy.baseTransport = func(req *http.Request) (*http.Response, error) {
				reqAt = time.Now()
				assert.Equal(fmt.Sprintf("http://%s/banana?peel=0#yum", test.expTarget),
					req.URL.String())
				body, err := ioutil.ReadAll(req.Body)
				assert.NoError(err)
				assert.NoError(req.Body.Close())
				assert.Equal("hi", string(body))
				if test.baseTransportErr != "" {
					return nil, errors.New(test.baseTransportErr)
				}
				return &http.Response{
					StatusCode: test.statusCode,
					Status:     fmt.Sprintf("%v Description Here", test.statusCode),
					Body:       ioutil.NopCloser(strings.NewReader("bye")),
				}, nil
			}

			// Execute the request.
			req, err := http.NewRequest("GET", "http://"+placeholderHost+"/banana?peel=0#yum",
				strings.NewReader("hi"))
			require.NoError(t, err)
			resp, err := proxy.RoundTrip(req)

			// Check the assertions.
			if test.expErr == "" {
				if assert.NoError(err) {
					assert.Equal(test.statusCode, resp.StatusCode)
					body, err := ioutil.ReadAll(resp.Body)
					assert.NoError(err)
					assert.NoError(resp.Body.Close())
					assert.Equal("bye", string(body))
				}
			} else {
				assert.EqualError(err, test.expErr)
			}
			assert.Equal(test.expHostPort, proxy.locked.hostPort)
			if test.expLastErr == "" {
				assert.NoError(proxy.locked.lastErr)
			} else {
				assert.EqualError(proxy.locked.lastErr, test.expLastErr)
			}
			switch test.expLastSuccess {
			case notUpdated:
				assert.Equal(startAt, proxy.locked.lastSuccess)
			case updatedBeforeRequest:
				assert.True(proxy.locked.lastSuccess.After(startAt) &&
					reqAt.After(proxy.locked.lastSuccess))
			case updatedAfterRequest:
				assert.True(reqAt.Before(proxy.locked.lastSuccess))
			}
		})
	}
}

// Tests what happens if Jaeger attempts to make requests to some host that's
// not placeholderHost.
func Test_collectorProxy_RoundTrip_invalidHost(t *testing.T) {
	proxy := &collectorProxy{}
	req, err := http.NewRequest("GET", "http://example.org/banana", strings.NewReader(""))
	require.NoError(t, err)
	_, err = proxy.RoundTrip(req)
	assert.EqualError(t, err,
		"tracing: Jaeger client tried to reach unexpected host: example.org")
}
