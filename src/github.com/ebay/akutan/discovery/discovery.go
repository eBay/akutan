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

// Package discovery defines basic concepts around service discovery and
// locating endpoints.
package discovery

import (
	"context"
	"fmt"
	"net"
	"strings"
)

// A Locator is used to discover servers or services to communicate with.
type Locator interface {
	// Cached returns the latest available endpoints without blocking. Repeated
	// calls to Cached should return fresher results over time.
	Cached() Result

	// WaitForUpdate blocks until it can return a different, newer Result from
	// the one identified by the given version, or the context expires. Version
	// 0 corresponds to a Result with no endpoints. The only error this method
	// returns is ctx.Err().
	WaitForUpdate(ctx context.Context, oldVersion uint64) (Result, error)

	// String returns a brief one-line description of the Locator.
	String() string
}

// Result is returned by a Locator to describe specific endpoints.
type Result struct {
	// The set of discovered endpoints, in no particular order. The caller must
	// not modify the contents of this slice. Locators should return the same
	// *Endpoint pointer in future Results if the values haven't changed.
	Endpoints []*Endpoint
	// A number that increases every time Endpoints changes.
	Version uint64
}

// An Endpoint represents a port on a particular server instance or load
// balancer.
type Endpoint struct {
	// Transport protocol. Almost always "tcp" or "udp". See net.Addr and
	// net.Dial.
	Network string
	// DNS name or IPv4/IPv6 address.
	Host string
	// Port number or (discouraged) well-known port name.
	Port string
	// Implementation- and user-defined key-value pairs.
	Annotations map[string]string
}

// HostPort returns a string like "example.com:8080".
func (e *Endpoint) HostPort() string {
	if e.Port == "" {
		panic(fmt.Sprintf("HostPort called on discovery.Endpoint with no port (host %s)",
			e.Host))
	}
	return net.JoinHostPort(e.Host, e.Port)
}

// String returns a string like "tcp://example.com:8080" meant for human
// consumption. The exact format should not be relied upon.
func (e *Endpoint) String() string {
	var b strings.Builder
	b.Grow(64)
	if e.Network == "" {
		b.WriteString("???")
	} else {
		b.WriteString(e.Network)
	}
	b.WriteString("://")
	if strings.IndexByte(e.Host, ':') >= 0 { // IPv6 address
		b.WriteByte('[')
		b.WriteString(e.Host)
		b.WriteByte(']')
	} else {
		b.WriteString(e.Host)
	}
	b.WriteByte(':')
	if e.Port == "" {
		b.WriteString("???")
	} else {
		b.WriteString(e.Port)
	}
	return b.String()
}

// GetNonempty blocks until the locator returns at least one endpoint, or the
// context expires. The only error it returns is ctx.Err().
func GetNonempty(ctx context.Context, locator Locator) (Result, error) {
	oldVersion := uint64(0)
	for {
		result, err := locator.WaitForUpdate(ctx, oldVersion)
		if err != nil {
			return Result{}, err
		}
		if len(result.Endpoints) > 0 {
			return result, nil
		}
		oldVersion = result.Version
	}
}
