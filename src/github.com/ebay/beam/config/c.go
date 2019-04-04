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

// Package config contains the configuration for a Beam server. The
// configuration is typically loaded from a JSON file on disk.
package config

// Beam describes the configuration for a Beam server.
type Beam struct {
	// How to access the Beam database change request log. Required on all
	// servers.
	BeamLog BeamLog `json:"beamLog"`

	// Where to find the GRPC ports of all the view instances. Required on all
	// servers.
	ViewsLocator Locator `json:"viewsLocator"`

	// If non-nil, the configuration for distributed tracing (OpenTracing). If
	// nil, the server will not collect traces.
	Tracing *Tracing `json:"tracing,omitempty"`

	// Configuration for API servers. Ignored for other types of servers.
	API *API `json:"api,omitempty"`

	// Configuration for transaction timeout view servers. Ignored for other
	// types of servers.
	TxTimeoutView *TxTimeoutView `json:"txTimeoutView,omitempty"`

	// Configuration for disk view servers. Ignored for other types of servers.
	DiskView *DiskView `json:"diskView,omitempty"`
}

// A Locator specifies how to find endpoints to communicate with. For this
// purpose, an endpoint is a particular port on a server or service.
type Locator struct {
	// Either "static" or "kube".
	Type string `json:"type"`

	// Required for static locators; ignored otherwise. The host:port endpoints
	// that the locator will return. These are assumed to be TCP ports.
	Addresses []string `json:"addresses,omitempty"`

	// Required for Kubernetes locators; ignored otherwise. A Kubernetes label
	// selector to filter pods in the current namespace. See
	// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors.
	LabelSelector string `json:"labelSelector,omitempty"`
	// Required for Kubernetes locators; ignored otherwise. Within the selected
	// pods, ports with this name on any container will be returned as endpoints.
	PortName string `json:"portName,omitempty"`
}

// BeamLog contains configuration describing how to access the Beam database
// change request log.
type BeamLog struct {
	// Either "logspec" for the logspec store, or "kafka"
	Type string `json:"type"`

	// For the logspec store, the GRPC endpoints on the servers. For Kafka, the
	// broker ports.
	Locator Locator `json:"locator"`
}

// Tracing contains configuration related to distributed execution tracing.
type Tracing struct {
	// Must be "jaeger" (for now).
	Type string `json:"type"`

	// Endpoints that accept jaeger.thrift over HTTP directly from clients.
	Locator Locator `json:"locator"`
}

// API contains configuration specific to the API servers.
type API struct {
	// The host:port or :port on which to serve GRPC requests. Required.
	GRPCAddress string `json:"grpcAddress"`

	// The host:port or :port on which to serve HTTP requests (admin, metrics,
	// etc). Required.
	HTTPAddress string `json:"httpAddress"`

	// Experimental: if true, queries will collect significant information and
	// dump them on disk somewhere. If false (or unset), it won't.
	DebugQuery bool `json:"debugQuery"`
}

// TxTimeoutView contains configuration specific to transaction timer servers.
type TxTimeoutView struct {
	// The host:port or :port on which to serve GRPC requests. Required.
	GRPCAddress string `json:"grpcAddress"`

	// If non-empty, the host:port or :port on which to serve Prometheus metrics
	// over HTTP. If empty (or unset), the metrics will not be served.
	MetricsAddress string `json:"metricsAddress"`
}

// DiskView contains configuration specific to DiskView servers.
type DiskView struct {
	// The host:port or :port on which to serve GRPC requests. Required.
	GRPCAddress string `json:"grpcAddress"`

	// If non-empty, the host:port or :port on which to serve Prometheus metrics
	// over HTTP. If empty (or unset), the metrics will not be served.
	MetricsAddress string `json:"metricsAddress"`

	// Either "hashsp" or "hashpo". Required.
	//
	// "hashsp" and "hashpo" are two critically important hash-spaces for Beam;
	// each covers the entire set of facts stored. Facts in the "hashsp" space
	// are distributed by the hash of the fact's subject and predicate. Facts in
	// the "hashpo" space are distributed by the hash of the fact's predicate
	// and object.
	Space string `json:"space"`

	// The server will store all facts that hash from FirstHash up through and
	// including LastHash in the corresponding Space. Both are required, and
	// LastHash must be at least as large as FirstHash. The "hashsp" and
	// "hashpo" spaces range from "0x0000000000000000" through
	// "0xffffffffffffffff", inclusive. The format must match "0x%016x" exactly.
	FirstHash string `json:"firstHash"`
	LastHash  string `json:"lastHash"`

	// Must be "rocksdb", which indicates the server will store facts in
	// RocksDB. Required.
	Backend string `json:"backend"`

	// Used to decide when to replay the log vs request a carousel at startup.
	// If > 0, indicates how many log entries the server is behind before using
	// a carousel to catchup rather than reading the log. If 0 (or unset), the
	// server will only use the carousel when the log has already discarded the
	// next entry needed for log replay. Values < 0 are undefined.
	CarouselCatchupLag int64 `json:"carouselCatchupLag"`

	// If true, disables the behavior where multiple clients can read the entire
	// disk contents at once; carousel requests will instead do simple disk
	// iterations. If false (or unset), the more efficient but more complex
	// implementation is enabled.
	DisableCarousel bool `json:"disableCarousel"`

	// If "snappy" or "lz4", the server will store all data on disk using that
	// compression algorithm. If "none" or the empty string (or unset), the
	// server will store its data without compression.
	Compression string `json:"compression"`

	// If true, enables the ability to wipe the disk view removing the entire
	// data set. If false (or unset), such requests are ignored. This should
	// generally be disabled (false) on production clusters.
	EnableWipe bool `json:"enableWipe"`

	// The base location of the database file(s) for this server. Required.
	Dir string `json:"dir"`
}
