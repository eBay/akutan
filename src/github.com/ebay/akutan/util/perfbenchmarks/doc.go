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

// Package perfbenchmarks contains benchmarks for Go language and standard
// library features.
package perfbenchmarks

// Dummy is used in hopes of preventing compiler optimizations from deleting the
// benchmark code entirely. It may not be necessary for dumb compilers, and it
// may not be sufficient for smart compilers.
var Dummy uint64
