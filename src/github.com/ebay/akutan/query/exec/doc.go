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

// Package exec is used to execute a KG query that was built by the query
// planner. The Execute method takes a plan and executes it, generating a stream
// of ResultChunks containing the results.
//
// Each FactSet represents a single valid set of facts that match the query, in
// addition to the facts the ResultChunk also contains a tabular set of values
// related to variables.
//
// Each node in the plan is converted into a queryOperator instance, creating a
// parallel tree of queryOperators. Executing the query is then a matter of
// executing the root node in the tree. Each queryOperator publishes a results
// stream on a caller supplied channel, slow consumption of the channel will
// apply back pressure down the tree, possibly all the way to the underling RPC
// calls to the Views.
package exec
