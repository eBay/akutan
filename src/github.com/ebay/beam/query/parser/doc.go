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

// Package parser implements a parser combinator for the beam query language.
// After parsing a query string into a query object, it is passed to the
// planner for a cost based evaluation of possible execution paths and finally
// the executor where the retrieval and join operations on the best plan are
// performed.
//
// For documentation on the query language specifics; see the root README.md.
//
// https://en.wikipedia.org/wiki/Parser_combinator
//
package parser
