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

// Package infer implements fact inference by traversing transitive predicates.
// If the predicate defines a heirarchy, and an entity has a fact relating it
// to an item in the heirarchy, then this can infer that the entity
// also has facts to each parent item in the herirachy path.
//
// For exampe, given facts that describe a type classification
//
// [cell phone] -type-> [electronics] -type-> [product]
//
// and a fact that says
//
// [iphone]-type-> [cell phone]
//
// then we can infer that these additional facts are also true
//
// [iphone] -type -> [electronics]
//
// [iphone] -type -> [product]
//
// This package supports inference in both directions,
// i.e. start at <subject> and follow predicate -> object -> predicate -> object
// and start at <object> and follow backwards <- predicate <- subject <- predicate <- subject
//
package infer
