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

// Package api contains ProtoBuf-generated types for the external gRPC API to Akutan.
package api

/*
Types used in major read & write paths
======================================

This doc describes the different sets of types used in the primary read & write
paths, and why they exist.

Write
=====
A high level view of write is:

    a) Client sends write request to API server using types defined in the api.proto

    b) API server translates write into types defined in the logentry package
       and writes them to the log

    c) Views read the entries from the log and convert them from the logentry
       type to the types defined in the rpc package / views.proto

These types are very similar but different in various ways related to the usage
and differing points in the facts state. e.g. for insert Facts, facts start by
being related by named variables at the api level. At the logentry level facts
are related by KID offsets rather than variables. At the rpc level facts have
converted the KID offsets into absolute KIDs.

Log entries are persisted for potentially a long time and across multiple
versions of the software. For this reason we want to be able to explicitly
control the versioning of the log entries independently from other usages. In
some cases the log entry and API versions will be very similar, in some cases
they'll be more different.

Entries read from the log reflect the fact that the log index isn't known before
the entry payload is sent to the log. Once read from the log the data can be
converted to the rpc based types which have resolved the item into a concrete,
fully defined item. e.g. all KIDs have been resolved to their final absolute
values.

The rpc version of KGObject is a little different in that it wraps the
serialized format for KGObject, allowing it to easily have the same semantics as
the serialized version in rocksDB. This also saves marshalling overhead as they
can be easily serialized to/from the wire format.

Read
====

A typical read requests flow consists of:

    a) A client sends a request to the API server

    b) the API server examines the request and determines which views(s) can be
       used to calculate the response. In some cases this may involve complex
       query parsing & optimization.

    c) the API server makes rpc requests to the relevant views, performing the
       needed transformations before returning the response to the client.

Requests from the API tier to the Views use the rpc types defined in
views.proto. The diskview upon received a read request will read the relevant
keys from rocksDB converting them into rpc.Fact objects and returning them to the
API server. The API server will convert these types from the rpc package into
their API equivalents. This allows us to easily evolve the external API and our
internal APIs.

API types are defined in proto/api/akutan_api.proto file

logentry types are defined in the logentry package, see
src/github.com/ebay/akutan/logentry/commands.proto

rpc types are defined in the rpc package, see
src/github.com/ebay/akutan/rpc/views.proto

*/
