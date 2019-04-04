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

package web

import (
	"fmt"
	"io"
	"net/http"
)

// APIError defines an error that is destined to be a HTTP response.
// It includes both a textual message and HTTP Status Code to use.
// Construct an APIError using NewError.
type APIError struct {
	statusCode int
	message    string
}

// NewError constructs a NewApiError with the supplied HTTP Status Code and
// formats the supplied msg & arguments
func NewError(statusCode int, formatMsg string, formatParams ...interface{}) error {
	return &APIError{
		statusCode: statusCode,
		message:    fmt.Sprintf(formatMsg, formatParams...),
	}
}

// Error implements the standard error interface
func (a *APIError) Error() string {
	return a.message
}

// HTTPWrite can be called to return this error as a HTTP Response
func (a *APIError) HTTPWrite(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(a.statusCode)
	io.WriteString(w, a.message)
	io.WriteString(w, "\n")
}

// Ensure APIError is a HTTPWriter
var _ HTTPWriter = &APIError{}
