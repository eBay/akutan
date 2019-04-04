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

// Package web aids in writing HTTP servers.
package web

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// WriteError will write a textual error response to the supplied ResponseWriter with the
// supplied HTTP StatusCode
func WriteError(w http.ResponseWriter, statusCode int, formatMsg string, params ...interface{}) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(statusCode)
	fmt.Fprintf(w, formatMsg, params...)
	io.WriteString(w, "\n")
}

// HTTPWriter defines a way for a type to control how its returned as a HTTP response
// values passed to Write that implement this interface will have their HTTPWrite function
// called to generate the HTTP Response
type HTTPWriter interface {
	HTTPWrite(w http.ResponseWriter)
}

// Write is a helper function to write out a http response. It'll write the first non-nil
// val in the val list (so you can do things like web.Write(w, err, foo)) and have err
// returned if it was set.
func Write(w http.ResponseWriter, vals ...interface{}) {
	for _, val := range vals {
		if val != nil {
			switch tv := val.(type) {
			case []byte:
				w.Write(tv)
			case string:
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				io.WriteString(w, tv)
			case HTTPWriter:
				tv.HTTPWrite(w)
			case error:
				WriteError(w, http.StatusInternalServerError, "Unexpected error: %s", tv)
			default:
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(tv)
			}
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}
