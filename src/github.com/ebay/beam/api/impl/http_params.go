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

package impl

import (
	"net/http"
	"strconv"
	"time"

	"github.com/ebay/beam/util/web"
)

// Silence bin/unused.
var _ = parseInt64Param

func parseInt64Param(r *http.Request, paramName string, defaultValuer func() (int64, error)) (int64, error) {
	sv := r.URL.Query().Get(paramName)
	if sv != "" {
		pv, err := strconv.ParseInt(sv, 10, 64)
		if err != nil {
			return 0, web.NewError(http.StatusBadRequest, "Unable to parse queryString params '%s': %v", paramName, err)
		}
		return pv, nil
	}
	if defaultValuer != nil {
		return defaultValuer()
	}
	return 0, web.NewError(http.StatusBadRequest, "QueryString param '%s' must be specified", paramName)
}

func parseDuration(r *http.Request, paramName string, defaultValuer func() (time.Duration, error)) (time.Duration, error) {
	sv := r.URL.Query().Get(paramName)
	if sv != "" {
		pv, err := time.ParseDuration(sv)
		if err != nil {
			return 0, web.NewError(http.StatusBadRequest, "Unable to parse queryString param '%s' into a Duration: %v", paramName, err)
		}
		return pv, err
	}
	if defaultValuer != nil {
		return defaultValuer()
	}
	return 0, web.NewError(http.StatusBadRequest, "QueryString param '%s' must be specified", paramName)
}
