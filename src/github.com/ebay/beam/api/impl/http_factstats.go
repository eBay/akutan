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

	"github.com/ebay/beam/rpc"
	statsutil "github.com/ebay/beam/util/stats"
	"github.com/ebay/beam/util/web"
	"github.com/julienschmidt/httprouter"
	opentracing "github.com/opentracing/opentracing-go"
)

func (s *Server) factStats(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	span, ctx := opentracing.StartSpanFromContext(r.Context(), "factStats")
	defer span.Finish()
	fetchExternalIDs := r.URL.Query().Get("noext") != "1"
	fetchIndex := uint64(0)
	if fetchExternalIDs {
		var err error
		fetchIndex, err = s.resolveIndex(ctx, fetchIndex)
		if err != nil {
			web.WriteError(w, http.StatusInternalServerError, "%v", err)
			return
		}
	}
	stats, err := s.source.FactStats(ctx, new(rpc.FactStatsRequest))
	if err != nil {
		web.WriteError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	err = statsutil.PrettyPrint(ctx, w, stats.ToRPCFactStats(), s.source, fetchIndex, fetchExternalIDs)
	if err != nil {
		web.WriteError(w, http.StatusInternalServerError, "%v", err)
		return
	}
}
