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
	"fmt"
	"net/http"
	"strconv"

	"github.com/ebay/beam/partitioning"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/errors"
	"github.com/ebay/beam/util/web"
	"github.com/julienschmidt/httprouter"
)

func (s *Server) rideCarousel(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	creq := rpc.CarouselRequest{}
	p := 0
	n := 1
	var err1, err2 error
	if r.URL.Query().Get("p") != "" {
		p, err1 = strconv.Atoi(r.URL.Query().Get("p"))
		n, err2 = strconv.Atoi(r.URL.Query().Get("n"))
		if err := errors.Any(err1, err2); err != nil {
			web.WriteError(w, http.StatusBadRequest, "Unable to parse request params: %v", err)
			return
		}
		if p >= n {
			web.WriteError(w, http.StatusBadRequest, "Partition # can't exceed number of partitions!")
			return
		}
		if p < 0 || n < 0 {
			web.WriteError(w, http.StatusBadRequest, "Partition & NumPartitions can't be negative")
		}
	}
	creq.HashPartition = partitioning.CarouselHashPartition(partitioning.NewHashSubjectPredicatePartition(p, n))
	c, err := s.source.RideCarousel(r.Context(), &creq, nil)
	if err != nil {
		web.Write(w, err)
		return
	}
	for {
		select {
		case item, chOpen := <-c.DataCh():
			if !chOpen {
				return
			}
			for _, fact := range item.Facts {
				fmt.Fprintf(w, "%d %d %d", fact.Id, fact.Subject, fact.Predicate)
				if fact.Object.UnitID() > 0 {
					fmt.Fprintf(w, " %d", fact.Object.UnitID())
				}
				fmt.Fprintf(w, " %s", fact.Object.String())
				if fact.Object.LangID() > 0 {
					fmt.Fprintf(w, " %d", fact.Object.LangID())
				}
				fmt.Fprint(w, "\n")
			}
		case err := <-c.ErrorsCh():
			fmt.Fprintf(w, "Error!: %v\n", err)
			c.Stop()
		}
	}
}
