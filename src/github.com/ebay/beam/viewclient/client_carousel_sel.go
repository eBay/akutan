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

package viewclient

import (
	"fmt"

	"github.com/ebay/beam/space"
	"github.com/ebay/beam/viewclient/fanout"
)

type carouselRequest struct {
	from     fanout.View
	reqRange space.Range
}

func (cr carouselRequest) String() string {
	return fmt.Sprintf("req: %s-%s from %v", cr.reqRange.Start, cr.reqRange.End, cr.from)
}

// selectCarouselRequests given a set of available views that can be used to fulfill the request
// whats set of requests/views should we make ?
// This will in general try and pick a number of small partitions to get the data from, but in the
// event it needs to read from a larger partition, it'll try and use as much of that partition as possible
// The set of requests will cover the range 'cover' if its not possible to fully cover that range
// it'll return an error
func selectCarouselRequests(views fanout.Views, cover space.Range) ([]carouselRequest, error) {
	// covering returns the view with the smallest sized partition
	// that covers the provided point, or nil if there isn't one
	covering := func(hash space.Point) fanout.View {
		rngSize := func(r space.Range) space.Hash64 {
			if r.End.Less(space.Infinity) {
				return r.End.(space.Hash64) - r.Start.(space.Hash64)
			}
			return -r.Start.(space.Hash64)
		}
		var c fanout.View
		for i := 0; i < views.Len(); i++ {
			view := views.View(i)
			r := view.Serves()
			if r.Contains(hash) {
				if c == nil || rngSize(r) < rngSize(c.Serves()) {
					c = view
				}
			}
		}
		return c
	}
	requests := make([]carouselRequest, 0, views.Len()) // the set of requests we're going to end up making
	// reqAt is the point in the space we're currently looking for a view for
	for reqAt := cover.Start; reqAt.Less(cover.End); {
		viewToUse := covering(reqAt)
		if viewToUse == nil {
			return nil, fmt.Errorf("no views found to supply data for hash point %s", reqAt)
		}
		req := carouselRequest{
			from: viewToUse,
			reqRange: space.Range{
				Start: reqAt,
				End:   space.PointMin(cover.End, viewToUse.Serves().End),
			},
		}
		// it's possible that view selected covers one or more previous partitions,
		// if so we should prefer this view for those partitions as well
		if len(requests) > 0 && viewToUse.Serves().Start.Less(reqAt) {
			for len(requests) > 0 {
				prior := requests[len(requests)-1]
				if space.PointLeq(viewToUse.Serves().Start, prior.reqRange.Start) {
					// viewToUse can cover the previous request as well
					requests = requests[:len(requests)-1]
					req.reqRange.Start = prior.reqRange.Start
				} else {
					break
				}
			}
		}
		requests = append(requests, req)
		reqAt = req.reqRange.End
	}
	return requests, nil
}
