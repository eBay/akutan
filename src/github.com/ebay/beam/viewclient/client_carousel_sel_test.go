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
	"math"
	"testing"

	"github.com/ebay/beam/partitioning"
	"github.com/ebay/beam/space"
	"github.com/ebay/beam/viewclient/fanout"
	"github.com/stretchr/testify/assert"
)

func Test_SelectCarouselRequests(t *testing.T) {
	// simple case, 4 source views, request asking for entire range
	v := newViews(4)
	requests, err := selectCarouselRequests(fanout.NewViews(v), fullRange)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(requests))
	assertReq(t, requests[0], v[0], mkRange(0, 4))
	assertReq(t, requests[1], v[1], mkRange(1, 4))
	assertReq(t, requests[2], v[2], mkRange(2, 4))
	assertReq(t, requests[3], v[3], mkRange(3, 4))

	// same as before but there's 2 extra views available that are 50/50, they shouldn't get used
	v2 := newViews(2)
	requests, err = selectCarouselRequests(flatten(v, v2), fullRange)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(requests))
	assertReq(t, requests[0], v[0], mkRange(0, 4))
	assertReq(t, requests[1], v[1], mkRange(1, 4))
	assertReq(t, requests[2], v[2], mkRange(2, 4))
	assertReq(t, requests[3], v[3], mkRange(3, 4))

	// now we make one of the /4 views unavailable, the relevant /2 view should be used instead
	// as we're using a /2 view, we'll use all of it
	v[1] = nil
	requests, err = selectCarouselRequests(flatten(v, v2), fullRange)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(requests))
	assertReq(t, requests[0], v2[0], mkRange(0, 2))
	assertReq(t, requests[1], v[2], mkRange(2, 4))
	assertReq(t, requests[2], v[3], mkRange(3, 4))

	// in this case, we're asking for just 1 slice from a /3, from the same views as before
	// if the /3 is in the first half, it'll use just v2 [due to the missing view in the /4],
	//if its in the later half, it'll use the 2 /4 views
	req := mkRange(0, 3)
	requests, err = selectCarouselRequests(flatten(v, v2), req)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(requests))
	assertReq(t, requests[0], v2[0], mkRange(0, 3))

	req = mkRange(2, 3)
	requests, err = selectCarouselRequests(flatten(v, v2), req)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(requests))
	v2Start := mkRange(2, 3).Start
	v2End := mkRange(2, 4).End
	assertReq(t, requests[0], v[2], space.Range{Start: v2Start, End: v2End})
	assertReq(t, requests[1], v[3], mkRange(3, 4))
}

func Test_SelectCarouselRequests_UsesViewFromStart(t *testing.T) {
	// if a view is selected, it should also cover any prior requests it can cover
	// we'll have some views at the start of the range, but a large
	// view at the end, that should end up replacing all the earlier ones
	v := []fanout.View{newView(0, 4), newView(1, 4), newView(2, 4), newView(0, 1)}
	requests, err := selectCarouselRequests(fanout.NewViews(v), fullRange)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(requests))
	assertReq(t, requests[0], v[3], fullRange)

	// the selected view might cover part of the prior request, but not all of it
	// that should leave the prior request along

	v[3] = newViewCovers(pctRange(45, 55))
	// v now contains views that cover the space like this
	//  0     25%    50%    75%
	// 	|  v0  |  v1  |  v2  |
	//               |v3|
	//
	reqRange := pctRange(35, 54)
	requests, err = selectCarouselRequests(fanout.NewViews(v), reqRange)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(requests))
	assertReq(t, requests[0], v[1], pctRange(35, 50))
	assertReq(t, requests[1], v[3], pctRange(50, 54))

	// in this case v1 will get selected, then v3, then v2
	// and it'll spot that v2 can cover the part of v3 being used
	// and remove the use of v3
	reqRange = pctRange(42, 60)
	requests, err = selectCarouselRequests(fanout.NewViews(v), reqRange)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(requests))
	assertReq(t, requests[0], v[1], pctRange(42, 50))
	assertReq(t, requests[1], v[2], pctRange(50, 60))
}

func Test_SelectCarouselRequests_MissingView(t *testing.T) {
	v := newViews(4)
	v[3] = nil
	requests, err := selectCarouselRequests(flatten(v), fullRange)
	assert.EqualError(t, err, "no views found to supply data for hash point 0xC000000000000000")
	assert.Nil(t, requests)

	// but if views are missing, but not in the request range, that shouldn't cause a problem
	requests, err = selectCarouselRequests(flatten(v), mkRange(0, 2))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(requests))
	assertReq(t, requests[0], v[0], mkRange(0, 4))
	assertReq(t, requests[1], v[1], mkRange(1, 4))
}

func mkRange(startPartition, numPartitions int) space.Range {
	return partitioning.NewHashSubjectPredicatePartition(startPartition, numPartitions).HashRange()
}

// pctRange returns a Hash64 range based on the supplied percentages (0-100) for the start & end points
func pctRange(startPercent, endPercent uint64) space.Range {
	pct := func(p uint64) space.Point {
		if p == 100 {
			return space.Infinity
		}
		one := float64(math.MaxUint64) / 100
		return space.Hash64(one * float64(p))
	}
	return space.Range{Start: pct(startPercent), End: pct(endPercent)}
}

func assertReq(t *testing.T, cr carouselRequest, expectedClient fanout.View, expRange space.Range) {
	t.Helper()
	assert.Equal(t, expectedClient.(*mockCarouselView).viewCount, cr.from.(*mockCarouselView).viewCount, "Unexpected View Selected")
	assert.Equal(t, expRange, cr.reqRange, "Request has unexpected Range selected")
}

type mockCarouselView struct {
	covers    space.Range
	viewCount int
}

var viewCount = 0

func (v *mockCarouselView) String() string {
	r := v.covers
	return fmt.Sprintf("View %d: partition %s-%s", v.viewCount, r.Start, r.End)
}

func newViewCovers(r space.Range) *mockCarouselView {
	viewCount++
	return &mockCarouselView{
		covers:    r,
		viewCount: viewCount,
	}
}
func newView(partition, numPartitions int) *mockCarouselView {
	return newViewCovers(partitioning.NewHashSubjectPredicatePartition(partition, numPartitions).HashRange())
}

func newViews(numPartitions int) []fanout.View {
	r := make([]fanout.View, numPartitions)
	for i := 0; i < numPartitions; i++ {
		r[i] = newView(i, numPartitions)
	}
	return r
}

func (v *mockCarouselView) Serves() space.Range {
	return v.covers
}

// flatten will take a list of views and return a singular list with all of them, nil entries are removed
// [so that you can do newViews(), nil out a couple from the results, then flatten, to test missing/stale views]
func flatten(views ...[]fanout.View) fanout.Views {
	r := make([]fanout.View, 0, len(views))
	for _, cv := range views {
		for _, v := range cv {
			if v != nil {
				r = append(r, v)
			}
		}
	}
	return fanout.NewViews(r)
}
