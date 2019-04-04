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

package fanout

import (
	"testing"

	"github.com/ebay/beam/space"
	"github.com/stretchr/testify/assert"
)

type TestPoint = space.Hash64

type TestView space.Range

func NewTestView(start, end space.Point) TestView {
	return TestView{
		Start: start,
		End:   end,
	}
}

func (view TestView) Serves() space.Range {
	return space.Range(view)
}

func Test_Partition(t *testing.T) {
	hashBoundaries := func(overallRange space.Range, views Views) []uint64 {
		points := Partition(overallRange, views).StartPoints
		hashes := make([]uint64, len(points))
		for i := range points {
			hashes[i] = uint64(points[i].(TestPoint))
		}
		return hashes
	}

	assert := assert.New(t)
	assert.Equal([]uint64{11},
		hashBoundaries(
			space.Range{Start: TestPoint(11), End: TestPoint(18)},
			NewViews([]View{
				NewTestView(TestPoint(10), TestPoint(20)),
			})))

	assert.Equal([]uint64{0, 10, 20},
		hashBoundaries(
			space.Range{Start: TestPoint(0), End: space.Infinity},
			NewViews([]View{
				NewTestView(TestPoint(10), TestPoint(20)),
			})))
	assert.Equal([]uint64{0, 10, 20, 30, 40},
		hashBoundaries(
			space.Range{Start: TestPoint(0), End: space.Infinity},
			NewViews([]View{
				NewTestView(TestPoint(30), TestPoint(40)),
				NewTestView(TestPoint(10), TestPoint(20)),
				NewTestView(TestPoint(30), TestPoint(40)),
			})))
}
