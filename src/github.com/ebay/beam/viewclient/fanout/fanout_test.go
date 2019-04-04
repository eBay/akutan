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
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/ebay/beam/space"
	"github.com/ebay/beam/util/clocks"
	"github.com/ebay/beam/util/random"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func shortPointStr(p space.Point) string {
	switch p := p.(type) {
	case space.Hash64:
		return fmt.Sprintf("%d", p)
	default:
		return fmt.Sprintf("%v", p)
	}
}

func viewsStr(views []View) string {
	var slice []string
	for _, v := range views {
		r := v.Serves()
		slice = append(slice, fmt.Sprintf("%v-%v",
			shortPointStr(r.Start),
			shortPointStr(r.End)))
	}
	return strings.Join(slice, " ")
}

// closed returns nil if the channel is closed or an error if the channel is not
// closed. Use with:
//     assert.NoError(closed(ch))
func closed(ch chan Chunk) error {
	_, open := <-ch
	if open {
		return errors.New("channel should have been closed")
	}
	return nil
}

func Test_Call_0points_0views(t *testing.T) {
	assert := assert.New(t)
	resultCh := make(chan Chunk, 4)
	err := Call(context.Background(), nil, nil, nil, resultCh)
	assert.NoError(err)
	assert.Len(resultCh, 0)
	assert.NoError(closed(resultCh))
}

func Test_Call_1point_0views(t *testing.T) {
	assert := assert.New(t)
	resultCh := make(chan Chunk, 4)
	points := []space.Point{space.Key("nowhere")}
	err := Call(context.Background(), points, nil, nil, resultCh)
	assert.Error(err)
	assert.Regexp("no server contains", err.Error())
	assert.Len(resultCh, 0)
	assert.NoError(closed(resultCh))
}

func Test_Call_1point_1view(t *testing.T) {
	assert := assert.New(t)
	resultCh := make(chan Chunk, 4)
	points := []space.Point{space.Key("hello")}
	views := NewViews([]View{NewTestView(space.Key("a"), space.Key("z"))})
	rpc := func(ctx context.Context, view View, offsets []int, results func(Result)) error {
		assert.Equal(views.View(0), view)
		assert.Equal([]int{0}, offsets)
		results("world")
		return nil
	}
	err := Call(context.Background(), points, views, rpc, resultCh)
	assert.NoError(err)
	if assert.Len(resultCh, 1) {
		assert.Equal(Chunk{
			View:    views.View(0),
			Offsets: []int{0},
			Result:  "world",
		}, <-resultCh)
		assert.NoError(closed(resultCh))
	}
}

func Test_Call_3points_1view(t *testing.T) {
	assert := assert.New(t)
	resultCh := make(chan Chunk, 4)
	// These are out of order to test that points get sorted correctly.
	points := []space.Point{
		space.Key("foo"),
		space.Key("bar"),
		space.Key("baz"),
	}
	views := NewViews([]View{NewTestView(space.Key("a"), space.Key("z"))})
	rpc := func(ctx context.Context, view View, offsets []int, results func(Result)) error {
		assert.Equal(views.View(0), view)
		// [points[i] for i in offsets] should be sorted
		assert.Equal([]int{1, 2, 0}, offsets)
		results("burger")
		return nil
	}
	err := Call(context.Background(), points, views, rpc, resultCh)
	assert.NoError(err)
	if assert.Len(resultCh, 1) {
		assert.Equal(Chunk{
			View:    views.View(0),
			Offsets: []int{1, 2, 0},
			Result:  "burger",
		}, (<-resultCh))
		assert.NoError(closed(resultCh))
	}
}

func Test_Call_1point_1view_rpcFail(t *testing.T) {
	assert := assert.New(t)
	resultCh := make(chan Chunk, 4)
	points := []space.Point{space.Key("hello")}
	views := NewViews([]View{NewTestView(space.Key("a"), space.Key("z"))})
	rpc := func(ctx context.Context, view View, offsets []int, results func(Result)) error {
		results("wor")
		return errors.New("failed after sending partial result")
	}
	err := Call(context.Background(), points, views, rpc, resultCh)
	assert.Error(err)
	assert.Regexp("failed after sending partial result", err.Error())
	if assert.Len(resultCh, 1) {
		assert.Equal(Chunk{
			View:    views.View(0),
			Offsets: []int{0},
			Result:  "wor",
		}, <-resultCh)
		assert.NoError(closed(resultCh))
	}
}

func Test_Call_1point_2views_retrySucceeds(t *testing.T) {
	assert := assert.New(t)
	resultCh := make(chan Chunk, 4)
	points := []space.Point{space.Key("hello")}
	views := NewViews([]View{
		NewTestView(space.Key("a"), space.Key("z")),
		NewTestView(space.Key("d"), space.Key("m")),
	})
	callCount := 0
	rpc := func(ctx context.Context, view View, offsets []int, results func(Result)) error {
		defer func() { callCount++ }()
		switch callCount {
		case 0:
			results("wor")
			return errors.New("failed after sending partial result")
		case 1:
			results("wor")
			results("ld")
			return nil
		default:
			assert.Fail("RPC called too many times")
			return nil
		}
	}
	err := Call(context.Background(), points, views, rpc, resultCh)
	assert.Nil(err)
	if assert.Len(resultCh, 3) {
		assert.Equal("wor", (<-resultCh).Result)
		assert.Equal("wor", (<-resultCh).Result)
		assert.Equal("ld", (<-resultCh).Result)
		assert.NoError(closed(resultCh))
	}
}

func Test_Call_1point_2views_retryFails(t *testing.T) {
	assert := assert.New(t)
	resultCh := make(chan Chunk, 4)
	points := []space.Point{space.Key("hello")}
	views := NewViews([]View{
		NewTestView(space.Key("a"), space.Key("z")),
		NewTestView(space.Key("d"), space.Key("m")),
	})
	callCount := 0
	rpc := func(ctx context.Context, view View, offsets []int, results func(Result)) error {
		defer func() { callCount++ }()
		switch callCount {
		case 0:
			results("wor")
			return errors.New("call 0 failed after sending wor")
		case 1:
			results("w")
			return errors.New("call 1 failed after sending w")
		default:
			assert.Fail("RPC called too many times")
			return nil
		}
	}
	err := Call(context.Background(), points, views, rpc, resultCh)
	assert.Error(err)
	assert.Regexp("failed after sending w$", err.Error())
	if assert.Len(resultCh, 2) {
		assert.Equal("wor", (<-resultCh).Result)
		assert.Equal("w", (<-resultCh).Result)
		assert.NoError(closed(resultCh))
	}
}

func Test_Call_3points_3views_ctxCanceled(t *testing.T) {
	assert := assert.New(t)
	resultCh := make(chan Chunk, 4)
	// These are out of order to test that points get sorted correctly.
	points := []space.Point{
		space.Key("foo"),
		space.Key("bar"),
		space.Key("baz"),
	}
	views := NewViews([]View{
		NewTestView(space.Key("a"), space.Key("z")),
		NewTestView(space.Key("d"), space.Key("m")),
		NewTestView(space.Key("m"), space.Key("t")),
	})
	rpc := func(ctx context.Context, view View, offsets []int, results func(Result)) error {
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err := Call(ctx, points, views, rpc, resultCh)
	assert.Error(err)
	assert.Regexp("deadline", err.Error())
	assert.NoError(closed(resultCh))
}

func Test_Call_100points_10views_viewAssignment(t *testing.T) {
	assert := assert.New(t)
	random.SeedMath()
	points := make([]space.Point, 100)
	for i := 0; i < 100; i++ {
		points[i] = space.Hash64(rand.Uint64())
	}
	// two random 5-way partitionings
	views := NewViews([]View{
		NewTestView(space.Hash64(0x0000000000000000), space.Hash64(0x18f2d898feed30fe)),
		NewTestView(space.Hash64(0x18f2d898feed30fe), space.Hash64(0x31ff8bb84a105436)),
		NewTestView(space.Hash64(0x31ff8bb84a105436), space.Hash64(0x6fd16b86ef54a101)),
		NewTestView(space.Hash64(0x6fd16b86ef54a101), space.Hash64(0xaa022072d49464ba)),
		NewTestView(space.Hash64(0xaa022072d49464ba), space.Infinity),
		NewTestView(space.Hash64(0x0000000000000000), space.Hash64(0x56a34cff19554e98)),
		NewTestView(space.Hash64(0x56a34cff19554e98), space.Hash64(0x7f15bdd4d35e844b)),
		NewTestView(space.Hash64(0x7f15bdd4d35e844b), space.Hash64(0x85db869a7af6b537)),
		NewTestView(space.Hash64(0x85db869a7af6b537), space.Hash64(0xff033be02bffbf8a)),
		NewTestView(space.Hash64(0xff033be02bffbf8a), space.Infinity),
	})
	rpc := func(ctx context.Context, view View, offsets []int, results func(Result)) error {
		log.Printf("view %v got %v points", view, len(offsets))
		var last space.Point = space.Hash64(0)
		for _, offset := range offsets {
			point := points[offset]
			assert.True(space.PointLeq(last, point))
			assert.True(view.Serves().Contains(point))
			last = point
			results(point)
		}
		return nil
	}
	resultCh := make(chan Chunk, 101)
	err := Call(context.Background(), points, views, rpc, resultCh)
	assert.NoError(err)
	saw := make(map[space.Point]struct{}, 100)
	for chunk := range resultCh {
		saw[chunk.Result.(space.Point)] = struct{}{}
	}
	assert.Len(saw, 100)
}

func Test_Call_1point_2views_hedge(t *testing.T) {
	assert := assert.New(t)
	mockClock := clocks.NewMock()
	clock = mockClock
	defer func() { clock = clocks.Wall }()
	resultCh := make(chan Chunk, 4)
	points := []space.Point{space.Key("hello")}
	views := NewViews([]View{
		NewTestView(space.Key("a"), space.Key("z")),
		NewTestView(space.Key("d"), space.Key("m")),
	})
	callCount := 0
	rpc := func(ctx context.Context, view View, offsets []int, results func(Result)) error {
		switch callCount {
		case 0:
			callCount++
			results("wor")
			log.Printf("Getting stuck for a while")
			mockClock.Advance(hedgeDuration + time.Nanosecond)
			<-ctx.Done()
			return ctx.Err()
		case 1:
			callCount++
			log.Printf("Backup request goes quickly")
			results("world")
			return nil
		default:
			assert.Fail("RPC called too many times")
			return nil
		}
	}
	err := Call(context.Background(), points, views, rpc, resultCh)
	assert.Nil(err)
	if assert.Len(resultCh, 2) {
		assert.Equal("wor", (<-resultCh).Result)
		assert.Equal("world", (<-resultCh).Result)
		assert.NoError(closed(resultCh))
	}
}

func Test_Call_2points_3views_wastedHedge(t *testing.T) {
	assert := assert.New(t)
	mockClock := clocks.NewMock()
	clock = mockClock
	defer func() { clock = clocks.Wall }()
	resultCh := make(chan Chunk, 4)
	points := []space.Point{
		space.Key("hello"),
		space.Key("x-ray"),
	}
	views := NewViews([]View{
		NewTestView(space.Key("a"), space.Key("x")),
		NewTestView(space.Key("d"), space.Key("m")),
		NewTestView(space.Key("x"), space.Key("z")),
	})
	callCount := 0
	ready := make(chan struct{})
	done := make(chan struct{})
	rpc := func(ctx context.Context, view View, offsets []int, results func(Result)) error {
		if offsets[0] == 1 { // just x-ray
			<-done
			return nil
		}
		switch callCount {
		case 0:
			callCount++
			results("wor")
			log.Printf("Getting stuck for a while")
			mockClock.Advance(hedgeDuration + time.Nanosecond)
			<-ready
			results("ld")
			close(done)
			return nil
		case 1:
			callCount++
			results("wo")
			close(ready)
			return errors.New("call 1 error")
		default:
			assert.Fail("RPC called too many times")
			return nil
		}
	}
	err := Call(context.Background(), points, views, rpc, resultCh)
	assert.Nil(err)
	if assert.Len(resultCh, 3) {
		assert.Equal("wor", (<-resultCh).Result)
		assert.Equal("wo", (<-resultCh).Result)
		assert.Equal("ld", (<-resultCh).Result)
		assert.NoError(closed(resultCh))
	}
}

func Test_viewFinder(t *testing.T) {
	assert := assert.New(t)
	f := newViewFinder(NewViews([]View{
		NewTestView(TestPoint(30), TestPoint(50)),
		NewTestView(TestPoint(10), TestPoint(20)),
		NewTestView(TestPoint(30), TestPoint(40)),
	}))
	assert.Equal("",
		viewsStr(f.Advance(TestPoint(8))))
	assert.Equal("",
		viewsStr(f.Advance(TestPoint(9))))
	assert.Equal("10-20",
		viewsStr(f.Advance(TestPoint(10))))
	assert.Equal("10-20",
		viewsStr(f.Advance(TestPoint(19))))
	assert.Equal("",
		viewsStr(f.Advance(TestPoint(20))))
	assert.Equal("",
		viewsStr(f.Advance(TestPoint(29))))
	assert.Equal("30-50 30-40",
		viewsStr(f.Advance(TestPoint(30))))
	assert.Equal("30-50 30-40",
		viewsStr(f.Advance(TestPoint(32))))
	assert.Equal("30-50 30-40",
		viewsStr(f.Advance(TestPoint(32))))
	assert.Equal("30-50",
		viewsStr(f.Advance(TestPoint(40))))
	assert.Equal("30-50",
		viewsStr(f.Advance(TestPoint(49))))
	assert.Equal("",
		viewsStr(f.Advance(TestPoint(50))))
	assert.Equal("",
		viewsStr(f.Advance(TestPoint(55))))
}

func Test_viewFinder2(t *testing.T) {
	assert := assert.New(t)
	f := newViewFinder(NewViews([]View{
		NewTestView(TestPoint(0), TestPoint(4)),
		NewTestView(TestPoint(4), TestPoint(8)),
		NewTestView(TestPoint(8), TestPoint(12)),
		NewTestView(TestPoint(12), TestPoint(16)),
		NewTestView(TestPoint(0), TestPoint(4)),
		NewTestView(TestPoint(4), TestPoint(8)),
		NewTestView(TestPoint(8), TestPoint(12)),
		NewTestView(TestPoint(12), TestPoint(16)),
	}))
	assert.Equal("0-4 0-4",
		viewsStr(f.Advance(TestPoint(0))))
	assert.Equal("4-8 4-8",
		viewsStr(f.Advance(TestPoint(4))))
	assert.Equal("8-12 8-12",
		viewsStr(f.Advance(TestPoint(8))))
	assert.Equal("12-16 12-16",
		viewsStr(f.Advance(TestPoint(12))))
}

func Test_NewViews(t *testing.T) {
	v1 := NewTestView(space.Key("a"), space.Key("d"))
	v2 := NewTestView(space.Key("d"), space.Key("x"))
	v := NewViews([]View{v1, v2})
	assert.Equal(t, 2, v.Len())
	assert.Equal(t, v1, v.View(0))
	assert.Equal(t, v2, v.View(1))
}
