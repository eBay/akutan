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

// Package fanout is useful for invoking RPCs across a bunch of servers. It
// knows how to collect items together into fewer requests, how to retry
// failed requests, and how to hedge requests to mask slow servers.
package fanout

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/ebay/akutan/space"
	"github.com/ebay/akutan/util/clocks"
	"github.com/ebay/akutan/util/random"
	log "github.com/sirupsen/logrus"
)

// A View is a server that hosts some continuous range of a space.
type View interface {
	// Serves returns what is hosted on the server.
	Serves() space.Range
}

// Views is a list of View, its abstracted so that callers don't have
// to keep converting their types into []View
type Views interface {
	// Len returns the number of Views in the list
	Len() int
	// View returns the View at supplied index into the list.
	// the Index -> View relationship should be stable for the lifetime
	// of the Views instance
	View(index int) View
}

// RPC synchronously invokes an RPC for a set of items.
//
// 'offsets' identify which items to include in the request. They correspond to
// the indices of the 'points' slice given to Call(). The function must not
// modify offsets.
//
// Any responses should be returned by invoking the results callback. It may be
// invoked multiple times to support streaming RPCs where one request can have
// many responses. Note that the callback may block to apply backpressure.
type RPC func(ctx context.Context, view View, offsets []int, results func(Result)) error

// A Result is a reply from an RPC. The caller will need to cast this
// appropriately.
type Result interface{}

// Chunk contains a single result from an RPC. Streaming RPCs may return
// multiple results, corresponding to multiple chunks.
type Chunk struct {
	// Same as passed to the RPC.
	View View
	// Same as passed to the RPC.
	Offsets []int
	// A result produced by the RPC.
	Result Result
}

type views []View

func (v views) Len() int {
	return len(v)
}

func (v views) View(at int) View {
	return v[at]
}

// NewViews will return an instance of the Views interface backed by the provided
// list of View.
func NewViews(v []View) Views {
	return views(v)
}

// Call sends out RPCs for a particular set of items.
//
// 'points' describe where each item fits in the key-space/hash-space. 'views'
// is a set of servers in the same key-space/hash-space. Note that Call() may
// modify the 'views' slice. 'rpc' will be called many times to fetch results.
//
// RPC results are sent to 'results' in no particular order. This channel is
// closed before this function returns. The caller may apply backpressure by
// reading slowly from this channel. Note that the caller must be prepared to
// receive duplicate results for some items; this can happen when servers are
// slow to respond or return errors after producing some results.
//
// Call returns an error if it could not successfully complete an RPC for
// every item. It will not return until all invoked RPCs have returned.
func Call(
	ctx context.Context,
	points []space.Point,
	views Views,
	rpc RPC,
	results chan<- Chunk,
) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	waitForRPCs := func() {}
	defer func() {
		cancelFunc()
		waitForRPCs()
		close(results)
	}()

	// Completions and errors from RPCs are sent back here, to be processed in the
	// main loop.
	completionCh := make(chan discreteCompletion)
	// If RPCs are going slow, we try to kick off more. This is the next time that
	// should be done (the minimum time for any item).
	nextHedgeAt := clock.Now().Add(aLongTime)

	// doRPC synchronously calls an RPC, then sends on 'completionCh'.
	doRPC := func(view View, items []*discreteItem) {
		offsets := make([]int, len(items))
		for i, item := range items {
			offsets[i] = item.offset
		}
		withResult := func(result Result) {
			select {
			case results <- Chunk{
				View:    view,
				Offsets: offsets,
				Result:  result,
			}:
			case <-ctx.Done():
			}
		}
		err := rpc(ctx, view, offsets, withResult)
		completionCh <- discreteCompletion{
			view:  view,
			items: items,
			err:   err,
		}
	}

	// send asynchronously transmits requests for the given items. The items given
	// are sorted by increasing point value.
	send := func(items []*discreteItem) {
		for len(items) > 0 {
			item := items[0]
			view := item.popRandomView()
			// If this view is an option to serve items[1] and onwards, we'll include those
			// in this request.
			j := 1
			for j < len(items) {
				if !items[j].removeView(view) {
					break
				}
				j++
			}
			hedgeAt := clock.Now().Add(hedgeDuration)
			if nextHedgeAt.After(hedgeAt) {
				nextHedgeAt = hedgeAt
			}
			for _, item := range items[:j] {
				item.hedgeAt = hedgeAt
				item.outstanding++
			}
			go doRPC(view, items[:j])
			items = items[j:]
		}
	}

	// Each point represents a work item, which gets tracked in 'allItems'. allItems
	// is sorted by increasing point value for efficiency in assigning views.
	allItems := make([]*discreteItem, len(points))
	for i := range points {
		allItems[i] = &discreteItem{
			offset: i,
		}
	}
	sort.Slice(allItems, func(i, j int) bool {
		return points[allItems[i].offset].Less(points[allItems[j].offset])
	})
	// This loop assigns the possible views to each item. It's done after sorting
	// items because viewFinder needs points in increasing order.
	viewFinder := newViewFinder(views)
	for _, item := range allItems {
		point := points[item.offset]
		item.options = viewFinder.Advance(point)
		if len(item.options) == 0 {
			// TODO: this message should identify the space with the missing point, but that
			// context isn't currently known here.
			return fmt.Errorf("no server contains point %v", point)
		}
	}

	// This runs after the context is canceled. It waits for all the outstanding
	// RPCs to finish.
	waitForRPCs = func() {
		totalOutstanding := 0
		for _, item := range allItems {
			totalOutstanding += item.outstanding
		}
		for totalOutstanding > 0 {
			completion := <-completionCh
			for range completion.items {
				totalOutstanding--
			}
		}
	}

	// Send out the initial RPCs. If these succeed, no more will be needed. However,
	// if these are slow or fail, we'll send more later.
	send(allItems)

	// This is the main event loop. It runs until every item has a result.
	done := 0                      // Number of items that have results.
	hedgeAlarm := clock.NewAlarm() // Wakes up the event loop to send backup requests.
	defer hedgeAlarm.Stop()
	for done < len(allItems) {
		// The following select might sleep, so make sure the timer is set up correctly.
		hedgeAlarm.Set(nextHedgeAt)
		select {

		// An RPC completed.
		case completion := <-completionCh:
			for _, item := range completion.items {
				item.outstanding--
			}
			if completion.err == nil {
				for _, item := range completion.items {
					if !item.done {
						item.done = true
						done++
					}
				}
			} else {
				log.Warnf("RPC to %v failed: %v", completion.view, completion.err)
				// Update bookkeeping and try to send requests to alternate servers.
				toSend := make([]*discreteItem, 0, len(completion.items))
				for _, item := range completion.items {
					if item.done {
						continue
					}
					if len(item.options) == 0 {
						if item.outstanding == 0 {
							return completion.err
						}
					} else {
						toSend = append(toSend, item)
					}
				}
				send(toSend)
			}

		// The hedge timer fired.
		case <-hedgeAlarm.WaitCh():
			// Update nextHedgeAt and try to send more requests.
			var toSend []*discreteItem
			now := clock.Now()
			nextHedgeAt = now.Add(aLongTime)
			for _, item := range allItems {
				if item.done || len(item.options) == 0 {
					continue
				}
				if item.hedgeAt.Before(now) {
					toSend = append(toSend, item)
				} else {
					if nextHedgeAt.After(item.hedgeAt) {
						nextHedgeAt = item.hedgeAt
					}
				}
			}
			send(toSend)

		// The context was canceled.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// clock is the source of time for this package. It may be overridden by unit
// tests.
var clock = clocks.Wall

// hedgeDuration is enough time for a healthy server to reply to an RPC. The
// Discrete function uses a timer for hedging requests. If a request is going
// slow and other servers are available, Discrete will re-send the request after
// 'hedgeDuration'.
//
// We may want this to be either configurable or automatically determined in the
// future.
const hedgeDuration = time.Second / 2

// aLongTime is a nearly infinite duration used with the hedge timer. There are
// two reasons to prefer this over the largest possible duration value. First,
// using a really large duration value can potentially lead to integer overflow.
// Second, if we accidentally leak a timer goroutine, we'd like it to fire soon
// and clean itself up. This shouldn't happen, but seriously, the docs on
// time.Timer are so scary and awful.
const aLongTime = time.Minute

// A discreteItem is a single thing with a point value that needs to go into an
// RPC.
type discreteItem struct {
	// The index into the 'points' slice from which this originated.
	// points[item.offset] is the item's point (usually a hash or key).
	offset int
	// If true, we already have results for the item; there's nothing left to do
	// with it.
	done bool
	// The set of servers that host this item's point, excluding those we've already
	// requested this item from.
	options []View
	// The number of RPCs that have been sent for this item and haven't yet
	// completed or failed.
	outstanding int
	// If the item isn't done at this time and we have more options, we should send
	// another RPC.
	hedgeAt time.Time
}

func init() {
	// popRandomView uses math/rand.
	random.SeedMath()
}

// Ugly SliceTrick to pop a random view from item.options.
func (item *discreteItem) popRandomView() View {
	i := rand.Intn(len(item.options))
	v := item.options[i]
	item.options[i] = item.options[len(item.options)-1]
	item.options = item.options[:len(item.options)-1]
	return v
}

// Ugly SliceTrick for remove on item.options. Does not preserve order.
func (item *discreteItem) removeView(view View) bool {
	for i := range item.options {
		if item.options[i] == view {
			item.options[i] = item.options[len(item.options)-1]
			item.options = item.options[:len(item.options)-1]
			return true
		}
	}
	return false
}

// A discreteCompletion describes the outcome of an RPC. Any RPC, even a
// streaming RPC, will result in exactly one completion.
type discreteCompletion struct {
	view  View
	items []*discreteItem
	err   error
}

// A viewFinder is used in assiging possible views to items.
type viewFinder struct {
	// The views with startPoint > currentPoint, sorted by increasing point value.
	views []View
	// Monotonically increasing point as given to Advance.
	currentPoint space.Point
	// Views preceding idx that include currentHash.
	open []View
}

func newViewFinder(views Views) viewFinder {
	f := viewFinder{}
	if views != nil {
		f.views = make([]View, views.Len())
		for i := 0; i < views.Len(); i++ {
			f.views[i] = views.View(i)
		}
	}
	// These need to be sorted by increasing start point. Using a stable sort isn't
	// necessary for the main code but is handy for unit tests.
	sort.SliceStable(f.views, func(i, j int) bool {
		irange := f.views[i].Serves()
		jrange := f.views[j].Serves()
		return irange.Start.Less(jrange.Start)
	})
	return f
}

// Advance returns the set of views that host 'point'. It must be called in
// increasing 'point' order. The caller may modify the returned slice.
func (f *viewFinder) Advance(point space.Point) []View {
	if f.currentPoint != nil && space.PointGt(f.currentPoint, point) {
		log.Panicf("Expected increasing point values but got %v then %v",
			f.currentPoint, point)
	}
	f.currentPoint = point

	// Move prefix of views to 'open' so that views[0] starts past currentPoint.
	for len(f.views) > 0 && space.PointLeq(f.views[0].Serves().Start, f.currentPoint) {
		f.open = append(f.open, f.views[0])
		f.views = f.views[1:]
	}

	// Filter f.open to include only views having currentPoint.
	wereOpen := f.open
	f.open = f.open[:0]
	for _, o := range wereOpen {
		if space.PointLt(f.currentPoint, o.Serves().End) {
			f.open = append(f.open, o)
		}
	}

	// Return a copy of f.open, since the next call to Advance will mutate f.open.
	return append([]View(nil), f.open...)
}
