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
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/logentry/logencoder"
	"github.com/ebay/akutan/partitioning"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/space"
	"github.com/ebay/akutan/util/cmp"
	errorUtil "github.com/ebay/akutan/util/errors"
	"github.com/ebay/akutan/util/parallel"
	"github.com/ebay/akutan/viewclient/fanout"
	"github.com/ebay/akutan/viewclient/viewreg"
	log "github.com/sirupsen/logrus"
)

var (
	errNoViewsToCarouselFrom = errors.New("no views found to handle carousel request")
)

// CarouselConsumer provides ways for a client of a carousel request to consume the resulting data
type CarouselConsumer interface {
	ReplayPosition() rpc.LogPosition
	DataCh() <-chan *rpc.CarouselResult
	ErrorsCh() <-chan error
	Stop()
}

type rpcCarouselConsumer struct {
	dataCh      chan *rpc.CarouselResult
	errors      chan error
	cancelFunc  func()
	completedWg sync.WaitGroup
	replayPos   rpc.LogPosition
}

// ReplayPosition returns the log Position that is the first position to
// read from the log once the carousel has finished.
func (cc *rpcCarouselConsumer) ReplayPosition() rpc.LogPosition {
	return cc.replayPos
}

// DataCh returns a channel that contains the snapshot data. Each item on the
// channel contains a list of Facts. Note that these appear in no specific
// order.
func (cc *rpcCarouselConsumer) DataCh() <-chan *rpc.CarouselResult {
	return cc.dataCh
}

// ErrorsCh returns a channel that reports any rpc errors while
// consuming the carousel
func (cc *rpcCarouselConsumer) ErrorsCh() <-chan error {
	return cc.errors
}

// Stop can be called to stop the carousel before its finished being consumed
func (cc *rpcCarouselConsumer) Stop() {
	cc.cancelFunc()
}

// LogStatusResult contains the calculated aggregate result of calling LogStatus on a set of views
type LogStatusResult struct {
	viewResults []*rpc.LogStatusResult
	viewErrors  []error
	byEncoding  []viewLogStates
}

type viewLogStates []viewLogState

type viewLogState struct {
	covers    space.Range
	nextPos   rpc.LogPosition
	replayPos rpc.LogPosition
}

func (v viewLogState) String() string {
	return fmt.Sprintf("{%s-%s nextPos=%v replayPos=%v}", v.covers.Start, v.covers.End, v.nextPos, v.replayPos)
}

func (v viewLogStates) Len() int {
	return len(v)
}

func (v viewLogStates) View(i int) fanout.View {
	return v[i]
}

func (v viewLogState) Serves() space.Range {
	return v.covers
}

// Results returns the underlying LogStatusResult and error for all the requested views
func (r *LogStatusResult) Results() ([]*rpc.LogStatusResult, []error) {
	return r.viewResults, r.viewErrors
}

func (r *LogStatusResult) processResults(views fanout.Views) {
	r.byEncoding = make([]viewLogStates, maxEncodingValue()+1)
	for i, res := range r.viewResults {
		if res != nil {
			enc := res.KeyEncoding
			r.byEncoding[enc] = append(r.byEncoding[enc], viewLogState{
				covers:    views.View(i).Serves(),
				nextPos:   res.NextPosition,
				replayPos: res.ReplayPosition,
			})
		}
	}
}

// ReplayPosition will return the oldest replay position across
// the full space range of all the encodings. If there are points
// that don't have any results it'll return an error.
func (r *LogStatusResult) ReplayPosition() (rpc.LogPosition, error) {
	pos, err := r.ReplayPositionInSpace(rpc.KeyEncodingPOS, fullRange)
	if err != nil {
		return rpc.LogPosition{}, err
	}
	pos2, err := r.ReplayPositionInSpace(rpc.KeyEncodingSPO, fullRange)
	return rpc.MinPosition(pos, pos2), err
}

// LastApplied will return the log index that all views have applied across the space
// range of all the encodings. If there are points not covered, it'll
// return an error.
func (r *LogStatusResult) LastApplied() (blog.Index, error) {
	pos1, err := r.NextPositionInSpace(rpc.KeyEncodingPOS, fullRange)
	if err != nil {
		return 0, err
	}
	pos2, err := r.NextPositionInSpace(rpc.KeyEncodingSPO, fullRange)
	if err != nil {
		return 0, err
	}
	return rpc.MinPosition(pos1, pos2).Index - 1, nil
}

// ReplayPositionInSpace will return the recommended replay position for the
// indicated range of the encoding space. If a point in this requested space
// does not have any valid results for it it'll return an error. If there are
// enough views (more than 2) for a partition, it'll ignore the worst value for
// that partition so that significant laggards or new views still catching up
// don't force processing of a lot of log. e.g. for the carousel its worth
// skipping the trailing view so that we can carousel at a more recent index
// requiring less catchup work once the carousel completes
func (r *LogStatusResult) ReplayPositionInSpace(enc rpc.FactKeyEncoding, covering space.Range) (rpc.LogPosition, error) {
	// we want to calculate the replay pos for each partition, ignoring the worst case
	// if there are enough views covering the partition. Then we take the worst case of all
	// the partitions as the result.
	return r.searchForValue(enc, covering, func(v *viewLogState) rpc.LogPosition {
		return v.replayPos
	}, func(positions []rpc.LogPosition) rpc.LogPosition {
		selectedOffset := len(positions) - 1
		if len(positions) > 2 {
			selectedOffset--
		}
		return positions[selectedOffset]
	})
}

// NextPositionInSpace will take the highest next position from all the views covering a partition, and then
// return the smallest of those across all the partition that cover the 'covering' range. At least one
// view in every relevant partition is at the returned index. If there are points in the requested range
// that have no coverage an error is returned.
func (r *LogStatusResult) NextPositionInSpace(enc rpc.FactKeyEncoding, covering space.Range) (rpc.LogPosition, error) {
	return r.searchForValue(enc, covering, func(v *viewLogState) rpc.LogPosition {
		return v.nextPos
	}, func(positions []rpc.LogPosition) rpc.LogPosition {
		return positions[0]
	})
}

// searchForValue will search the partition space to calculate the smallest value across the partitions.
// The actual value for each rpc result to be considered is generated by the provided valFunc function.
// The valSelector function will be passed a sorted (highest index first) list of values for a single partition
// and it can return the one it'd like to be applied for that partition. The overall result is then the
// minimum of the valSelector outputs for all the partitons that cover the suppled 'covering' range
func (r *LogStatusResult) searchForValue(
	enc rpc.FactKeyEncoding,
	covering space.Range,
	valFunc func(v *viewLogState) rpc.LogPosition,
	valSelector func([]rpc.LogPosition) rpc.LogPosition) (rpc.LogPosition, error) {

	views := r.byEncoding[enc]
	partitions := fanout.Partition(fullRange, views)
	partitionLogIndexes := make([][]rpc.LogPosition, len(partitions.StartPoints))
	for idx := range views {
		v := &views[idx]
		vIndexed := valFunc(v)
		pointIdx := partitions.Find(v.covers.Start)
		for ; pointIdx < len(partitions.StartPoints); pointIdx++ {
			point := partitions.StartPoints[pointIdx]
			if point.Less(v.covers.End) {
				partitionLogIndexes[pointIdx] = append(partitionLogIndexes[pointIdx], vIndexed)
			} else {
				break
			}
		}
	}
	overallResult := rpc.LogPosition{Index: math.MaxUint64}
	for pointIdx := range partitions.StartPoints {
		partRange := partitions.Get(pointIdx)
		if partRange.Overlaps(covering) {
			tx := partitionLogIndexes[pointIdx]
			if len(tx) == 0 {
				prevErr := errorUtil.Any(r.viewErrors...)
				var prevMsg string
				if prevErr != nil {
					prevMsg = fmt.Sprintf(". Some results missing (sample error: %v)", prevErr)
				}
				return rpc.LogPosition{}, fmt.Errorf("no view found to cover partition range %v-%v in %v%v",
					partRange.Start, partRange.End, enc, prevMsg)
			}
			sort.Slice(tx, func(a, b int) bool {
				return tx[b].Index < tx[a].Index
			})
			overallResult = rpc.MinPosition(overallResult, valSelector(tx))

		} else if covering.End.Less(partRange.Start) {
			break
		}
	}
	return overallResult, nil
}

func (r *LogStatusResult) encodingWithResultsThatsNot(e rpc.FactKeyEncoding) rpc.FactKeyEncoding {
	for enc, views := range r.byEncoding {
		if rpc.FactKeyEncoding(enc) != e && len(views) > 0 {
			return rpc.FactKeyEncoding(enc)
		}
	}
	return rpc.KeyEncodingUnknown
}

func maxEncodingValue() int32 {
	m := int32(0)
	for _, v := range rpc.FactKeyEncoding_value {
		m = cmp.MaxInt32(m, v)
	}
	return m
}

// LogStatus will call LogStatus on each of the supplied client views, and
// aggregate the results into an all clients wide view. For each normalized
// partition it collects the most recent atIndex and the oldest replay index
// then takes the overall min of each for an overall view.
func (c *Client) LogStatus(ctx context.Context, clients CarouselClients) (*LogStatusResult, error) {
	if clients.Len() == 0 {
		return nil, errNoViewsToCarouselFrom
	}
	res := LogStatusResult{
		viewResults: make([]*rpc.LogStatusResult, len(clients)),
		viewErrors:  make([]error, len(clients)),
	}
	thisCtx, cancelFunc := context.WithTimeout(ctx, time.Second*2)
	defer cancelFunc()
	req := &rpc.LogStatusRequest{}
	_ = parallel.InvokeN(thisCtx, clients.Len(), func(ctx context.Context, idx int) error {
		cc := clients[idx]
		res.viewResults[idx], res.viewErrors[idx] = cc.Stub.LogStatus(ctx, req)
		return nil // don't return err or InvokeN will cancel ctx
	})
	res.processResults(clients)
	return &res, nil
}

// RideCarousel will start a carousel client, fanning out the carousel request
// to the relevant source views. This will check with the views to find the most
// recent index where there are no pending transactions and start the carousel
// from that.
//
// if viewPred is not empty, then only views that pass this predicate will be
// included. For efficiency this will try to select views that use the same key
// encoding as the request, as that can reduce the number of views needed to
// fetch data from (because we can safely rule out that some view won't have any
// relevant data). However if needed it will fallback to Carouselling across the
// entire key space of an alternative key encoding.
func (c *Client) RideCarousel(ctx context.Context, req *rpc.CarouselRequest, viewPred ViewInfoPredicate) (CarouselConsumer, error) {
	requestedPartition, err := partitioning.PartitionFromCarouselHashPartition(req.HashPartition)
	if err != nil {
		return nil, err
	}
	carouselClients := c.CarouselViews(func(v viewreg.View) bool {
		if viewPred != nil && !viewPred(v) {
			return false
		}
		// does this view cover the requested dataset
		if v.Partition.Encoding() == req.HashPartition.Encoding {
			// same encoding type, see if there's overlap in the request ranges
			return v.Partition.HashRange().Overlaps(requestedPartition.HashRange())
		}
		// different encoding type, can fallback to this view
		return true
	})
	if carouselClients.Len() == 0 {
		return nil, errNoViewsToCarouselFrom
	}
	ctx, cancelFunc := context.WithCancel(ctx)
	// note we don't just defer cancelFunc() here because the context needs to last longer than
	// this function, as we're streaming data from the carousel services in other goroutines
	// the rpcCarouselConsumer will clean it up when the carousel completes
	logStatus, err := c.LogStatus(ctx, carouselClients)
	if err != nil {
		cancelFunc()
		return nil, err
	}
	fallbackReq := *req
	replayPosForMatchingEncoding, mainPosErr := logStatus.ReplayPositionInSpace(requestedPartition.Encoding(), requestedPartition.HashRange())
	replayPosForAltEncoding, altPosErr := logStatus.ReplayPositionInSpace(logStatus.encodingWithResultsThatsNot(requestedPartition.Encoding()), fullRange)
	req.MaxIndex = replayPosForMatchingEncoding.Index - 1 // yes, these are nonsense if there was an error, thats handled below
	fallbackReq.MaxIndex = replayPosForAltEncoding.Index - 1

	// TODO: is this really right, how should we distingush really being at 0 vs no results
	// if this is a fresh instance, then its valid that every view is at index 0
	// in which case there's nothing to do
	if req.MaxIndex <= 0 {
		cancelFunc()
		return newEmptyCarousel(), nil
	}
	// filter out any views that aren't at index we want, and split them up into
	// views that have the preferred encoding order, and the rest
	atIndexAndKeyEncoding := make(CarouselClients, 0, len(carouselClients)/2)
	atIndexOtherKeyEncoding := make(CarouselClients, 0, len(carouselClients)/2)
	for idx, cc := range carouselClients {
		r := logStatus.viewResults[idx]
		if r == nil {
			continue
		}
		if r.KeyEncoding == req.HashPartition.Encoding {
			if r.ReplayPosition.Index > req.MaxIndex && mainPosErr == nil {
				atIndexAndKeyEncoding = append(atIndexAndKeyEncoding, cc)
			}
		} else if r.ReplayPosition.Index > fallbackReq.MaxIndex && altPosErr == nil {
			atIndexOtherKeyEncoding = append(atIndexOtherKeyEncoding, cc)
		}
	}
	selectedPosition := replayPosForMatchingEncoding
	requests, err := selectCarouselRequests(atIndexAndKeyEncoding, requestedPartition.HashRange())
	if err != nil {
		selectedPosition = replayPosForAltEncoding
		requests, err = selectCarouselRequests(atIndexOtherKeyEncoding, fullRange)
		if err != nil {
			cancelFunc()
			return nil, err
		}
		req = &fallbackReq
	}
	log.Infof("Starting CarouselRequest with requests: %v", requests)

	res := rpcCarouselConsumer{
		replayPos:  selectedPosition,
		dataCh:     make(chan *rpc.CarouselResult, len(requests)*16),
		errors:     make(chan error, len(requests)),
		cancelFunc: cancelFunc,
	}
	res.completedWg.Add(len(requests))
	for idx := range requests {
		go func(cr *carouselRequest) {
			defer res.completedWg.Done()
			thisContext, cancelFunc := context.WithCancel(ctx)
			defer cancelFunc()
			thisReq := *req
			thisReq.HashPartition.Start = uint64(cr.reqRange.Start.(space.Hash64))
			thisReq.HashPartition.End = 0
			if cr.reqRange.End.Less(space.Infinity) {
				thisReq.HashPartition.End = uint64(cr.reqRange.End.(space.Hash64))
			}
			rideStream, err := cr.from.(*CarouselClient).Stub.Ride(thisContext, &thisReq)
			if err != nil {
				log.Warnf("Carousel ride error: %v", err)
				res.errors <- err
				return
			}
			for {
				next, err := rideStream.Recv()
				if err == io.EOF {
					return
				}
				if err != nil {
					log.Warnf("Carousel ride error: %v", err)
					res.errors <- err
					return
				}
				res.dataCh <- next
			}
		}(&requests[idx])
	}
	go func() {
		res.completedWg.Wait()
		close(res.dataCh)
	}()
	return &res, nil
}

func newEmptyCarousel() *emptyCarousel {
	r := emptyCarousel{
		data: make(chan *rpc.CarouselResult),
		err:  make(chan error),
	}
	close(r.data)
	return &r
}

type emptyCarousel struct {
	data chan *rpc.CarouselResult
	err  chan error
}

func (c *emptyCarousel) ReplayPosition() rpc.LogPosition {
	return logencoder.LogInitialPosition()
}

func (c *emptyCarousel) DataCh() <-chan *rpc.CarouselResult {
	return c.data
}

func (c *emptyCarousel) ErrorsCh() <-chan error {
	return c.err
}

func (c *emptyCarousel) Stop() {
}
