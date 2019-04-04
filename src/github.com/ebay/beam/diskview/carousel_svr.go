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

package diskview

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ebay/beam/blog"
	"github.com/ebay/beam/diskview/database"
	"github.com/ebay/beam/diskview/keys"
	"github.com/ebay/beam/logentry/logencoder"
	"github.com/ebay/beam/partitioning"
	"github.com/ebay/beam/rpc"
	log "github.com/sirupsen/logrus"
)

// carouselChunkSize controls both the size of the chunks coming out of the carousel as well as the size
// of the RPC chunks returned from ride [having the 2 aligned helps if the ride is not filtering anything]
const carouselChunkSize = 1024

type carouselConductor struct {
	db         database.DB
	newRiderCh chan *carouselRider

	// protects stats
	statsMutex sync.Mutex
	stats      carouselStats
}

type carouselStats struct {
	numRiders uint32
	lastKey   string
}

type carouselItem struct {
	parsedKey interface{} // see keys.go
	key       []byte
	value     []byte
}

func (i *carouselItem) String() string {
	if len(i.key) > 0 {
		return string(i.key)
	}
	return i.parsedKey.(fmt.Stringer).String()
}

type carouselItems []carouselItem

// last returns the last item in this chunk of items, or nil if there are no items
func (ci carouselItems) last() *carouselItem {
	if len(ci) == 0 {
		return nil
	}
	return &ci[len(ci)-1]
}

func (ci carouselItems) lastKey() string {
	i := ci.last()
	if i == nil {
		return ""
	}
	return i.String()
}

// the conductor will put a first horse & last horse key on the dataCh stream to deliniate
// the overrall ride

type markerType uint8

const firstHorse markerType = 1
const lastHorse markerType = 2

func (t markerType) String() string {
	switch t {
	case firstHorse:
		return "FirstHorse"
	case lastHorse:
		return "LastHorse"
	}
	return "Unkown markerType"
}

type markerKey struct {
	marker markerType
}

func (m markerKey) String() string {
	return "[Marker:" + m.marker.String() + "]"
}

type carouselRider struct {
	itemCount          int64
	dismountNow        int32
	firstSawItem       *carouselItem // this is the first item from the source, we use it to determine when to "jump on" and start passing along items, we want to start on an node or edge boundary
	firstProcessedItem *carouselItem // this is the first item this rider processed and sent along its way, used to determine when the rider should get off
	dataCh             chan carouselItems
	dismountCh         chan struct{}
	errorCh            chan error
}

func (cc *carouselConductor) init(db database.DB) {
	cc.db = db
	cc.newRiderCh = make(chan *carouselRider, 6)
	go cc.conduct()
}

func (cc *carouselConductor) getStats() carouselStats {
	cc.statsMutex.Lock()
	defer cc.statsMutex.Unlock()
	return cc.stats // (a copy)
}

func (cc *carouselConductor) addRider() *carouselRider {
	rider := carouselRider{
		dataCh:     make(chan carouselItems, 32),
		dismountCh: make(chan struct{}, 1),
		errorCh:    make(chan error, 1),
	}
	cc.newRiderCh <- &rider
	return &rider
}

type carouselRiders []*carouselRider

func (riders carouselRiders) add(newRider *carouselRider) carouselRiders {
	for i := range riders {
		if riders[i] == nil {
			riders[i] = newRider
			return riders
		}
	}
	return append(riders, newRider)
}

// trim removes any empty slots, it may change the order of the riders within the list
func (riders carouselRiders) trim() carouselRiders {
	for i := 0; i < len(riders); {
		if riders[i] == nil {
			riders[i] = riders[len(riders)-1]
			riders = riders[:len(riders)-1]
		} else {
			i++
		}
	}
	return riders
}

func (riders carouselRiders) consume(items carouselItems) carouselRiders {
	someoneDismounted := false
	for i := range riders {
		if riders[i] != nil {
			if !riders[i].consume(items) {
				riders[i] = nil
				someoneDismounted = true
			}
		}
	}
	if someoneDismounted {
		riders = riders.trim()
		log.Infof("rider(s) got off, riders now: %v", riders)
	}
	return riders
}

func (riders carouselRiders) consumeErr(e error) {
	for i := range riders {
		riders[i].consumeErr(e)
	}
}

func (riders carouselRiders) isActive() bool {
	return len(riders) > 0
}

func isLastHorse(ci carouselItems) bool {
	if len(ci) != 1 {
		return false
	}
	mkey, isMarker := ci[0].parsedKey.(markerKey)
	return isMarker && (mkey.marker == lastHorse)
}

func (cc *carouselConductor) setNumRiders(num int) {
	cc.statsMutex.Lock()
	cc.stats.numRiders = uint32(num)
	cc.statsMutex.Unlock()
}

func (cc *carouselConductor) conduct() {
	riders := make(carouselRiders, 0, 4)
	cc.setNumRiders(len(riders))

	running := false
	count := uint64(0)
	emptyCarousel := &carouselRide{} // all the channel are nil, so they're safe to read from in the select [it'll never read from it]
	currentRide := emptyCarousel
	for {
		select {
		case newRider := <-cc.newRiderCh:
			riders = riders.add(newRider)
			cc.setNumRiders(len(riders))
			if !running {
				// try and get any remaining queued new riders on now, hopefully everyone can get on at firstHorse
				// and save a second spin
				time.Sleep(time.Millisecond * 5) // allow time for other riders to turn up
				for {
					select {
					case newRider = <-cc.newRiderCh:
						riders = riders.add(newRider)
						cc.setNumRiders(len(riders))
						continue
					default:
					}
					break
				}
				running = true
				currentRide = cc.spinOneTime()
			}
			log.Infof("New rider(s): riders now: %v", riders)

		case ci := <-currentRide.dataCh:
			riders = riders.consume(ci)
			count++
			empty := !riders.isActive()
			last := isLastHorse(ci)
			if last || empty || count%400 == 0 {
				cc.statsMutex.Lock()
				cc.stats.numRiders = uint32(len(riders))
				cc.stats.lastKey = ci.lastKey()
				cc.statsMutex.Unlock()
			}
			if last {
				log.Infof("Conductor saw lastHorse! riders: %v", riders)
				if riders.isActive() {
					// still some riders left, go again
					currentRide = cc.spinOneTime()
				} else {
					currentRide = emptyCarousel
					running = false
				}
				riders = riders.trim()
				cc.setNumRiders(len(riders))
			} else if empty {
				log.Infof("Last rider dismounted, stopping carousel early @ %v", ci.lastKey())
				currentRide.stop()
				currentRide = emptyCarousel
				running = false
			}

		case e := <-currentRide.errorCh:
			riders.consumeErr(e)
		}
	}
}

type carouselRide struct {
	dataCh  chan carouselItems
	errorCh chan error
	stopCh  chan struct{}
}

func (r *carouselRide) stop() {
	close(r.stopCh)
}

func (cc *carouselConductor) spinOneTime() *carouselRide {
	res := carouselRide{
		dataCh:  make(chan carouselItems, 128),
		errorCh: make(chan error, 1),
		stopCh:  make(chan struct{}),
	}
	go func() {
		res.dataCh <- carouselItems{{parsedKey: markerKey{firstHorse}}}
		snap := cc.db.Snapshot()
		defer snap.Close()
		stoppedShort := false
		chunk := make(carouselItems, 0, carouselChunkSize)
		err := snap.Enumerate(nil, []byte{0xFF}, func(key []byte, value []byte) error {
			pk, err := keys.ParseKey(key)
			if err != nil {
				log.Warnf("Unable to parse stored key: %v", err)
				return nil
			}
			if _, isFactKey := pk.(keys.FactKey); !isFactKey {
				return nil // if it's not a FactKey, skip it
			}
			ci := carouselItem{
				parsedKey: pk,
				key:       key,
				value:     value,
			}
			chunk = append(chunk, ci)
			if len(chunk) == carouselChunkSize {
				select {
				case <-res.stopCh:
				case res.dataCh <- chunk:
				}
				chunk = make(carouselItems, 0, carouselChunkSize)
			}
			select {
			case <-res.stopCh:
				stoppedShort = true
				return database.ErrHalt
			default:
				return nil
			}
		})
		if err != nil {
			res.errorCh <- err
			return
		}
		if !stoppedShort {
			if len(chunk) > 0 {
				res.dataCh <- chunk
			}
			res.dataCh <- carouselItems{{parsedKey: markerKey{lastHorse}}}
		}
	}()
	return &res
}

// consume these items from the carousel, return true if you want to stay on, false if you're ready to dismount
func (r *carouselRider) consume(items carouselItems) bool {
	dismounting := false
	if r.firstProcessedItem == nil {
		for idx := range items {
			item := &items[idx]
			if r.shouldJumpOn(item) {
				r.firstProcessedItem = item
				log.Infof("carouselRider getting on at %s", item)
				items = items[idx:]
				break
			}
		}
		if r.firstProcessedItem == nil {
			return true
		}
	} else {
		for idx := range items {
			item := &items[idx]
			if r.shouldDismount(item) {
				log.Infof("carouselRider getting off at %s", item)
				items = items[:idx]
				dismounting = true
				break
			}
		}
	}
	if len(items) > 0 {
		r.itemCount += int64(len(items))
		select {
		case r.dataCh <- items:
		case <-r.dismountCh:
			log.Infof("carouselRider getting off at %s", items[0])
			dismounting = true
		}
	}
	if dismounting {
		close(r.dataCh)
		return false
	}
	return true
}

func (r *carouselRider) consumeErr(e error) {
	r.errorCh <- e
}

// shouldJumpOn returns true if its seen a distinct fact boundary [so that consumes always see log order per key]
// as a special case, if we're at the firstHorse we can get on
func (r *carouselRider) shouldJumpOn(cur *carouselItem) bool {
	if cm, ok := cur.parsedKey.(markerKey); ok && cm.marker == firstHorse {
		return true
	}
	if r.firstSawItem == nil {
		r.firstSawItem = cur
		return false
	}
	return !keys.FactKeysEqualIgnoreIndex(r.firstSawItem.key, cur.key)
}

// if current has looped back around to our first, or as a special case, if first == firstHourse & cur == lastHorse
// its time to get off
func (r *carouselRider) shouldDismount(cur *carouselItem) bool {
	if atomic.LoadInt32(&r.dismountNow) == 1 {
		return true
	}
	// TODO: profile this, the type switch will always return the same thing for the lifetime of the rider
	// so it sucks we're doing it for every item.
	switch tf := r.firstProcessedItem.parsedKey.(type) {
	case markerKey:
		if cm, ok := cur.parsedKey.(markerKey); ok {
			return tf.marker == cm.marker || (tf.marker == firstHorse && cm.marker == lastHorse)
		}
		return false
	default:
		return bytes.Equal(cur.key, r.firstProcessedItem.key)
	}
}

func (r *carouselRider) DataCh() chan carouselItems {
	return r.dataCh
}

func (r *carouselRider) ErrorCh() chan error {
	return r.errorCh
}

func (r *carouselRider) dismount() {
	if atomic.CompareAndSwapInt32(&r.dismountNow, 0, 1) {
		r.dismountCh <- struct{}{}
	}
}

func resultIsReady(r *rpc.CarouselResult) bool {
	return len(r.Facts) >= carouselChunkSize
}

func resultNeedsSending(r *rpc.CarouselResult) bool {
	return len(r.Facts) > 0
}

func (view *DiskView) includeCarouselFact(fk *keys.FactKey, minIndex, maxIndex blog.Index, p partitioning.FactPartition) bool {
	// min/max is inclusive
	if fk.Fact.Index < minIndex || fk.Fact.Index > maxIndex {
		return false
	}
	return p.HasFact(fk.Fact)
}

type rider interface {
	DataCh() chan carouselItems
	ErrorCh() chan error
	dismount()
}

// Ride implements the gRPC Ride request, allowing a caller to see all (or some subset) of data
// from this diskview. Sets of facts are streamed to caller, multiple concurrent Ride's will
// share a single iteration of the underling database.
func (view *DiskView) Ride(req *rpc.CarouselRequest, results rpc.Carousel_RideServer) error {
	if req.MaxIndex == 0 {
		req.MaxIndex = view.LastApplied()
	}
	log.Debugf("Carousel.Ride %+v", req)
	var rider rider
	if view.cfg.DiskView.DisableCarousel || req.Dedicated {
		log.Info("Starting Dedicated Carousel Ride")
		rider = newDedicatedRider(view.carousel.spinOneTime())
	} else {
		rider = view.carousel.addRider()
	}
	nextRes := rpc.CarouselResult{
		Facts: make([]rpc.Fact, 0, carouselChunkSize),
	}
	dataCh := rider.DataCh()
	errorCh := rider.ErrorCh()
	requestedPartition, err := partitioning.PartitionFromCarouselHashPartition(req.HashPartition)
	if err != nil {
		return err
	}
	for {
		select {
		case <-results.Context().Done():
			log.Infof("Context cancelled, stopping ride: %v", results.Context().Err())
			rider.dismount()
			return results.Context().Err()

		case citems, chOpen := <-dataCh:
			if !chOpen {
				if resultNeedsSending(&nextRes) {
					results.Send(&nextRes)
				}
				return nil
			}
			for _, ci := range citems {
				switch tk := ci.parsedKey.(type) {
				case keys.FactKey:
					if view.includeCarouselFact(&tk, req.MinIndex, req.MaxIndex, requestedPartition) {
						nextRes.Facts = append(nextRes.Facts, *tk.Fact)
					}
				}
				if resultIsReady(&nextRes) {
					results.Send(&nextRes)
					nextRes.Facts = nextRes.Facts[:0]
				}
			}

		case err := <-errorCh:
			log.Warnf("Ride error: %v", err)
			rider.dismount()
			return err
		}
	}
}

// LogStatus returns a summary of the current log processing state of this diskview,
// it includes both the current applied log index, as well as the log index of the oldest pending
// we're tracking. If there are no outstanding transactions OldestPendingTxnIndex will return
// the same value as AtIndex.
func (view *DiskView) LogStatus(ctx context.Context, r *rpc.LogStatusRequest) (*rpc.LogStatusResult, error) {
	view.lock.RLock()
	defer view.lock.RUnlock()
	res := rpc.LogStatusResult{
		NextPosition:        view.nextPosition,
		KeyEncoding:         view.partition.Encoding(),
		MaxVersionSupported: logencoder.MaxVersionSupported,
		ReplayPosition:      view.nextPosition,
	}
	for _, tx := range view.txns {
		res.ReplayPosition = rpc.MinPosition(res.ReplayPosition, tx.position)
	}
	if res.NextPosition == logencoder.LogInitialPosition() {
		// is this really a valid empty cluster, or is this a new instance in an existing cluster, see where the actual log is at
		info, err := view.beamLog.Info(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to determine tail of log: %v", err)
		}
		if info.LastIndex > 0 {
			return nil, fmt.Errorf("view is still initializing and not valid")
		}
	}
	return &res, nil
}

type dedicatedRider struct {
	ride   *carouselRide
	dataCh chan carouselItems
}

func newDedicatedRider(r *carouselRide) rider {
	dr := dedicatedRider{
		ride:   r,
		dataCh: make(chan carouselItems, 32),
	}
	go func() {
		for {
			ci, open := <-dr.ride.dataCh
			if !open || isLastHorse(ci) {
				close(dr.dataCh)
				return
			}
			dr.dataCh <- ci
		}
	}()
	return &dr
}

func (r *dedicatedRider) DataCh() chan carouselItems {
	return r.dataCh
}

func (r *dedicatedRider) ErrorCh() chan error {
	return r.ride.errorCh
}

func (r *dedicatedRider) dismount() {
	r.ride.stop()
	close(r.ride.dataCh)
}
