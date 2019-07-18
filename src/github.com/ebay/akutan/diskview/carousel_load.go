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
	"context"
	"fmt"
	"time"

	"github.com/ebay/akutan/diskview/keys"
	"github.com/ebay/akutan/partitioning"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/viewclient"
	"github.com/ebay/akutan/viewclient/viewreg"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

// carouselCatchup will update this diskview from the carousel. This is used
// when a diskview comes up and its local copy is at a point that has been
// truncated away in the log. nextPosition is set before carouselCatchup returns
// so that it can be used to start the log processing.
func (view *DiskView) carouselCatchup() error {
	viewClient, err := viewclient.New(view.cfg)
	if err != nil {
		log.Fatalf("Unable to initialize carousel view client: %v", err)
	}
	defer viewClient.Close()

	// We first want to wait until the registry has found all the views form the
	// current space that it's likely to find. This will avoid needlessly using
	// the fallback (other) space.
	//
	// TODO: It would be better to split RideCarousel in two, so we can
	// aggressively attempt to ride a carousel in the current space, then wait
	// for the registry, and only if necessary switch to the fallback space.
	logrus.Info("Waiting until view registry is ready")
	// It's safe to ignore the returned error: the only error from
	// WaitForStable() is ctx.Err(), and context.TODO() won't ever expire.
	_ = viewClient.Registry.WaitForStable(context.TODO(), 5*time.Second)
	summary := viewClient.Registry.Summary()
	logrus.WithFields(summary).Info("Assuming view registry is ready")

	var cc viewclient.CarouselConsumer
	creq := rpc.CarouselRequest{
		MinIndex:      view.nextPosition.Index,
		HashPartition: partitioning.CarouselHashPartition(view.partition),
	}
	excludeSelf := func(v viewreg.View) bool {
		// TODO: This won't exclude anything under Kubernetes, where
		// view.cfg.DiskView.GRPCAddress only contains ":9980".
		return v.Endpoint.HostPort() != view.cfg.DiskView.GRPCAddress
	}
	// when running locally, many of the view are still starting up, so we need to handle
	// the case where everything is not ready to start carouseling
	for tries := 0; cc == nil && tries < 10; tries++ {
		cc, err = viewClient.RideCarousel(context.TODO(), &creq, excludeSelf)
		if err != nil {
			log.Infof("Unable to initialize carousel consumer: %v", err)
			time.Sleep(time.Second)
			continue
		}
	}
	if err != nil {
		return fmt.Errorf("giving up after 10 tries trying to start a carousel ride: %v", err)
	}
	log.Infof("Starting Carousel Ride through log index %v", cc.ReplayPosition().Index-1)
	factCount := 0
	start := time.Now()
	view.lock.Lock()
	defer view.lock.Unlock()

	writer := view.db.BulkWrite()
	defer writer.Close()
	for {
		select {
		case item, chOpen := <-cc.DataCh():
			if !chOpen {
				err := writer.Close()
				dur := time.Since(start)
				log.Infof("RideCarousel completed, received %d facts, in %v %v", factCount, dur, err)
				if err != nil {
					log.Fatalf("Error commiting K/V store tx: %v", err)
				}
				view.nextPosition = cc.ReplayPosition()
				view.graphCounts.haveWritten()
				if err := view.writeMeta(); err != nil {
					log.Warnf("Unable to persist updated meta: %v", err)
					// We don't return this error out of the carousel as that'll just end up carouseling
					// again, which isn't going to help. The diskview will try and persist meta again
					// once it starts processing the log.
				}
				return nil
			}
			for idx := range item.Facts {
				fact := item.Facts[idx]
				if view.partition.HasFact(&fact) {
					key := keys.FactKey{Fact: &fact, Encoding: view.partition.Encoding()}.Bytes()
					if err := writer.Buffer(key, nil); err != nil {
						log.Fatalf("Error writing to K/V store: %v", err)
					}
					factCount++
				}
			}
			if factCount%10000 == 0 {
				log.Infof("Ride in progress saw %d fact", factCount)
			}

		case err := <-cc.ErrorsCh():
			cc.Stop()
			log.Fatalf("Carousel Error!: %v", err)
		}
	}
}
