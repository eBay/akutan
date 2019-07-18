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

package main

import (
	"time"

	"github.com/ebay/akutan/rpc"
)

type rideSink struct {
	writeFact func(*rpc.Fact)
	close     func()

	start     time.Time
	printFreq time.Duration

	facts        uint64
	msgs         uint64
	bytes        uint64
	lastProgress time.Time
}

func newRideSink(o *options) (s rideSink, err error) {
	s = rideSink{}
	s.writeFact, s.close, err = o.factSink()
	if err != nil {
		return s, err
	}
	s.printFreq = o.progressInterval()
	s.start = time.Now()
	return s, nil
}

func (s *rideSink) add(next *rpc.CarouselResult) {
	now := time.Now()
	s.msgs++
	s.facts += uint64(len(next.Facts))
	for idx := range next.Facts {
		fact := &next.Facts[idx]
		s.bytes += (8 * 3) + uint64(fact.Object.Size())
		s.writeFact(fact)
	}
	if now.Sub(s.lastProgress) > s.printFreq {
		fmtr.Printf("Ride in progress, %d facts, %d messages after %v (avg %0.2f MiB/s)\n",
			s.facts, s.msgs, now.Sub(s.start), float64(s.bytes)/1024.0/1024.0/now.Sub(s.start).Seconds())
		s.lastProgress = now
	}
}

func (s *rideSink) printResults() {
	now := time.Now()
	fmtr.Printf("Ride complete, %d facts, %d messages\n", s.facts, s.msgs)
	fmtr.Printf("Took %v\n", now.Sub(s.start))
	fmtr.Printf("Average %0.2f MiB/s\n", float64(s.bytes)/1024.0/1024.0/now.Sub(s.start).Seconds())
}
