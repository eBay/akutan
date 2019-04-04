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
	"context"
	"time"

	"github.com/ebay/beam/blog"
	"github.com/ebay/beam/util/clocks"
	"github.com/ebay/beam/util/cmp"
	log "github.com/sirupsen/logrus"
)

// waitForIndex queries for stats and waits until the minimum index as seen by all of the views
// is gte the index supplied or the timeout elapses and returns the minimum index seen
func (s *Server) waitForIndex(ctx context.Context, index blog.Index, timeout time.Duration) blog.Index {
	start := time.Now()
	vs := s.source.Stats(ctx)
	vsi := make([]blog.Index, len(vs))
	set := func() {
		for i := range vs {
			if vs[i].Err == nil {
				vsi[i] = vs[i].Stats.LastIndex
			} else {
				vsi[i] = 0
				log.Debugf("wait for index got error %v", vs[i].Err)
			}
		}
		log.Debugf("wait for index: %v", vsi)
	}
	set()
	atIndex := cmp.MinUint64(vsi[0], vsi[1:]...)
	for index < atIndex {
		now := time.Now()
		if now.Sub(start) > timeout {
			break
		}
		clocks.Wall.SleepUntil(ctx, now.Add(time.Millisecond*100))
		vs = s.source.Stats(ctx)
		set()
		atIndex = cmp.MinUint64(vsi[0], vsi[1:]...)
	}
	return atIndex
}
