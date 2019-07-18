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

package profiling

import (
	"os"
	"runtime/pprof"
	"time"

	"github.com/ebay/akutan/util/cmp"
	log "github.com/sirupsen/logrus"
)

// View defines an abstract way for the StartupProfile function to see
// where in the log the current processed point is.
type View interface {
	LastLogIndex() (uint64, error)
	LastApplied() uint64
}

// StartupProfile will create a CPU profile to the supplied name, it'll run
// until the Views LastApplied() results is >= an initial LastLogIndex result.
// You can use this to easily profile the startup/catchup phase of a view
func StartupProfile(filename string, view View) {
	pf, err := os.Create(filename)
	if err != nil {
		log.Errorf("Unable to create file for Startup Profiling, skipping: %v", err)
		return
	}
	err = pprof.StartCPUProfile(pf)
	if err != nil {
		log.Errorf("Unable to begin Startup CPU Profile, skipping: %v", err)
		return
	}
	go profileWatcher(pf, view)
}

func profileWatcher(pf *os.File, view View) {
	hwm, _ := view.LastLogIndex()
	log.Infof("Profiling until AtIndex=%d", hwm)
	prev := view.LastApplied()
	wait := uint64(1)
	for {
		time.Sleep(time.Duration(wait) * time.Second)
		at := view.LastApplied()
		if at >= hwm {
			log.Infof("Partition reached HWM of %d, stoping CPU Profile", hwm)
			pprof.StopCPUProfile()
			pf.Close()
			return
		}
		rate := cmp.MaxUint64(1, (at-prev)/wait)
		wait = cmp.MinUint64(60, cmp.MaxUint64(1, ((hwm-at)/rate)*5/10))
		prev = at
	}
}
