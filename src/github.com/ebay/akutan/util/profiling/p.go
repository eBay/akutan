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

	log "github.com/sirupsen/logrus"
)

// CPUProfileForDuration will start generating a CPU profile (via pprof) to the
// supplied file. The profiling will run for the supplied duration and then stop.
// Only one profile can be running at a time, if you attempt to start a profile
// while profiling is already running, an error will be returned.
func CPUProfileForDuration(outputFilename string, duration time.Duration) error {
	f, err := os.Create(outputFilename)
	if err != nil {
		return err
	}
	log.Infof("Starting CPU profiling, to %s for %s", outputFilename, duration)
	err = pprof.StartCPUProfile(f)
	if err != nil {
		log.Errorf("CPU profiling error: %s", err)
		f.Close()
		return err
	}
	go func() {
		time.Sleep(duration)
		pprof.StopCPUProfile()
		f.Close()
		log.Infof("Completed CPU profile to %s", outputFilename)
	}()
	return nil
}
