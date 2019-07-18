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

package discovery

import (
	"context"
	"fmt"
	"sync"
)

// StaticLocator is a Locator that returns a Result stored within. It's possible
// to update the StaticLocator's endpoints using Set().
type StaticLocator struct {
	// Protects the fields in 'locked'.
	lock sync.Mutex
	// Protected by 'lock'.
	locked struct {
		// The result's []*Endpoint slice reference and its Version are protected by
		// 'lock'. The []*Endpoint array and values are immutable.
		result Result
		// When 'result' is changed, this is closed and re-initialized to a new
		// channel. The channel reference is protected by 'lock'. Channel
		// operations are safe without holding 'lock'.
		waiting chan struct{}
	}
}

// StaticLocator implements Locator.
var _ Locator = (*StaticLocator)(nil)

// NewStaticLocator returns a Locator containing a single Result that can only
// be updated externally. The caller may not modify the endpoints slice after
// calling this function. When endpoints is nil the Result.Version is set to 0,
// 1 otherwise.
//
// This is useful for testing and development purposes. It's also useful where
// the endpoints are a static list of DNS hostnames or load balancers, which may
// point to different hosts over time. Using Set(), this can also form the basis
// of a truly dynamic locator.
func NewStaticLocator(endpoints []*Endpoint) *StaticLocator {
	locator := &StaticLocator{}
	v := uint64(0)
	if len(endpoints) > 0 {
		v = uint64(1)
	}
	locator.locked.result = Result{
		Endpoints: endpoints,
		Version:   v,
	}
	locator.locked.waiting = make(chan struct{})
	return locator
}

// Set updates the Locator to return the given endpoints. This is thread-safe.
// The caller may not modify the endpoints slice after calling this function.
func (locator *StaticLocator) Set(endpoints []*Endpoint) {
	locator.lock.Lock()
	defer locator.lock.Unlock()
	locator.locked.result = Result{
		Endpoints: endpoints,
		Version:   locator.locked.result.Version + 1,
	}
	close(locator.locked.waiting)
	locator.locked.waiting = make(chan struct{})
}

// Cached implements Locator.Cached.
func (locator *StaticLocator) Cached() Result {
	locator.lock.Lock()
	res := locator.locked.result
	locator.lock.Unlock()
	return res
}

// WaitForUpdate implements Locator.WaitForUpdate.
func (locator *StaticLocator) WaitForUpdate(ctx context.Context, oldVersion uint64) (Result, error) {
	for {
		locator.lock.Lock()
		res := locator.locked.result
		waiting := locator.locked.waiting
		locator.lock.Unlock()

		if res.Version > oldVersion {
			return res, nil
		}
		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		case <-waiting:
		}
	}
}

// String implements Locator.String.
func (locator *StaticLocator) String() string {
	result := locator.Cached()
	switch len(result.Endpoints) {
	case 0:
		return "empty StaticLocator"
	case 1:
		return fmt.Sprintf("StaticLocator(%v)", result.Endpoints[0])
	default:
		return fmt.Sprintf("StaticLocator(len=%v, first=%v)",
			len(result.Endpoints), result.Endpoints[0])
	}
}
