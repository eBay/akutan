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

// Package discoveryfactory constructs service discovery implementations. Users
// of this package don't need to know which implementation of service discovery
// they're using.
package discoveryfactory

import (
	"context"
	"fmt"
	"sync"

	"github.com/ebay/akutan/config"
	"github.com/ebay/akutan/discovery"
)

// All of the discovery implementations are registered here at
// initialization-time. The map key is the same as config.Locator.Type.
var impls = map[string]*discoveryImpl{}

// A single implementation of service discovery.
type discoveryImpl struct {
	// This lock prevents concurrent calls to setUp.
	lock sync.Mutex
	// Initialize the implementation. This is only invoked if a factory is needed.
	setUp func() (locatorFactory, error)
	// nil until the implementation has been initialized, then used to create
	// locators.
	newLocator locatorFactory
}

// A locatorFactory creates locators. It takes the same arguments as NewLocator.
type locatorFactory func(context.Context, *config.Locator) (discovery.Locator, error)

// NewLocator returns a locator as defined by the configuration. It returns an
// error if the configuration is invalid or if the underlying implementation
// cannot create such a locator. Closing the given context will stop the locator
// from providing any more updates.
func NewLocator(ctx context.Context, cfg *config.Locator) (discovery.Locator, error) {
	newLocator, err := getFactory(cfg)
	if err != nil {
		return nil, err
	}
	return newLocator(ctx, cfg)
}

// getFactory is a helper to NewLocator. It's split out to defer an unlock.
func getFactory(cfg *config.Locator) (locatorFactory, error) {
	impl, ok := impls[cfg.Type]
	if !ok {
		return nil, fmt.Errorf("locator type not supported: %v", cfg.Type)
	}
	impl.lock.Lock()
	defer impl.lock.Unlock()
	if impl.newLocator == nil {
		newLocator, err := impl.setUp()
		if err != nil {
			return nil, fmt.Errorf("failed to initialize implementation for %v locators: %v",
				cfg.Type, err)
		}
		impl.newLocator = newLocator
	}
	return impl.newLocator, nil
}
