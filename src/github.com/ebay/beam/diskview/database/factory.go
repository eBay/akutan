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

package database

import (
	"fmt"
	"sync"

	"github.com/ebay/beam/config"
	"github.com/ebay/beam/partitioning"
)

// Factory defines a method for creating new instances of DB. Specific DB engines
// should implement this and call Register
type Factory func(FactoryArgs) (DB, error)

// FactoryArgs contains configuration data to instantiate an instance of a DB
type FactoryArgs struct {
	Partition partitioning.FactPartition
	Config    *config.DiskView
	Dir       string
}

var (
	factoriesLock sync.RWMutex // protects factories
	factories     = make(map[string]Factory)
)

// Register allows implementations of the DB interface to register their constructor
// Factory with one or more names
func Register(f Factory, names ...string) {
	factoriesLock.Lock()
	for _, n := range names {
		factories[n] = f
	}
	factoriesLock.Unlock()
}

// New returns a new instance of named DB implementation type, that was constructed with
// the supplied arguments. It'll return an error if the named impl type has not be previously
// registered via the Register method
func New(impl string, args FactoryArgs) (DB, error) {
	factoriesLock.RLock()
	factory, ok := factories[impl]
	factoriesLock.RUnlock()
	if !ok {
		return nil, fmt.Errorf("database implementation %v not found", impl)
	}
	return factory(args)
}
