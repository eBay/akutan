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

package discoveryfactory

import (
	"context"
	"errors"

	"github.com/ebay/beam/config"
	"github.com/ebay/beam/discovery"
	"github.com/ebay/beam/discovery/kubediscovery"
)

func init() {
	impls["kube"] = &discoveryImpl{setUp: setUpKube}
}

func setUpKube() (locatorFactory, error) {
	client, err := kubediscovery.NewKubeClient()
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, cfg *config.Locator) (discovery.Locator, error) {
		if cfg.LabelSelector == "" {
			return nil, errors.New("missing labelSelector in Kubernetes locator")
		}
		if cfg.PortName == "" {
			return nil, errors.New("missing portName in Kubernetes locator")
		}
		return client.NewLocator(ctx, cfg.PortName, cfg.LabelSelector)
	}, nil
}
