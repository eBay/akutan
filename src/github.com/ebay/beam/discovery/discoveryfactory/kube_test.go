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
	"testing"

	"github.com/ebay/beam/config"
	"github.com/stretchr/testify/assert"
)

// We can't assume this test is running under Kubernetes, so theres's not much
// we can assert here. It's still good to make sure there's no panic.
func Test_NewLocator_kube(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Either not running in kube or missing labelSelector:
	_, err := NewLocator(ctx, &config.Locator{
		Type: "kube",
	})
	assert.Error(err)

	// Either not running in kube or OK:
	_, err = NewLocator(ctx, &config.Locator{
		Type:          "kube",
		PortName:      "junk",
		LabelSelector: "shoe=shoe",
	})
	if err != nil {
		assert.Contains(err.Error(),
			"failed to initialize implementation for kube locators:")
	}
}
