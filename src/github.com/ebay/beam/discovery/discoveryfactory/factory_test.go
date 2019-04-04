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
	"fmt"
	"testing"

	"github.com/ebay/beam/config"
	"github.com/stretchr/testify/assert"
)

func Test_NewLocator(t *testing.T) {
	assert := assert.New(t)

	_, err := NewLocator(context.Background(), &config.Locator{
		Type: "",
	})
	assert.EqualError(err, "locator type not supported: ")

	_, err = NewLocator(context.Background(), &config.Locator{
		Type: "rainbows",
	})
	assert.EqualError(err, "locator type not supported: rainbows")

	_, err = NewLocator(context.Background(), &config.Locator{
		Type:      "static",
		Addresses: []string{"whereisthecolonhere"},
	})
	if assert.Error(err) {
		assert.Contains(err.Error(), "missing port")
	}

	locator, err := NewLocator(context.Background(), &config.Locator{
		Type:      "static",
		Addresses: []string{"example.com:80"},
	})
	if assert.NoError(err) {
		assert.Equal("[tcp://example.com:80]",
			fmt.Sprint(locator.Cached().Endpoints))
	}
}
