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

package viewreg

import (
	"fmt"
	"testing"

	"github.com/ebay/beam/discovery"
	"github.com/ebay/beam/partitioning"
	"github.com/stretchr/testify/assert"
)

func Test_Feature_String(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("Diagnostics", DiagnosticsFeature.String())
	assert.Equal("Carousel", CarouselFeature.String())
	assert.Equal("HashPO", HashPOFeature.String())
	assert.Equal("HashSP", HashSPFeature.String())
	assert.Equal("Unknown feature (0)", Feature(0).String())
	next := 1 << uint(len(allFeatures))
	assert.Equal(fmt.Sprintf("Unknown feature (%d)", next), Feature(next).String())
}

func Test_View_Provides(t *testing.T) {
	assert := assert.New(t)
	view := View{}
	assert.True(view.Provides())
	assert.False(view.Provides(DiagnosticsFeature))
	view.SetFeaturesForTesting(DiagnosticsFeature, DiagnosticsFeature, HashSPFeature)
	assert.True(view.Provides())
	assert.True(view.Provides(DiagnosticsFeature))
	assert.True(view.Provides(DiagnosticsFeature, DiagnosticsFeature))
	assert.True(view.Provides(DiagnosticsFeature, DiagnosticsFeature, HashSPFeature))
	assert.True(view.Provides(HashSPFeature, DiagnosticsFeature))
	assert.True(view.Provides(HashSPFeature))
	assert.False(view.Provides(CarouselFeature))
	assert.False(view.Provides(HashSPFeature, CarouselFeature))
}

func Test_View_Healthy(t *testing.T) {
	assert := assert.New(t)
	assert.False((&View{connReady: false, pingHealthy: false}).Healthy())
	assert.False((&View{connReady: false, pingHealthy: true}).Healthy())
	assert.False((&View{connReady: true, pingHealthy: false}).Healthy())
	assert.True((&View{connReady: true, pingHealthy: true}).Healthy())
}

func Test_View_String(t *testing.T) {
	tests := []struct {
		name string
		view View
		str  string
	}{
		{
			name: "empty",
			view: View{},
			str:  "View{Endpoint: Partition: Features: Healthy:false}",
		},
		{
			name: "endpoint",
			view: View{
				Endpoint: &discovery.Endpoint{
					Network: "tcp",
					Host:    "example.com",
					Port:    "80",
				},
			},
			str: `View{Endpoint:"tcp://example.com:80" Partition: Features: Healthy:false}`,
		},
		{
			name: "endpoint",
			view: View{
				Partition: partitioning.NewHashPredicateObjectPartition(0, 2),
			},
			str: `View{Endpoint: Partition:[POS: 0x00.. - 0x80..] Features: Healthy:false}`,
		},
		{
			name: "features:Carousel",
			view: View{
				features: CarouselFeature,
			},
			str: `View{Endpoint: Partition: Features:Carousel Healthy:false}`,
		},
		{
			name: "features:Diagnostics|HashPO",
			view: View{
				features: DiagnosticsFeature | HashPOFeature,
			},
			str: `View{Endpoint: Partition: Features:Diagnostics,HashPO Healthy:false}`,
		},
		{
			name: "healthy:!ping&conn",
			view: View{
				pingHealthy: false,
				connReady:   true,
			},
			str: `View{Endpoint: Partition: Features: Healthy:false}`,
		},
		{
			name: "healthy:ping&!conn",
			view: View{
				pingHealthy: true,
				connReady:   false,
			},
			str: `View{Endpoint: Partition: Features: Healthy:false}`,
		},
		{
			name: "healthy:ping&conn",
			view: View{
				pingHealthy: true,
				connReady:   true,
			},
			str: `View{Endpoint: Partition: Features: Healthy:true}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			assert.Equal(test.str, test.view.String())
			assert.Equal(test.str, fmt.Sprint(test.view)) // requires a non-pointer receiver
			assert.Equal(test.str, fmt.Sprint(&test.view))
		})
	}
}
