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

package gen

import (
	"testing"

	"github.com/ebay/akutan/config"
	"github.com/stretchr/testify/assert"
)

func minimalSpec() *Spec {
	return &Spec{
		BaseCfg: &config.Akutan{},
		API: APISpec{
			Replicas:  1,
			GRPCPorts: []string{"api01:9987"},
			HTTPPorts: []string{"api01:9988"},
		},
		Views: ViewsSpec{
			GRPCPorts: []string{"view01:9987", "view02:9987", "view03:9987"},
			HTTPPorts: []string{"view01:9988", "view02:9988", "view03:9988"},
		},
		TxTimeout: TxTimeoutSpec{
			Replicas: 1,
		},
		HashPO: HashPOSpec{
			Partitions: 1,
			Replicas:   1,
		},
		HashSP: HashSPSpec{
			Partitions: 1,
			Replicas:   1,
		},
	}
}

func Test_Generate_minimal(t *testing.T) {
	assert := assert.New(t)
	spec := minimalSpec()
	spec.BaseCfg.API = &config.API{DebugQuery: true}
	cluster, err := spec.Generate()

	viewsLocator := config.Locator{
		Type:      "static",
		Addresses: []string{"view01:9987", "view02:9987", "view03:9987"},
	}
	if assert.NoError(err) && assert.Len(cluster, 4) {
		assert.Equal("akutan-api-00", cluster[0].Name)
		assert.Equal("akutan-api", string(cluster[0].Type))
		assert.Equal(&config.Akutan{
			ViewsLocator: viewsLocator,
			API: &config.API{
				GRPCAddress: "api01:9987",
				HTTPAddress: "api01:9988",
				DebugQuery:  true,
			},
		}, cluster[0].Cfg)

		assert.Equal("akutan-txview-00", cluster[1].Name)
		assert.Equal("akutan-txview", string(cluster[1].Type))
		assert.Equal(&config.Akutan{
			ViewsLocator: viewsLocator,
			TxTimeoutView: &config.TxTimeoutView{
				GRPCAddress:    "view01:9987",
				MetricsAddress: "view01:9988",
			},
		}, cluster[1].Cfg)

		assert.Equal("akutan-diskview-hashsp-00", cluster[2].Name)
		assert.Equal("akutan-diskview", string(cluster[2].Type))
		assert.Equal(&config.Akutan{
			ViewsLocator: viewsLocator,
			DiskView: &config.DiskView{
				GRPCAddress:    "view02:9987",
				MetricsAddress: "view02:9988",
				Space:          "hashsp",
				FirstHash:      "0x0000000000000000",
				LastHash:       "0xffffffffffffffff",
			},
		}, cluster[2].Cfg)

		assert.Equal("akutan-diskview-hashpo-00", cluster[3].Name)
		assert.Equal("akutan-diskview", string(cluster[3].Type))
		assert.Equal(&config.Akutan{
			ViewsLocator: viewsLocator,
			DiskView: &config.DiskView{
				GRPCAddress:    "view03:9987",
				MetricsAddress: "view03:9988",
				Space:          "hashpo",
				FirstHash:      "0x0000000000000000",
				LastHash:       "0xffffffffffffffff",
			},
		}, cluster[3].Cfg)
	}
}

// Tests that if the BaseCfg's DiskView parameters get copied.
func Test_diskViewCfg_copiesFirst(t *testing.T) {
	assert := assert.New(t)
	spec := minimalSpec()
	spec.BaseCfg.DiskView = &config.DiskView{
		Compression: "bananas",
	}
	cluster, err := spec.Generate()
	assert.NoError(err)
	diskViews := cluster.Filter(DiskViewProcess)
	assert.NotEmpty(diskViews)
	for _, proc := range diskViews {
		assert.Equal("bananas", proc.Cfg.DiskView.Compression)
	}
}
