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
	"strings"

	"github.com/ebay/beam/discovery"
	"github.com/ebay/beam/partitioning"
	"github.com/ebay/beam/rpc"
	"google.golang.org/grpc"
)

// A Feature represents a gRPC service that a server may provide.
type Feature uint8

// Each of these symbols corresponds to one gRPC service. Use the Provides()
// method on a View to determine if the View is providing the service. The
// values here are intended for this package's internal use.
//
// Note: Features that a server supports may not be "provided" if the Registry
// has not confirmed that server as healthy.
const (
	DiagnosticsFeature Feature = (1 << 0) // rpc.Diagnostics service
	CarouselFeature    Feature = (1 << 1) // rpc.Carousel service
	HashPOFeature      Feature = (1 << 2) // rpc.ReadFactsPO service
	HashSPFeature      Feature = (1 << 3) // rpc.ReadFactsSP service
)

// An immutable list of all the defined Feature values that is convenient to
// iterate over.
var allFeatures = []Feature{
	DiagnosticsFeature,
	CarouselFeature,
	HashPOFeature,
	HashSPFeature,
}

// String returns human-readable strings like "Diagnostics".
func (f Feature) String() string {
	switch f {
	case DiagnosticsFeature:
		return "Diagnostics"
	case CarouselFeature:
		return "Carousel"
	case HashPOFeature:
		return "HashPO"
	case HashSPFeature:
		return "HashSP"
	default:
		return fmt.Sprintf("Unknown feature (%d)", uint8(f))
	}
}

// A View represents a single remote Beam server.
type View struct {
	// The location of the server's gRPC port. This is a shared value; callers
	// must not modify it.
	Endpoint *discovery.Endpoint
	// If the server hosts a partition, this identifies its space and range.
	// Otherwise, this is nil. This is guaranteed to be non-nil if the view
	// provides any of CarouselFeature, HashPOFeature, or HashSPFeature.
	Partition partitioning.FactPartition
	// Bit flags that indicate which gRPC services the endpoint provices.
	features Feature
	// True if the pinger believed the server to be likely to respond to RPCs (when
	// this View was created), false otherwise.
	pingHealthy bool
	// True if 'conn' was in the READY state (when this View was created), false
	// otherwise.
	connReady bool
	// A non-nil connection to the endpoint.
	conn *grpc.ClientConn
}

// Provides returns true if the server is known to have all of the given
// features, false otherwise.
func (view *View) Provides(features ...Feature) bool {
	test := Feature(0)
	for _, f := range features {
		test |= f
	}
	return view.features&test == test
}

// SetFeaturesForTesting sets the View's provided features to those given. It is
// intended for unit testing only.
func (view *View) SetFeaturesForTesting(features ...Feature) {
	set := Feature(0)
	for _, f := range features {
		set |= f
	}
	view.features = set
}

// Healthy returns true if the server had a high likelihood of being reachable
// at the time the View was created, false otherwise.
func (view *View) Healthy() bool {
	return view.pingHealthy && view.connReady
}

// String returns a short description of the view's address and services.
//
// Note: This intentionally takes a non-pointer receiver so that
// fmt.Sprint(View{...}) calls this.
func (view View) String() string {
	var b strings.Builder
	b.Grow(120)
	b.WriteString("View{Endpoint:")
	if view.Endpoint != nil {
		b.WriteByte('"')
		b.WriteString(view.Endpoint.String())
		b.WriteByte('"')
	}
	b.WriteString(" Partition:")
	if view.Partition != nil {
		b.WriteByte('[')
		b.WriteString(partitioning.String(view.Partition))
		b.WriteByte(']')
	}
	b.WriteString(" Features:")
	needComma := false
	for _, f := range allFeatures {
		if view.Provides(f) {
			if needComma {
				b.WriteByte(',')
			}
			b.WriteString(f.String())
			needComma = true
		}
	}
	if view.Healthy() {
		b.WriteString(" Healthy:true}")
	} else {
		b.WriteString(" Healthy:false}")
	}
	return b.String()
}

// Diagnostics returns a stub to invoke Diagnostics RPCs. If the View is not
// known to provide the DiagnosticsFeature, the RPCs invoked through this stub
// are likely to fail. This method is cheap to call frequently.
func (view *View) Diagnostics() rpc.DiagnosticsClient {
	return rpc.NewDiagnosticsClient(view.conn)
}

// Carousel returns a stub to invoke Carousel RPCs. If the View is not known to
// provide the CarouselFeature, the RPCs invoked through this stub are likely to
// fail. This method is cheap to call frequently.
func (view *View) Carousel() rpc.CarouselClient {
	return rpc.NewCarouselClient(view.conn)
}

// HashPO returns a stub to invoke ReadFactsPO RPCs. If the View is not known to
// provide the HashPOFeature, the RPCs invoked through this stub are likely to
// fail. This method is cheap to call frequently.
func (view *View) HashPO() rpc.ReadFactsPOClient {
	return rpc.NewReadFactsPOClient(view.conn)
}

// HashSP returns a stub to invoke ReadFactsSP RPCs. If the View is not known to
// provide the HashSPFeature, the RPCs invoked through this stub are likely to
// fail. This method is cheap to call frequently.
func (view *View) HashSP() rpc.ReadFactsSPClient {
	return rpc.NewReadFactsSPClient(view.conn)
}
