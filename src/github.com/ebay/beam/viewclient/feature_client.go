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

package viewclient

import (
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/space"
	"github.com/ebay/beam/viewclient/fanout"
	"github.com/ebay/beam/viewclient/viewreg"
)

// ****************************************************************************************************************
// This code used to be automatically generated, but it's no longer worth the trouble. Copy-paste as needed.
// ****************************************************************************************************************

// CarouselClient is a client for the Carousel service running on the particular view instance
type CarouselClient struct {
	View viewreg.View
	Stub rpc.CarouselClient
}

// Serves returns the Range in the HashSpace that this view contains.
func (c *CarouselClient) Serves() space.Range {
	return c.View.Partition.HashRange()
}

func (c *CarouselClient) String() string {
	return c.View.String()
}

// A ViewInfoPredicate is used to filter views. Sometimes nil is used in place
// of a predicate that always returns true.
type ViewInfoPredicate func(v viewreg.View) bool

// CarouselViews returns all views that support the Carousel feature, if pred is non-nil
// only those views passing the view predicate are included
func (c *Client) CarouselViews(pred ViewInfoPredicate) CarouselClients {
	views := c.Registry.HealthyViews(viewreg.CarouselFeature)
	res := make(CarouselClients, 0, len(views))
	for _, view := range views {
		if pred == nil || pred(view) {
			res = append(res, &CarouselClient{
				View: view,
				Stub: view.Carousel(),
			})
		}
	}
	return res
}

// CarouselClients is a collection of CarouselClient, you typically get one of these
// from the Client.CarouselViews() method.
type CarouselClients []*CarouselClient

// Len returns the number of clients in this list. This is part of the implementation
// of the fanout.Views interface
func (c CarouselClients) Len() int {
	return len(c)
}

// View returns the i'th client in this collect. This is part of the implementation
// of the fanout.Views interface
func (c CarouselClients) View(i int) fanout.View {
	return c[i]
}

// DiagnosticsClient is a client for the Diagnostic service running on the particular view instance
type DiagnosticsClient struct {
	View viewreg.View
	Stub rpc.DiagnosticsClient
}

func (c *DiagnosticsClient) String() string {
	return c.View.String()
}

// DiagnosticsViews returns all views that support the Diagnostics feature.
func (c *Client) DiagnosticsViews() []*DiagnosticsClient {
	// c.Registry.HealthyViews(viewreg.DiagnosticsFeature) would filter out
	// unhealthy views, but callers of this function usually want those.
	views := c.Registry.AllViews()
	res := make([]*DiagnosticsClient, len(views))
	for i := range views {
		res[i] = &DiagnosticsClient{
			View: views[i],
			Stub: views[i].Diagnostics(),
		}
	}
	return res
}

// ReadFactsPOClient is a client for the ReadFactsPO service running on the particular view instance
type ReadFactsPOClient struct {
	View viewreg.View
	Stub rpc.ReadFactsPOClient
}

// Serves returns the hash range that this view contains. this is an implementation of the
// fanout.View interface
func (c *ReadFactsPOClient) Serves() space.Range {
	return c.View.Partition.HashRange()
}

func (c *ReadFactsPOClient) String() string {
	return c.View.String()
}

// ReadFactsPOClients is a collection of ReadFatsPOClient's. Typically you'd get these from
// a call to Client.ReadFactsPOViews()
type ReadFactsPOClients []*ReadFactsPOClient

// Len returns the number of clients in this collection. This is part of the implementation
// of the fanout.Views interface
func (c ReadFactsPOClients) Len() int {
	return len(c)
}

// View return's the i'th View in this collection. This is part of the implementation
// of the fanout.Views interface
func (c ReadFactsPOClients) View(i int) fanout.View {
	return c[i]
}

// ReadFactsPOViews returns all views that support the ReadFactsPO feature.
func (c *Client) ReadFactsPOViews() ReadFactsPOClients {
	views := c.Registry.HealthyViews(viewreg.HashPOFeature)
	res := make([]*ReadFactsPOClient, len(views))
	for i := range views {
		res[i] = &ReadFactsPOClient{
			View: views[i],
			Stub: views[i].HashPO(),
		}
	}
	return res
}

// ReadFactsSPClient is a client for the ReadFactsSP service running on the particular view instance
type ReadFactsSPClient struct {
	View viewreg.View
	Stub rpc.ReadFactsSPClient
}

// Serves returns the range in Hash(SP) space that the related view contains. This is an implementation
// of the fanout.View interface
func (c *ReadFactsSPClient) Serves() space.Range {
	return c.View.Partition.HashRange()
}

func (c *ReadFactsSPClient) String() string {
	return c.View.String()
}

// ReadFactsSPClients is a collection of ReadFactsSPClient's. You typically get one of these from
// calling Client.ReadFactsSPViews()
type ReadFactsSPClients []*ReadFactsSPClient

// Len returns the number of clients in this collection. This is part of the implementation of the
// fanout.Views interface
func (c ReadFactsSPClients) Len() int {
	return len(c)
}

// View returns the i'th client in the collection. This is part of the fanout.Views interface implementation
func (c ReadFactsSPClients) View(i int) fanout.View {
	return c[i]
}

// ReadFactsSPViews returns all views that support the ReadFactsSP feature.
func (c *Client) ReadFactsSPViews() ReadFactsSPClients {
	views := c.Registry.HealthyViews(viewreg.HashSPFeature)
	res := make([]*ReadFactsSPClient, len(views))
	for i := range views {
		res[i] = &ReadFactsSPClient{
			View: views[i],
			Stub: views[i].HashSP(),
		}
	}
	return res
}
