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
	"context"
	"fmt"

	"github.com/ebay/akutan/config"
	"github.com/ebay/akutan/discovery"
	"github.com/ebay/akutan/discovery/discoveryfactory"
	"github.com/ebay/akutan/viewclient/viewreg"
)

// New constructs a Client that uses view instances from the given
// configuration.
func New(cfg *config.Akutan) (*Client, error) {
	locator, err := discoveryfactory.NewLocator(context.TODO(), &cfg.ViewsLocator)
	if err != nil {
		return nil, fmt.Errorf("failed to create locator for view servers: %v", err)
	}
	return NewWithLocator(cfg, locator), nil
}

// NewWithLocator constructs a Client that finds view instances from the
// given Locator.
func NewWithLocator(cfg *config.Akutan, views discovery.Locator) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		cfg:           cfg,
		closeRegistry: cancel,
		Registry:      viewreg.NewRegistry(ctx, views),
	}
}

// Client is a compatibility layer around the newer viewreg.Registry. Client
// currently owns its Registry, but in the future we plan for the current users
// of Client to manage their own Registry.
//
// TODO: meh viewclient.Client
type Client struct {
	cfg           *config.Akutan
	closeRegistry func()
	Registry      *viewreg.Registry
}

// Close shuts down the view registry, disconnecting from all the views. After
// this, the Client is no longer useful, as it will not initiate new
// connections. Close() is safe to call more than once.
func (c *Client) Close() {
	c.closeRegistry()
}
