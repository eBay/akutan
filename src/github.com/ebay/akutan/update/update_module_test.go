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

package update

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ebay/akutan/api"
	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/blog/mockblog"
	"github.com/ebay/akutan/viewclient/mockstore"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file contains higher-level tests for the update package.

var moduleTests = []struct {
	name      string
	preload   string
	insert    string
	expRes    *api.InsertResult
	expErr    error
	expStored string
}{
	{
		name:   "basic",
		insert: `the:sky akutan:is colors:blue`,
		expRes: &api.InsertResult{
			Index:  1,
			Status: api.InsertStatus_OK,
		},
	},
	{
		name: "meta0",
		preload: `
			        the:sky  akutan:is       colors:blue`,
		insert: `
			?zero   the:sky  akutan:is       colors:blue
			        ?zero    akutan:source   place:san_francisco`,
		expRes: &api.InsertResult{
			Index:  3,
			Status: api.InsertStatus_OK,
		},
	},
	{
		name: "meta1",
		preload: `
			?zero   the:sky  akutan:is       colors:blue
			        ?zero    akutan:source   place:san_francisco`,
		insert: `
			?zero   the:sky  akutan:is       colors:blue
			?one    ?zero    akutan:source   place:san_francisco
			        ?one     akutan:source   web:weather_com`,
		expRes: &api.InsertResult{
			Index:  3,
			Status: api.InsertStatus_OK,
		},
	},
	{
		name: "meta3",
		preload: `
			?zero   the:sky  akutan:is       colors:blue
			?one    ?zero    akutan:source   place:san_francisco
			?two    ?one     akutan:source   web:weather_com
			?three  ?two     akutan:channel  comms:web
			        ?three   data:signal   0.8`,
		insert: `
			?zero   the:sky  akutan:is       colors:blue
			?one    ?zero    akutan:source   place:san_francisco
			?two    ?one     akutan:source   web:weather_com
			?three  ?two     akutan:channel  comms:web
			        ?three   data:noise    0.65
			        ?three   akutan:like     <aol>
			        ?three   data:signal   0.8`,
		expRes: &api.InsertResult{
			Index:  3,
			Status: api.InsertStatus_OK,
		},
	},

	{
		name: "externalID wellknown fact",
		insert: `
			<HasExternalID> <HasExternalID> "HasExternalID"`,
		expRes: &api.InsertResult{
			Index:  0,
			Status: api.InsertStatus_OK,
		},
	},
	{
		name: "assign externalID",
		preload: `
			the:sky akutan:is colors:blue`,
		insert: `
			the:sky <HasExternalID> "the:skies"`,
		expRes: &api.InsertResult{
			Index:  3,
			Status: api.InsertStatus_OK,
		},
	},
	{
		name: "assign and use externalID existing",
		preload: `
			the:sky akutan:is colors:blue`,
		insert: `
			the:sky <HasExternalID> "the:skies"
			the:skies akutan:is colors:gray`,
		expRes: &api.InsertResult{
			Index:  3,
			Status: api.InsertStatus_OK,
		},
	},
	{
		name: "assign and use externalID new",
		insert: `
			the:sky <HasExternalID> "the:skies"
			the:sky akutan:is colors:blue
			the:skies akutan:is colors:gray`,
		expRes: &api.InsertResult{
			Index:  1,
			Status: api.InsertStatus_OK,
		},
	},
	{
		name: "use before assign externalID",
		insert: `
			the:skies akutan:is colors:gray
			the:sky <HasExternalID> "the:skies"`,
		expRes: &api.InsertResult{
			Status: api.InsertStatus_SchemaViolation,
			Error:  `can't use and then manually assign external ID "the:skies" (fact 2)`,
		},
	},

	{
		name: "assign external ID to fact",
		preload: `
			?fact  the:sky  akutan:is          colors:blue
			       ?fact    <HasExternalID>  "skyisblue"`,
		insert: `
			<skyisblue> akutan:source place:san_francisco`,
		expRes: &api.InsertResult{
			Status: api.InsertStatus_OK,
			Index:  3,
		},
		expStored: `
			{idx:1 id:1001 s:1002 p:4 o:"the:sky"}
			{idx:1 id:1003 s:1004 p:4 o:"akutan:is"}
			{idx:1 id:1005 s:1006 p:4 o:"colors:blue"}
			{idx:1 id:1007 s:1002 p:1004 o:#1006}
			{idx:1 id:1008 s:1007 p:4 o:"skyisblue"}
			{idx:3 id:3001 s:3002 p:4 o:"akutan:source"}
			{idx:3 id:3003 s:3004 p:4 o:"place:san_francisco"}
			{idx:3 id:3005 s:1007 p:3002 o:#3004}
		`,
	},

	{
		name: "double variable assignment",
		insert: `
			?fact  the:sky    akutan:is  colors:blue
			?fact  the:grass  akutan:is  colors:green`,
		expRes: &api.InsertResult{
			Status: api.InsertStatus_ParseError,
			Error:  `parser: variable ?fact captured more than once`,
		},
	},

	{
		name:    "external ID different re-assignment",
		preload: `the:sky akutan:is colors:blue`,
		insert:  `the:grass <HasExternalID> "the:sky"`,
		expRes: &api.InsertResult{
			Status: api.InsertStatus_SchemaViolation,
			Error:  `can't manually re-assign external ID "the:sky" (fact 1)`,
		},
	},
	{
		name:    "external ID same re-assignment",
		preload: `the:sky akutan:is colors:blue`,
		insert:  `the:sky <HasExternalID> "the:sky"`,
		expRes: &api.InsertResult{
			Status: api.InsertStatus_SchemaViolation,
			Error:  `can't use and then manually assign external ID "the:sky" (fact 1)`,
		},
	},
	{
		name:   "external ID same re-assignment wellknown",
		insert: `<HasExternalID> <HasExternalID> "HasExternalID"`,
		expRes: &api.InsertResult{
			Status: api.InsertStatus_OK,
			Index:  0,
		},
	},
}

func Test_Update_module(t *testing.T) {
	for _, test := range moduleTests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			assert, require := assert.New(t), require.New(t)
			aLog := mockblog.New(ctx)
			store := mockstore.NewLogConsumer()
			go store.Consume(ctx, aLog)
			lastLogIndex := func(ctx context.Context) blog.Index {
				info, err := aLog.Info(ctx)
				require.NoError(err)
				return info.LastIndex
			}
			if test.preload != "" {
				logrus.Info("preload")
				res, err := Insert(ctx,
					&api.InsertRequest{
						Format: "tsv",
						Facts:  test.preload,
					},
					lastLogIndex(ctx),
					store.Lookups(),
					aLog)
				require.NoError(err)
				require.NotNil(res)
				require.Equal(api.InsertStatus_OK, res.Status)
				snap, err := store.Snapshot(ctx, res.Index)
				assert.NoError(err)
				if assert.NotNil(snap) {
					snap.CheckInvariants(t)
					snap.AssertFacts(t, test.preload)
				}
			}

			logrus.Info("main insert")
			res, err := Insert(ctx,
				&api.InsertRequest{
					Format: "tsv",
					Facts:  test.insert,
				},
				lastLogIndex(ctx),
				store.Lookups(),
				aLog)

			logrus.Info("validations")
			assert.Equal(test.expErr, err)
			if test.expErr != nil {
				assert.Nil(test.expRes)
				assert.Nil(res)
				return
			}
			if assert.NotNil(test.expRes, "expected API result") &&
				assert.NotNil(res, "actual API result") {
				assert.Equal(test.expRes.String(), res.String())

				snap, err := store.Snapshot(ctx, res.Index)
				assert.NoError(err)
				if assert.NotNil(snap) {
					snap.CheckInvariants(t)
					if test.expRes.Status == api.InsertStatus_OK {
						snap.AssertFacts(t, test.preload)
						snap.AssertFacts(t, test.insert)
					}
				}
			}

			if test.expStored != "" {
				var expected strings.Builder
				for _, fact := range strings.Split(strings.TrimSpace(test.expStored), "\n") {
					fmt.Fprintf(&expected, "%v\n", strings.TrimSpace(fact))
				}
				var actual strings.Builder
				for _, fact := range store.Current().StoredFacts() {
					fmt.Fprintf(&actual, "%v\n", fact)
				}
				assert.Equal(expected.String(), actual.String(), "stored facts")
			}
		})
	}
}
