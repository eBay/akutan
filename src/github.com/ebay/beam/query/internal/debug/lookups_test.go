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

package debug

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/clocks"
	"github.com/ebay/beam/viewclient/lookups/mocklookups"
	"github.com/stretchr/testify/assert"
)

func Test_LookupStats(t *testing.T) {
	l := lookupStats{}
	l.locked.duration = time.Millisecond * 2400
	l.locked.rpcCount = 3
	l.locked.resultsTotal = 6
	l.locked.lookupsTotal = 33

	s := strings.Builder{}
	l.dump(&s, "Lookup")
	assert.Equal(t, `
Lookup
	RPC Count:      3
	Avg Lookups:    11.0
	Avg Results:    2.0
	Total RPC Time: 2.4s
	Avg RPC time:   800ms
`, "\n"+s.String())
}

func Test_QueryLookupStats(t *testing.T) {
	clock := advancingClock{Mock: clocks.NewMock(), advance: time.Second}
	res := rpc.LookupChunk{
		Facts: []rpc.LookupChunk_Fact{
			{Fact: rpc.Fact{Subject: 1}},
		},
	}
	reqS1 := &rpc.LookupSRequest{
		Index: 1, Subjects: make([]uint64, 8),
	}
	reqS2 := &rpc.LookupSRequest{
		Index: 1, Subjects: make([]uint64, 16),
	}
	reqSP := &rpc.LookupSPRequest{
		Index: 2, Lookups: make([]rpc.LookupSPRequest_Item, 9),
	}
	reqSPO := &rpc.LookupSPORequest{
		Index: 3, Lookups: make([]rpc.LookupSPORequest_Item, 10),
	}
	reqPO := &rpc.LookupPORequest{
		Index: 4, Lookups: make([]rpc.LookupPORequest_Item, 5),
	}
	reqPOCmp := &rpc.LookupPOCmpRequest{
		Index: 5, Lookups: make([]rpc.LookupPOCmpRequest_Item, 6),
	}
	lookups, assertDone := mocklookups.New(t,
		mocklookups.OK(reqS1, res),
		mocklookups.OK(reqS2, res),
		mocklookups.OK(reqSP, res),
		mocklookups.OK(reqSPO, res),
		mocklookups.OK(reqPO, res),
		mocklookups.OK(reqPOCmp, res),
	)
	q := queryLookupStats{clock: clock, impl: lookups}
	b := strings.Builder{}
	q.dump(&b)
	assert.Equal(t, "", b.String())
	q.LookupS(context.Background(), reqS1, make(chan *rpc.LookupChunk, 4))
	q.LookupS(context.Background(), reqS2, make(chan *rpc.LookupChunk, 4))
	q.LookupSP(context.Background(), reqSP, make(chan *rpc.LookupChunk, 4))
	q.LookupSPO(context.Background(), reqSPO, make(chan *rpc.LookupChunk, 4))
	q.LookupPO(context.Background(), reqPO, make(chan *rpc.LookupChunk, 4))
	q.LookupPOCmp(context.Background(), reqPOCmp, make(chan *rpc.LookupChunk, 4))
	b.Reset()
	q.dump(&b)
	assertDone()
	assert.Equal(t,
		`LookupS
	RPC Count:      2
	Avg Lookups:    12.0
	Avg Results:    1.0
	Total RPC Time: 2s
	Avg RPC time:   1s
LookupSP
	RPC Count:      1
	Avg Lookups:    9.0
	Avg Results:    1.0
	Total RPC Time: 1s
	Avg RPC time:   1s
LookupSPO
	RPC Count:      1
	Avg Lookups:    10.0
	Avg Results:    1.0
	Total RPC Time: 1s
	Avg RPC time:   1s
LookupPO
	RPC Count:      1
	Avg Lookups:    5.0
	Avg Results:    1.0
	Total RPC Time: 1s
	Avg RPC time:   1s
LookupPOCmp
	RPC Count:      1
	Avg Lookups:    6.0
	Avg Results:    1.0
	Total RPC Time: 1s
	Avg RPC time:   1s
`, b.String())
}

type advancingClock struct {
	*clocks.Mock
	advance time.Duration
}

func (c advancingClock) Now() time.Time {
	defer c.Advance(c.advance)
	return c.Mock.Now()
}
