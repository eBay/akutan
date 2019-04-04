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

package logread

import (
	"testing"
	"time"

	"github.com/ebay/beam/api"
	"github.com/ebay/beam/blog"
	"github.com/ebay/beam/logentry"
	"github.com/ebay/beam/logentry/logwrite"
	"github.com/ebay/beam/msg/kgobject"
	"github.com/ebay/beam/rpc"
	"github.com/stretchr/testify/assert"
)

func Test_Fact(t *testing.T) {
	a := logentry.InsertFact{
		FactIDOffset: 2,
		Subject:      logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Offset{Offset: 1}},
		Predicate:    logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: 4}},
		Object:       logwrite.AString("Bob", 5),
	}
	expRPCFact := rpc.Fact{
		Index:     1,
		Id:        1002,
		Subject:   1001,
		Predicate: 4,
		Object:    rpc.AString("Bob", 5),
	}
	act := ToRPCFact(blog.Index(1), &a)
	assert.Equal(t, expRPCFact, act)

	expAPIFact := api.ResolvedFact{
		Index:     1,
		Id:        1002,
		Subject:   1001,
		Predicate: 4,
		Object:    kgobject.AString("Bob", 5),
	}
	assert.Equal(t, expAPIFact, act.ToAPIFact())
}

func Test_ToRPCKGObject(t *testing.T) {
	type tc struct {
		src logentry.KGObject
		exp rpc.KGObject
	}
	tm := time.Date(2018, 9, 14, 14, 24, 1, 0, time.UTC)
	tests := []tc{
		{logentry.KGObject{}, rpc.KGObject{}},
		{logwrite.AString("Bob", 1), rpc.AString("Bob", 1)},
		{logwrite.AFloat64(42.42, 2), rpc.AFloat64(42.42, 2)},
		{logwrite.AInt64(42, 3), rpc.AInt64(42, 3)},
		{logwrite.ATimestamp(tm, logentry.Second, 4), rpc.ATimestamp(tm, logentry.Second, 4)},
		{logwrite.ABool(true, 5), rpc.ABool(true, 5)},
		{logwrite.AKID(6), rpc.AKID(6)},
		{logwrite.AKIDOffset(7), rpc.AKID(1007)},
	}
	for _, test := range tests {
		act := ToRPCKGObject(1, test.src)
		assert.Equal(t, test.exp, act)
	}
}
