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

package logwrite

import (
	"testing"
	"time"

	"github.com/ebay/akutan/logentry"
	"github.com/stretchr/testify/assert"
)

func Test_InsertFactBuilder(t *testing.T) {
	type tc struct {
		built logentry.InsertFact
		exp   logentry.InsertFact
	}
	b := new(InsertFactBuilder)
	tm := time.Date(2018, 9, 12, 15, 13, 2, 2, time.UTC)
	tests := []tc{
		{
			b.SID(1).PID(2).OKID(3).Fact(),
			logentry.InsertFact{FactIDOffset: 1, Subject: koKid(1), Predicate: koKid(2), Object: AKID(3)},
		}, {
			b.SOffset(100).POffset(101).OOffset(102).Fact(),
			logentry.InsertFact{FactIDOffset: 2, Subject: koOffset(100), Predicate: koOffset(101), Object: AKIDOffset(102)},
		}, {
			b.SID(1).PID(2).OString("Bob", 42).Fact(),
			logentry.InsertFact{FactIDOffset: 3, Subject: koKid(1), Predicate: koKid(2), Object: AString("Bob", 42)},
		}, {
			b.SID(1).PID(2).OInt64(1234, 42).Fact(),
			logentry.InsertFact{FactIDOffset: 4, Subject: koKid(1), Predicate: koKid(2), Object: AInt64(1234, 42)},
		}, {
			b.SID(1).PID(2).OFloat64(42.42, 2).Fact(),
			logentry.InsertFact{FactIDOffset: 5, Subject: koKid(1), Predicate: koKid(2), Object: AFloat64(42.42, 2)},
		}, {
			b.SID(1).PID(2).OBool(true, 2).Fact(),
			logentry.InsertFact{FactIDOffset: 6, Subject: koKid(1), Predicate: koKid(2), Object: ABool(true, 2)},
		}, {
			b.SID(1).PID(2).OTimestamp(tm, logentry.Nanosecond, 222).Fact(),
			logentry.InsertFact{FactIDOffset: 7, Subject: koKid(1), Predicate: koKid(2), Object: ATimestamp(tm, logentry.Nanosecond, 222)},
		}, {
			b.FactID(123).OBool(true, 2).Fact(),
			logentry.InsertFact{FactIDOffset: 123, Subject: koKid(1), Predicate: koKid(2), Object: ABool(true, 2)},
		}, {
			b.OBool(false, 2).Fact(),
			logentry.InsertFact{FactIDOffset: 123, Subject: koKid(1), Predicate: koKid(2), Object: ABool(false, 2)},
		}, {
			b.FactID(0).SetAutoFactID(false).Fact(),
			logentry.InsertFact{FactIDOffset: 0, Subject: koKid(1), Predicate: koKid(2), Object: ABool(false, 2)},
		},
	}
	for _, test := range tests {
		assert.Equal(t, test.built, test.exp)
	}
}
