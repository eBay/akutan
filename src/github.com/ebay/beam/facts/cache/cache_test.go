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

package cache

import (
	"testing"

	"github.com/ebay/beam/rpc"
	"github.com/stretchr/testify/assert"
)

var noPath []rpc.Fact

func Test_CopyOfPath(t *testing.T) {
	assert.Nil(t, copyOfPath(nil))
	assert.Nil(t, copyOfPath([]rpc.Fact{}))
	p := []rpc.Fact{
		{Id: 1, Index: 2, Subject: 3, Predicate: 4, Object: rpc.AKID(5)},
		{Id: 6, Index: 7, Subject: 5, Predicate: 9, Object: rpc.AKID(10)},
	}
	c := copyOfPath(p)
	assert.Equal(t, c, p)
	p[0].Id = 2
	assert.NotEqual(t, c, p)
}

func Test_FictionCache(t *testing.T) {
	c := New()
	obj := rpc.AKID(42)
	c.AddFiction(1, 2, 3, obj)
	c.AddFiction(2, 2, 3, obj)
	c.AddFiction(2, 5, 3, obj)
	assert.True(t, c.IsFiction(1, 2, 3, obj))
	assert.True(t, c.IsFiction(2, 2, 3, obj))
	assert.True(t, c.IsFiction(2, 5, 3, obj))
	assert.False(t, c.IsFiction(3, 2, 3, obj))
	obj2 := rpc.AKID(43)
	assert.False(t, c.IsFiction(1, 2, 3, obj2))
	// KGOBject type matters
	assert.False(t, c.IsFiction(1, 2, 3, rpc.AInt64(42, 0)))
}

func Test_FactCache(t *testing.T) {
	c := New()
	obj := rpc.AKID(32)
	obj2 := rpc.AKID(33)
	f1 := rpc.Fact{Id: 1, Index: 1, Subject: 2, Predicate: 3, Object: obj}
	f2 := rpc.Fact{Id: 10, Index: 1, Subject: 12, Predicate: 13, Object: obj}
	c.AddFact(&f1)
	c.AddFact(&f2)
	assertEqualsFactAndPath(t, c, &f1, noPath, 1, 2, 3, obj)
	assertEqualsFactAndPath(t, c, &f2, noPath, 1, 12, 13, obj)
	assertEqualsFactAndPath(t, c, nil, noPath, 2, 12, 13, obj)
	assertEqualsFactAndPath(t, c, nil, noPath, 1, 13, 13, obj)
	assertEqualsFactAndPath(t, c, nil, noPath, 1, 12, 13, obj2)
	assertEqualsFactAndPath(t, c, nil, noPath, 1, 2, 3, rpc.AInt64(32, 0))
}

func Test_InferredFactCache(t *testing.T) {
	c := New()
	f1 := rpc.Fact{Index: 1, Subject: 1, Predicate: 2, Object: rpc.AKID(32)}
	p1 := []rpc.Fact{
		{Index: 1, Subject: 1, Predicate: 2, Object: rpc.AKID(3)},
		{Index: 1, Subject: 3, Predicate: 2, Object: rpc.AKID(31)},
		{Index: 1, Subject: 31, Predicate: 2, Object: rpc.AKID(32)},
	}
	c.AddInferedFact(&f1, p1)
	_, ap := assertEqualsFactAndPath(t, c, &f1, p1, 1, 1, 2, rpc.AKID(32))
	// if the caller futz with the returned path, it shouldn't affect the cached version
	ap[0].Index = 2
	assertEqualsFactAndPath(t, c, &f1, p1, 1, 1, 2, rpc.AKID(32))
	// if the path passed to Add is fiddled with, that shouldn't affect the cached version either
	originalp1 := copyOfPath(p1)
	p1[0].Index = 2
	assertEqualsFactAndPath(t, c, &f1, originalp1, 1, 1, 2, rpc.AKID(32))
	assertEqualsFactAndPath(t, c, nil, noPath, 2, 1, 2, rpc.AKID(32))
	assertEqualsFactAndPath(t, c, nil, noPath, 1, 1, 2, rpc.AKID(33))
}

func assertEqualsFactAndPath(t *testing.T, c FactCache, expFact *rpc.Fact, expPath []rpc.Fact,
	index uint64, subject, predicate uint64, obj rpc.KGObject) (actual *rpc.Fact, actualPath []rpc.Fact) {

	actFact, actPath := c.Fact(index, subject, predicate, obj)
	assert.Equal(t, actFact, expFact)
	assert.Equal(t, actPath, expPath)
	return actFact, actPath
}
