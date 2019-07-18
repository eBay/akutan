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

package diskview

import (
	"bytes"
	"testing"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/diskview/database"
	"github.com/ebay/akutan/diskview/keys"
	"github.com/ebay/akutan/rpc"
	"github.com/stretchr/testify/assert"
)

func TestApplyWipeAt(t *testing.T) {
	ks := newMockKS(rpc.KeyEncodingSPO,
		rpc.Fact{Index: 99, Id: 1, Subject: 1, Predicate: 1, Object: rpc.AKID(1)},
		rpc.Fact{Index: 99, Id: 1, Subject: 1, Predicate: 1, Object: rpc.AKID(2)},
		rpc.Fact{Index: 99, Id: 1, Subject: 1, Predicate: 1, Object: rpc.AKID(3)},
	)
	testApplyWipeAt(t, 99, ks, 3, [][]byte{})
	ks = newMockKS(rpc.KeyEncodingSPO,
		rpc.Fact{Index: 99, Id: 1, Subject: 1, Predicate: 1, Object: rpc.AKID(1)},
		rpc.Fact{Index: 99, Id: 1, Subject: 1, Predicate: 1, Object: rpc.AKID(2)},
		rpc.Fact{Index: 99, Id: 1, Subject: 1, Predicate: 1, Object: rpc.AKID(3)},
	)
	testApplyWipeAt(t, 98, ks, 3, [][]byte{})
	ks = newMockKS(rpc.KeyEncodingSPO,
		rpc.Fact{Index: 99, Id: 1, Subject: 1, Predicate: 1, Object: rpc.AKID(1)},
		rpc.Fact{Index: 99, Id: 1, Subject: 1, Predicate: 1, Object: rpc.AKID(2)},
		rpc.Fact{Index: 99, Id: 1, Subject: 1, Predicate: 1, Object: rpc.AKID(3)},
	)
	testApplyWipeAt(t, 100, ks, 3, [][]byte{})
	ks = newMockKS(rpc.KeyEncodingSPO,
		rpc.Fact{Index: 99, Id: 1, Subject: 1, Predicate: 1, Object: rpc.AKID(1)},
		rpc.Fact{Index: 99, Id: 1, Subject: 1, Predicate: 1, Object: rpc.AKID(2)},
		rpc.Fact{Index: 100, Id: 1, Subject: 1, Predicate: 1, Object: rpc.AKID(3)},
	)
	testApplyWipeAt(t, 100, ks, 3, [][]byte{})
}

func testApplyWipeAt(t *testing.T, wipeIdx blog.Index, mks *mockKS, expectedRemoveCount int, expected [][]byte) {
	ws, err := applyWipeAt(wipeIdx, mks, mks)
	assert.NoError(t, err)
	assert.NoError(t, mks.Close())
	assert.Equal(t, ws.AtIndex, wipeIdx)
	assert.Equal(t, int64(expectedRemoveCount), ws.RemovedCount)
	assert.Equal(t, expected, mks.keys)
}

// TODO: we have 2 mock key stores in this package, do we really need both?
type mockKS struct {
	keys   [][]byte
	remove map[int]bool
}

func (m *mockKS) EnumerateKeys(startKey, endKey []byte, emit func(key []byte) error) error {
	i := 0
	for ; len(startKey) > 0 && i < len(m.keys) && bytes.Compare(m.keys[i], startKey) < 0; i++ {
	}
	for ; i < len(m.keys) && bytes.Compare(m.keys[i], endKey) < 0; i++ {
		err := emit(m.keys[i])
		if err != nil {
			if err == database.ErrHalt {
				return nil
			}
			return err
		}
	}
	return nil
}

func (m *mockKS) Remove(k []byte) error {
	for i := 0; i < len(m.keys); i++ {
		if bytes.Equal(k, m.keys[i]) {
			m.remove[i] = true
			break
		}
	}
	return nil
}

func (m *mockKS) Close() error {
	update := [][]byte{}
	for i := 0; i < len(m.keys); i++ {
		if rm := m.remove[i]; rm {
			continue
		}
		update = append(update, m.keys[i])
	}
	m.keys = update
	m.remove = map[int]bool{}
	return nil
}

func newMockKS(ke rpc.FactKeyEncoding, facts ...rpc.Fact) *mockKS {
	mockKeys := make([][]byte, len(facts))
	for i := range facts {
		mockKeys[i] = keys.FactKey{
			Encoding: ke,
			Fact:     &facts[i],
		}.Bytes()
	}
	return &mockKS{mockKeys, map[int]bool{}}
}
