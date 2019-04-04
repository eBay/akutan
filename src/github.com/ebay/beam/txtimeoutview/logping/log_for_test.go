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

package logping

import (
	"context"
	"sync"

	"github.com/ebay/beam/blog"
)

type mockLogAppender struct {
	appendSingle func(data []byte) (blog.Index, error)
}

func (beamLog *mockLogAppender) AppendSingle(ctx context.Context, data []byte) (blog.Index, error) {
	return beamLog.appendSingle(data)
}

type mockLogReader struct {
	read   func(nextIndex blog.Index, entriesCh chan<- []blog.Entry) error
	lock   sync.Mutex
	locked struct {
		numDisconnects int
	}
}

func (beamLog *mockLogReader) Read(ctx context.Context, nextIndex blog.Index, entriesCh chan<- []blog.Entry) error {
	return beamLog.read(nextIndex, entriesCh)
}

func (beamLog *mockLogReader) Disconnect() {
	beamLog.lock.Lock()
	beamLog.locked.numDisconnects++
	beamLog.lock.Unlock()
}

func (beamLog *mockLogReader) NumDisconnects() int {
	beamLog.lock.Lock()
	n := beamLog.locked.numDisconnects
	beamLog.lock.Unlock()
	return n
}
