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

	"github.com/ebay/akutan/blog"
)

type mockLogAppender struct {
	appendSingle func(data []byte) (blog.Index, error)
}

func (mlog *mockLogAppender) AppendSingle(ctx context.Context, data []byte) (blog.Index, error) {
	return mlog.appendSingle(data)
}

type mockLogReader struct {
	read   func(nextIndex blog.Index, entriesCh chan<- []blog.Entry) error
	lock   sync.Mutex
	locked struct {
		numDisconnects int
	}
}

func (mlog *mockLogReader) Read(ctx context.Context, nextIndex blog.Index, entriesCh chan<- []blog.Entry) error {
	return mlog.read(nextIndex, entriesCh)
}

func (mlog *mockLogReader) Disconnect() {
	mlog.lock.Lock()
	mlog.locked.numDisconnects++
	mlog.lock.Unlock()
}

func (mlog *mockLogReader) NumDisconnects() int {
	mlog.lock.Lock()
	n := mlog.locked.numDisconnects
	mlog.lock.Unlock()
	return n
}
