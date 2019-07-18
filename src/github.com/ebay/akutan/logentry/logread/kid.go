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
	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/logentry"
	"github.com/sirupsen/logrus"
)

// MaxOffset contains the limit for the number of offsets available for a single
// log entry. offset must be 1 <= offset < MaxOffset
const MaxOffset = 1000

// KID calculates a KID given a log index and an offset.
// KIDs represent entities in the graph: subject, predicate, factIds.
// Index must be > 0 and offset must be 1 <= offset < MaxOffset.
// If you pass an index or offset that's outside the valid ranges for those
// this function will panic.
func KID(index blog.Index, offset int32) uint64 {
	// They're made up by taking the log index and multiplying it by 1000 and
	// then adding the offset
	if offset >= MaxOffset || offset < 1 {
		logrus.Panicf("Offset of %d passed to logread.KID which is out of bounds", offset)
	}
	if index < 1 {
		logrus.Panicf("Index of %d passed to logread.KID which is out of bounds", index)
	}
	return index*MaxOffset + uint64(offset)
}

// KIDof will return the KID for this KIDOrOffset, resolving an offset to a
// KID given the provided log index if needed. It returns 0 if k does not have a specific
// KID or Offset set.
func KIDof(idx blog.Index, k *logentry.KIDOrOffset) uint64 {
	if k.Value == nil {
		return 0
	}
	switch t := k.Value.(type) {
	case *logentry.KIDOrOffset_Kid:
		return t.Kid
	case *logentry.KIDOrOffset_Offset:
		return KID(idx, t.Offset)
	}
	logrus.Panicf("KIDofKIDOrOffset passed a KIDOrOffset with an unexpected type %T %v", k.Value, k.Value)
	return 0 // never gets here
}
