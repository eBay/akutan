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

package impl

import (
	"context"
	"time"

	"github.com/ebay/beam/api"
	"github.com/ebay/beam/update"
)

// Insert implements the gRPC method of the BeamFactStore service.
func (s *Server) Insert(ctx context.Context, req *api.InsertRequest) (*api.InsertResult, error) {
	// TODO: we should probably do this on all our RPC handlers
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	recentIdx, err := s.fetchRecentLogIndex(ctx, s.beamLog)
	if err != nil {
		return nil, err
	}
	return update.Insert(ctx, req, recentIdx, s.source, s.beamLog)
}
