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
	"net"

	"github.com/ebay/akutan/api"
	grpcserverutil "github.com/ebay/akutan/util/grpc/server"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	log "github.com/sirupsen/logrus"
)

func (s *Server) startGRPC() error {
	log.Infof("Starting GRPPC API server on %v", s.cfg.API.GRPCAddress)
	l, err := net.Listen("tcp", s.cfg.API.GRPCAddress)
	if err != nil {
		return err
	}
	grpcServer := grpcserverutil.NewServer()
	api.RegisterFactStoreServer(grpcServer, s)
	grpc_prometheus.Register(grpcServer)
	go grpcServer.Serve(l)
	return nil
}
