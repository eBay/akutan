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

// Package grpcserverutil has helpers for configuring gRPC servers
package grpcserverutil

import (
	"context"
	"fmt"
	"strings"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// NewServer returns a new gRPC Server instance that has been configured with our common
// options, and to capture metrics & tracing
func NewServer() *grpc.Server {
	tracer := opentracing.GlobalTracer()
	if !strings.Contains(fmt.Sprintf("%v", tracer), "hostname") {
		log.Warnf("Initializing GRPC server but OpenTracing tracer looks empty: %+v", tracer)
	}
	maxMsgSize := 1024 * 1024 * 1024
	return grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             time.Second,
			PermitWithoutStream: true,
		}),
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
		grpc.UnaryInterceptor(UnaryInterceptorChain(
			grpc_prometheus.UnaryServerInterceptor,
			otgrpc.OpenTracingServerInterceptor(tracer))),
		grpc.StreamInterceptor(StreamInterceptorChain(
			grpc_prometheus.StreamServerInterceptor,
			otgrpc.OpenTracingStreamServerInterceptor(tracer))))
}

// UnaryInterceptorChain lets you use multiple interceptors on a single server.
// gRPC only lets you have one top-level UnaryServerInterceptor per server, so
// this chains them together.
func UnaryInterceptorChain(first, second grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		return first(ctx, req, info, func(ctx context.Context, req interface{}) (interface{}, error) {
			return second(ctx, req, info, handler)
		})
	}
}

// StreamInterceptorChain lets you use multiple stream interceptors on a single
// server. gRPC only lets you have one top-level StreamServerInterceptor per
// server, so this chains them together.
func StreamInterceptorChain(first, second grpc.StreamServerInterceptor) grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		return first(srv, stream, info, func(srv interface{}, stream grpc.ServerStream) error {
			return second(srv, stream, info, handler)
		})
	}
}
