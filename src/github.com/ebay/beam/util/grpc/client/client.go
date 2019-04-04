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

// Package grpcclientutil has helpers for configuring gRPC clients.
package grpcclientutil

import (
	"context"
	"fmt"
	"net"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
)

// InvalidConn returns a non-nil gRPC connection in the TransientFailure state.
// Callers should Close() the returned connection when they're done with it.
//
// InvalidConn can be useful to generate a dummy value in situations where
// having a nil grpc.ClientConn would be inconvenient.
func InvalidConn(msg string) *grpc.ClientConn {
	dialer := func(_ context.Context, _ string) (net.Conn, error) {
		return nil, fmt.Errorf("intentionally invalid connection (%v)", msg)
	}
	conn, err := grpc.Dial("-invalid-",
		grpc.WithInsecure(),
		grpc.WithContextDialer(dialer))
	if err != nil {
		panic(fmt.Sprintf("Expected nil error from grpc.Dial. Got %v", err))
	}
	for {
		state := conn.GetState()
		if state == connectivity.TransientFailure {
			return conn
		}
		conn.WaitForStateChange(context.Background(), state)
	}
}

// InsecureDialContext connects to the given address. It sets up Prometheus and
// OpenTracing monitoring. It disables transport-level security.
//
// The actual connection happens in the background; the given 'connectCtx' can
// be used to cancel this background activity. The caller must not pass
// grpc.WithBlock() as an option.
func InsecureDialContext(connectCtx context.Context, address string, opts ...grpc.DialOption) *grpc.ClientConn {
	tracer := opentracing.GlobalTracer()
	conn, err := grpc.DialContext(connectCtx, address, append([]grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithBackoffMaxDelay(time.Second),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			// 10s is the minimum keepalive allowed by the client library.
			Time:                time.Second * 10,
			Timeout:             time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(1024*1024*1024),
			grpc.MaxCallSendMsgSize(1024*1024*1024),
		),
		grpc.WithUnaryInterceptor(UnaryInterceptorChain(otgrpc.OpenTracingClientInterceptor(tracer),
			grpc_prometheus.UnaryClientInterceptor)),
		grpc.WithStreamInterceptor(StreamInterceptorChain(otgrpc.OpenTracingStreamClientInterceptor(tracer),
			grpc_prometheus.StreamClientInterceptor)),
	}, opts...)...)
	if err != nil {
		if err != connectCtx.Err() {
			logrus.WithError(err).Error("Expected only ctx.Err() from grpc.DialContext")
		}
		return InvalidConn(err.Error())
	}
	return conn
}

// UnaryInterceptorChain lets you use multiple interceptors on a single client.
// gRPC only lets you have one top-level UnaryClientInterceptor per client, so
// this chains them together.
func UnaryInterceptorChain(first, second grpc.UnaryClientInterceptor) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption) error {
		return first(ctx, method, req, reply, cc, func(
			ctx context.Context,
			method string,
			req, reply interface{},
			cc *grpc.ClientConn,
			opts ...grpc.CallOption,
		) error {
			return second(ctx, method, req, reply, cc, invoker, opts...)
		}, opts...)
	}
}

// StreamInterceptorChain lets you use multiple stream interceptors on a single
// client. gRPC only lets you have one top-level StreamClientInterceptor per
// client, so this chains them together.
func StreamInterceptorChain(first, second grpc.StreamClientInterceptor) grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return first(ctx, desc, cc, method, func(
			ctx context.Context,
			desc *grpc.StreamDesc,
			cc *grpc.ClientConn,
			method string,
			opts ...grpc.CallOption,
		) (grpc.ClientStream, error) {
			return second(ctx, desc, cc, method, streamer, opts...)
		}, opts...)
	}
}
