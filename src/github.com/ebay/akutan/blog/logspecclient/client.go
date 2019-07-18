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

// Package logspecclient implements a client for the akutan/logspec API.
package logspecclient

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/config"
	"github.com/ebay/akutan/discovery"
	"github.com/ebay/akutan/logspec"
	"github.com/ebay/akutan/util/clocks"
	grpcclientutil "github.com/ebay/akutan/util/grpc/client"
	"github.com/ebay/akutan/util/random"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const backoff = 100 * time.Millisecond
const fullBackoff = 2 * backoff

func init() {
	factory := func(
		ctx context.Context, cfg *config.Akutan, servers discovery.Locator,
	) (blog.AkutanLog, error) {
		return New(ctx, cfg, servers)
	}
	blog.Factories["logspec"] = factory
}

// Log is a client to the logspec API.
type Log struct {
	logger *logrus.Entry
	// Used to find server addresses.
	servers discovery.Locator
	// Context for background goroutines.
	ctx context.Context
	// Protects 'locked'. Held only for short durations.
	lock sync.Mutex
	// The fields in this struct are protected by 'lock'.
	locked struct {
		// Currently open connection to use for new requests. Only the pointer
		// is protected by the Log lock; the clientConn value is safe to share.
		client *clientConn
		// Set to the endpoint that was just disconnected from. This endpoint
		// will be ignored in the next connectAny attempt.
		lastEndpoint string
	}
	// Each request is <= appendBatchSize.
	unbatchedAppends chan appendFuture
	// Each group of requests is <= appendBatchSize.
	batchedAppends chan appendBatch
	// The clock to use for all timing related operations.
	clock clocks.Source
	// The target size in bytes for a single appendBatch.
	appendBatchSize int
}

// A clientConn represents a connection to a particular server.
type clientConn struct {
	// Protects 'locked'. Held only for short durations.
	lock sync.Mutex
	// The fields in this struct are protected by 'lock'.
	locked struct {
		// Best current description for the server, intended for human consumption.
		server string
	}
	// The underlying gRPC connection.
	conn *grpc.ClientConn
	// The RPC stub that wraps 'conn'.
	service logspec.LogClient
}

// server returns the best current description for the server, which is useful
// to include in log messages. This may be vague until the Dialer has chosen a
// server.
func (cc *clientConn) server() string {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	return cc.locked.server
}

// internalOptions contains various things used by the akutanLog that unit tests
// may want to have different values for.
type internalOptions struct {
	clock           clocks.Source
	startGoroutines bool
	appendBatchSize int
}

func defaultInternalOptions() internalOptions {
	return internalOptions{
		clock:           clocks.Wall,
		startGoroutines: true,
		appendBatchSize: 8 * 1024 * 1024,
	}
}

// New constructs a Log.
func New(
	ctx context.Context,
	cfg *config.Akutan, servers discovery.Locator,
) (*Log, error) {
	return newLog(ctx, cfg, servers, defaultInternalOptions())
}

func newLog(
	ctx context.Context,
	cfg *config.Akutan, servers discovery.Locator,
	opts internalOptions,
) (*Log, error) {
	aLog := &Log{
		ctx:              ctx,
		logger:           logrus.NewEntry(logrus.StandardLogger()),
		servers:          servers,
		unbatchedAppends: make(chan appendFuture),
		batchedAppends:   make(chan appendBatch),
		clock:            opts.clock,
		appendBatchSize:  opts.appendBatchSize,
	}

	// Run cleanup after ctx is canceled.
	go func() {
		<-ctx.Done()
		aLog.Disconnect()
	}()

	if opts.startGoroutines {
		go aLog.batchAppends()
		go aLog.runAppends()
	}
	return aLog, nil
}

// Discard implements the method declared in blog.AkutanLog.
func (aLog *Log) Discard(ctx context.Context, firstIndex blog.Index) error {
	for {
		client, err := aLog.getConnection()
		if err != nil {
			return err
		}
		err = aLog.tryDiscard(ctx, client, firstIndex)
		switch err {
		case nil:
			return nil
		case ctx.Err():
			return err
		default:
			aLog.logger.WithFields(logrus.Fields{
				"RPC":    "Discard",
				"server": client.server(),
				"error":  err,
			}).Warnf("Retrying")
			continue
		}
	}
}

func (aLog *Log) tryDiscard(ctx context.Context, client *clientConn, firstIndex blog.Index) error {
	req := logspec.DiscardRequest{
		FirstIndex: firstIndex,
	}
	res, err := client.service.Discard(ctx, &req)
	if err != nil {
		return aLog.handleRPCError(ctx, client, err)
	}
	switch res := res.Reply.(type) {
	case *logspec.DiscardReply_Ok:
		return nil
	case *logspec.DiscardReply_Redirect:
		return aLog.handleRedirect(client, res.Redirect)
	default:
		return aLog.handleUnknownError(ctx, client)
	}
}

// Read implements the method declared in blog.AkutanLog.
func (aLog *Log) Read(ctx context.Context, nextIndex blog.Index, entriesCh chan<- []blog.Entry) error {
	defer close(entriesCh)
	for {
		client, err := aLog.getConnection()
		if err != nil {
			return err
		}
		err = aLog.tryRead(ctx, client, &nextIndex, entriesCh)
		switch {
		case err == ctx.Err():
			return err
		case blog.IsTruncatedError(err):
			return err
		default:
			aLog.disconnectFrom(client)
			aLog.logger.WithFields(logrus.Fields{
				"RPC":    "Read",
				"server": client.server(),
				"error":  err,
			}).Warnf("Retrying")
			continue
		}
	}
}

func (aLog *Log) tryRead(ctx context.Context, client *clientConn, nextIndex *blog.Index, entriesCh chan<- []blog.Entry) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()
	// Also cancel the stream when the entire log is shutting down.
	go func() {
		select {
		case <-aLog.ctx.Done():
			cancelFunc()
		case <-ctx.Done():
		}
	}()

	req := logspec.ReadRequest{
		NextIndex: *nextIndex,
	}
	stream, err := client.service.Read(ctx, &req)
	if err != nil {
		return aLog.handleRPCError(ctx, client, err)
	}
	for {
		res, err := stream.Recv()
		if err != nil {
			return aLog.handleRPCError(ctx, client, err)
		}
		switch res := res.Reply.(type) {
		case *logspec.ReadReply_Ok:
			entries := make([]blog.Entry, len(res.Ok.Entries))
			bytes := 0
			for i := range res.Ok.Entries {
				entries[i] = *res.Ok.Entries[i]
				bytes += bytesLen(entries[i].Data)
				if *nextIndex != entries[i].Index {
					// Entries from the log should have sequential indexes, but
					// this entry doesn't.
					msg := strings.Builder{}
					fmt.Fprintf(&msg, "received invalid log index from Log service server: %s\n", client.server())
					fmt.Fprintf(&msg, "expected log index %d, but got %d\n", *nextIndex, entries[i].Index)
					fmt.Fprintf(&msg, "log store read started at log index %d\n", req.NextIndex)
					fmt.Fprintf(&msg, "ReadReply from Log service has entries:\n")
					for _, e := range res.Ok.Entries {
						fmt.Fprintf(&msg, "\tidx=%d skip=%v data=%v\n", e.Index, e.Skip, e.Data)
					}
					panic(msg.String())
				}
				*nextIndex = entries[i].Index + 1
			}
			metrics.readBytes.Observe(float64(bytes))
			metrics.readEntries.Observe(float64(len(entries)))
			select {
			case entriesCh <- entries:
			case <-ctx.Done():
				return ctx.Err()
			}
		case *logspec.ReadReply_Redirect:
			return aLog.handleRedirect(client, res.Redirect)
		case *logspec.ReadReply_Truncated:
			return blog.TruncatedError{Requested: req.NextIndex}
		default:
			return aLog.handleUnknownError(ctx, client)
		}
	}
}

// Info implements the method declared in blog.AkutanLog.
func (aLog *Log) Info(ctx context.Context) (*blog.Info, error) {
	for {
		client, err := aLog.getConnection()
		if err != nil {
			return nil, err
		}
		info, err := aLog.tryInfo(ctx, client)
		switch err {
		case nil:
			return info, nil
		case ctx.Err():
			return nil, err
		default:
			aLog.logger.WithFields(logrus.Fields{
				"RPC":    "Info",
				"server": client.server(),
				"error":  err,
			}).Warnf("Retrying")
			continue
		}
	}
}

func (aLog *Log) tryInfo(ctx context.Context, client *clientConn) (*blog.Info, error) {
	req := logspec.InfoRequest{}
	res, err := client.service.Info(ctx, &req)
	if err != nil {
		return nil, aLog.handleRPCError(ctx, client, err)
	}
	switch res := res.Reply.(type) {
	case *logspec.InfoReply_Ok:
		return res.Ok, nil
	case *logspec.InfoReply_Redirect:
		return nil, aLog.handleRedirect(client, res.Redirect)
	default:
		return nil, aLog.handleUnknownError(ctx, client)
	}
}

// InfoStream implements the method declared in blog.AkutanLog.
func (aLog *Log) InfoStream(ctx context.Context, infoCh chan<- *blog.Info) error {
	defer close(infoCh)
	for {
		client, err := aLog.getConnection()
		if err != nil {
			return err
		}
		err = aLog.tryInfoStream(ctx, client, infoCh)
		switch err {
		case ctx.Err():
			return err
		default:
			aLog.logger.WithFields(logrus.Fields{
				"RPC":    "InfoStream",
				"server": client.server(),
				"error":  err,
			}).Warnf("Retrying")
			continue
		}
	}
}

func (aLog *Log) tryInfoStream(ctx context.Context, client *clientConn, infoCh chan<- *blog.Info) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()
	// Also cancel the stream when the entire log is shutting down.
	go func() {
		select {
		case <-aLog.ctx.Done():
			cancelFunc()
		case <-ctx.Done():
		}
	}()

	req := logspec.InfoRequest{}
	stream, err := client.service.InfoStream(ctx, &req)
	if err != nil {
		return aLog.handleRPCError(ctx, client, err)
	}
	for {
		res, err := stream.Recv()
		if err != nil {
			return aLog.handleRPCError(ctx, client, err)
		}
		switch res := res.Reply.(type) {
		case *logspec.InfoReply_Ok:
			select {
			case infoCh <- res.Ok:
			case <-ctx.Done():
				return ctx.Err()
			}
		case *logspec.InfoReply_Redirect:
			return aLog.handleRedirect(client, res.Redirect)
		default:
			return aLog.handleUnknownError(ctx, client)
		}
	}
}

// getConnection returns an open connection to a server; it opens a new
// connection if needed. getConnection will return quickly, even if it needs a
// new connection. It returns either an open connection and a nil error, or
// ErrClosed. It swallows connection errors and retries them.
func (aLog *Log) getConnection() (*clientConn, error) {
	aLog.lock.Lock()
	defer aLog.lock.Unlock()
	if aLog.ctx.Err() != nil {
		return nil, blog.ClosedError{}
	}
	if aLog.locked.client == nil {
		aLog.connectAnyLocked()
	}
	return aLog.locked.client, nil
}

func init() {
	// math/rand is used in connectAnyLocked
	random.SeedMath()
}

// connectAnyLocked opens a new connection to some available server. It
// disconnects any existing connection and saves the new connection for later
// use. connectAnyLocked will return quickly (since grpc.Dial returns quickly).
// It must be called with the aLog.lock held. Returns nil on success, a
// context error if the log is shutting down, or (unlikely) some immediate
// connection error on failure.
func (aLog *Log) connectAnyLocked() {
	if aLog.locked.client != nil {
		aLog.disconnectFromLocked(aLog.locked.client) // clears out aLog.locked.client
	}
	locatorString := aLog.servers.String()
	cc := new(clientConn)
	// This string will be overwritten by the Dialer from another goroutine later.
	cc.locked.server = locatorString
	dialer := func(ctx context.Context, target string) (net.Conn, error) {
		result, err := discovery.GetNonempty(ctx, aLog.servers)
		if err != nil {
			return nil, err
		}
		endpoints := result.Endpoints
		// If there are enough endpoints, ignore the most recently used endpoint.
		if len(endpoints) > 1 && aLog.locked.lastEndpoint != "" {

			endpoints = make([]*discovery.Endpoint, 0, len(endpoints)-1)
			for _, e := range result.Endpoints {
				if e.String() != aLog.locked.lastEndpoint {
					endpoints = append(endpoints, e)
				}
			}
			// If there's a configuration error, such as having multiple endpoint instances
			// all with the same address its possible that endpoints is now empty.
			if len(endpoints) == 0 {
				endpoints = result.Endpoints
			}
		}
		aLog.locked.lastEndpoint = ""
		endpoint := endpoints[rand.Intn(len(endpoints))]
		endpointStr := endpoint.String()
		cc.lock.Lock()
		cc.locked.server = endpointStr
		cc.lock.Unlock()
		aLog.logger.WithFields(logrus.Fields{
			"server": endpointStr,
		}).Infof("Logspec client connecting to")
		return new(net.Dialer).DialContext(ctx, endpoint.Network, endpoint.HostPort())
	}
	conn := grpcclientutil.InsecureDialContext(aLog.ctx, locatorString, grpc.WithContextDialer(dialer))
	cc.conn = conn
	cc.service = logspec.NewLogClient(conn)
	aLog.locked.client = cc
}

// connectToLocked is like connectAnyLocked but opens a connection to the
// particular server given by 'address'.
func (aLog *Log) connectToLocked(address string) {
	if aLog.locked.client != nil {
		aLog.disconnectFromLocked(aLog.locked.client) // clears out aLog.locked.client
	}
	conn := grpcclientutil.InsecureDialContext(aLog.ctx, address)
	cc := &clientConn{
		conn:    conn,
		service: logspec.NewLogClient(conn),
	}
	cc.locked.server = address
	aLog.locked.client = cc
	aLog.logger.WithFields(logrus.Fields{
		"server": address,
	}).Infof("Logspec client connecting to")
}

// Disconnect implements the method declared in blog.Disconnector.
func (aLog *Log) Disconnect() {
	aLog.lock.Lock()
	defer aLog.lock.Unlock()
	if aLog.locked.client != nil {
		aLog.disconnectFromLocked(aLog.locked.client)
		metrics.disconnectsTotal.Inc()
	}
}

// disconnectFrom closes the connection to 'from' and ensures it won't be
// returned by getConnection. It must be called without holding aLog.lock.
func (aLog *Log) disconnectFrom(from *clientConn) {
	aLog.lock.Lock()
	defer aLog.lock.Unlock()
	aLog.disconnectFromLocked(from)
}

// disconnectFromLocked closes the connection to 'from' and ensures it won't be
// returned by getConnection. It must be called with aLog.lock held.
func (aLog *Log) disconnectFromLocked(from *clientConn) {
	err := from.conn.Close()
	if err != nil {
		if status.Code(err) != codes.Canceled {
			aLog.logger.WithFields(logrus.Fields{
				"server": from.server(),
				"error":  err,
			}).Warnf("Error closing connection")
		}
	}
	if aLog.locked.client == from {
		aLog.locked.lastEndpoint = from.server()
		aLog.locked.client = nil
	}
}

// handleRedirect disconnects from 'from' and, if 'info' contains a host
// address, tries to use that for the next connection. It returns quickly and
// always returns a non-nil error with a message describing the redirection.
func (aLog *Log) handleRedirect(from *clientConn, info *logspec.Redirect) error {
	metrics.redirectsTotal.Inc()
	aLog.lock.Lock()
	defer aLog.lock.Unlock()
	if aLog.locked.client == from {
		aLog.disconnectFromLocked(from) // clears out aLog.locked.client
		if aLog.ctx.Err() == nil && info.Host != "" {
			aLog.connectToLocked(info.Host)
		}
	} else {
		aLog.disconnectFromLocked(from)
	}
	if info.Host != "" {
		return fmt.Errorf("redirected to %v", info.Host)
	}
	return errors.New("redirected (to no particular host)")
}

// handleUnknownError is called for non-OK replies that the client doesn't understand.
// It disconnects from 'from' and waits 'backoff', then returns with either ctx.Err()
// or a non-nil error mentioning a non-OK reply.
func (aLog *Log) handleUnknownError(ctx context.Context, from *clientConn) error {
	aLog.disconnectFrom(from)
	err := aLog.clock.SleepUntil(ctx, aLog.clock.Now().Add(backoff))
	if err != nil {
		return err
	}
	return errors.New("got unknown non-OK reply")
}

// handleRPCError is called for gRPC errors. It disconnects from 'from' and
// waits 'backoff', then returns with either ctx.Err() or rpcErr.
func (aLog *Log) handleRPCError(ctx context.Context, from *clientConn, rpcErr error) error {
	aLog.disconnectFrom(from)
	err := aLog.clock.SleepUntil(ctx, aLog.clock.Now().Add(backoff))
	if err != nil {
		return err
	}
	return rpcErr
}
