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

package main

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/ebay/akutan/logspec"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// Server runs a logspec-compatible gRPC service backed by an in-memory slice.
type Server struct {
	// A background context that is only closed when the server shuts down.
	ctx context.Context
	// User-supplied options.
	options *Options
	// This is used in conjunction with options.Limit to discard entries from
	// the log. A second value is needed to avoid copying the entries slice on
	// every append. If >0, when len(entries) reaches this size, the prefix of
	// the log is truncated such that len(entries) == options.Limit.
	hardLimit int
	// lock protects the fields in locked.
	lock   sync.Mutex
	locked struct {
		// The first index in the log, which is possibly not yet created. Starts at 1 and grows monotonically.
		startIndex Index
		// entries[i] is the log entry with index startIndex+i. The reachable
		// underlying array of pointers is immutable, as is each logspec.Entry
		// value.
		entries []*logspec.Entry
		// changed is closed and re-initialized every time 'startIndex' or 'entries'
		// changes. It is never nil.
		changed chan struct{}
	}
}

// An Index into the log, starting from 1.
type Index = uint64

// Options contains immutable settings for Server.
type Options struct {
	// The host:port to serve gRPC requests on.
	Address string
	// If >0, sometime after the log reaches at least this many entries, the
	// oldest entries will be automatically discarded. At least this number of
	// entries will be kept. If <= 0, auto-discarding the log is disabled.
	Limit int
}

// NewServer constructs a Server and starts listening for gRPC requests. The
// given context controls the lifetime of the server; close it to shut the
// server down.
func NewServer(ctx context.Context, options *Options) (*Server, error) {
	server := &Server{
		ctx:       ctx,
		options:   options,
		hardLimit: options.Limit * 3 / 2,
	}
	logrus.WithFields(logrus.Fields{
		"address":   options.Address,
		"softLimit": options.Limit,
		"hardLimit": server.hardLimit,
	}).Info("Starting new server")
	server.locked.startIndex = 1
	server.locked.changed = make(chan struct{})
	listener, err := net.Listen("tcp", options.Address)
	if err != nil {
		return nil, err
	}
	const maxMsgSize = 1024 * 1024 * 1024
	service := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize))
	logspec.RegisterLogServer(service, server)
	go service.Serve(listener)
	go func() {
		<-ctx.Done()
		listener.Close()
		service.Stop()
	}()
	return server, nil
}

// Append implements the RPC defined in the logspec.
func (server *Server) Append(streams logspec.Log_AppendServer) error {
	seq := uint64(0)
	for {
		seq++
		req, err := streams.Recv()
		if err != nil {
			logrus.WithError(err).Info("Append: error receiving from client")
			return err
		}
		if req.Sequence != seq {
			logrus.WithFields(logrus.Fields{
				"got":      req.Sequence,
				"expected": seq,
			}).Error("Received Append request with invalid sequence number")
			return errors.New("out of sequence")
		}
		indexes := server.append(req.Proposals)
		res := logspec.AppendReply{
			Reply: &logspec.AppendReply_Ok{
				Ok: &logspec.AppendReply_OK{
					Sequence: seq,
					Indexes:  indexes,
				},
			},
		}
		err = streams.Send(&res)
		if err != nil {
			logrus.WithError(err).Info("Append: error sending to client")
			return err
		}
	}
}

// append adds entries to the log. It must be called without holding the lock.
func (server *Server) append(proposals [][]byte) []Index {
	server.lock.Lock()
	defer server.lock.Unlock()
	prevLastIndex := server.lastIndexLocked()
	nextIndex := prevLastIndex + 1
	indexes := make([]Index, len(proposals))
	for i := range proposals {
		indexes[i] = nextIndex
		nextIndex++
		server.locked.entries = append(server.locked.entries, &logspec.Entry{
			Index: indexes[i],
			Data:  proposals[i],
			Skip:  false,
		})
	}
	logrus.WithFields(logrus.Fields{
		"count":         len(proposals),
		"startIndex":    server.locked.startIndex,
		"prevLastIndex": prevLastIndex,
		"newLastIndex":  server.lastIndexLocked(),
	}).Debug("Appended new entries")

	if server.hardLimit > 0 && len(server.locked.entries) >= server.hardLimit {
		firstIndex := nextIndex - uint64(server.options.Limit)
		diff := firstIndex - server.locked.startIndex
		server.locked.entries = append([]*logspec.Entry(nil),
			server.locked.entries[diff:]...)
		logrus.WithFields(logrus.Fields{
			"prevStartIndex": server.locked.startIndex,
			"newStartIndex":  firstIndex,
			"lastIndex":      server.lastIndexLocked(),
			"retained":       len(server.locked.entries),
			"discarded":      firstIndex - server.locked.startIndex,
			"softLimit":      server.options.Limit,
			"hardLimit":      server.hardLimit,
		}).Warn("Auto-discarded old entries")
		server.locked.startIndex = firstIndex
	}

	close(server.locked.changed)
	server.locked.changed = make(chan struct{})
	return indexes
}

// lastIndexLocked returns the index of the last log entry. For an empty log, it
// returns one index before the log's start index. It must be called with the
// lock held.
func (server *Server) lastIndexLocked() Index {
	return server.locked.startIndex + uint64(len(server.locked.entries)) - 1
}

// Discard implements the RPC defined in the logspec.
func (server *Server) Discard(ctx context.Context, req *logspec.DiscardRequest) (*logspec.DiscardReply, error) {
	server.discard(req.FirstIndex)
	return &logspec.DiscardReply{
		Reply: &logspec.DiscardReply_Ok{
			Ok: &logspec.DiscardReply_OK{},
		},
	}, nil
}

// discard truncates the prefix of the log. It must be called without holding
// the lock.
func (server *Server) discard(firstIndex Index) {
	server.lock.Lock()
	defer server.lock.Unlock()
	if firstIndex <= server.locked.startIndex {
		return
	}
	diff := firstIndex - server.locked.startIndex
	if uint64(len(server.locked.entries)) < diff {
		server.locked.entries = nil
	} else {
		server.locked.entries = append([]*logspec.Entry(nil),
			server.locked.entries[diff:]...)
	}
	logrus.WithFields(logrus.Fields{
		"prevStartIndex": server.locked.startIndex,
		"newStartIndex":  firstIndex,
		"lastIndex":      server.lastIndexLocked(),
		"retained":       len(server.locked.entries),
		"discarded":      firstIndex - server.locked.startIndex,
	}).Warn("Discarded entries")
	server.locked.startIndex = firstIndex
	close(server.locked.changed)
	server.locked.changed = make(chan struct{})
}

// Read implements the RPC defined in the logspec.
func (server *Server) Read(req *logspec.ReadRequest, stream logspec.Log_ReadServer) error {
	ctx := stream.Context()
	nextIndex := req.NextIndex
	for {
		server.lock.Lock()
		startIndex := server.locked.startIndex
		lastIndex := server.lastIndexLocked()
		entries := server.locked.entries
		changedCh := server.locked.changed
		server.lock.Unlock()

		// Asking for discarded entries: reject.
		if nextIndex < startIndex {
			logrus.WithFields(logrus.Fields{
				"need":  nextIndex,
				"start": startIndex,
			}).Warn("Attempted to read discarded entries")
			return stream.Send(&logspec.ReadReply{
				Reply: &logspec.ReadReply_Truncated{
					Truncated: true,
				},
			})
		}

		// Reached the end of the log: wait.
		if nextIndex > lastIndex {
			select {
			case <-ctx.Done():
				logrus.Info("Read client disconnected")
				return ctx.Err()
			case <-server.ctx.Done():
				return server.ctx.Err()
			case <-changedCh:
				continue
			}
		}

		// Send the entries.
		entries = entries[nextIndex-startIndex:]
		if len(entries) > 10 {
			entries = entries[:10]
		}
		err := stream.Send(&logspec.ReadReply{
			Reply: &logspec.ReadReply_Ok{
				Ok: &logspec.ReadReply_OK{
					Entries: entries,
				},
			},
		})
		if err != nil {
			logrus.WithError(err).Info("Read: error sending to client")
			return err
		}
		nextIndex += uint64(len(entries))
	}
}

// Info implements the RPC defined in the logspec.
func (server *Server) Info(context.Context, *logspec.InfoRequest) (*logspec.InfoReply, error) {
	res, _ := server.info()
	return res, nil
}

// info returns statistics about the log, and a channel that will be closed when
// the log changes. It must be called without holding the lock.
func (server *Server) info() (*logspec.InfoReply, chan struct{}) {
	server.lock.Lock()
	defer server.lock.Unlock()
	return &logspec.InfoReply{
		Reply: &logspec.InfoReply_Ok{
			Ok: &logspec.InfoReply_OK{
				FirstIndex: server.locked.startIndex,
				LastIndex:  server.lastIndexLocked(),
			},
		},
	}, server.locked.changed
}

// InfoStream implements the RPC defined in the logspec.
func (server *Server) InfoStream(req *logspec.InfoRequest, stream logspec.Log_InfoStreamServer) error {
	for {
		res, changedCh := server.info()
		err := stream.Send(res)
		if err != nil {
			logrus.WithError(err).Info("Info: error sending to client")
			return err
		}
		select {
		case <-stream.Context().Done():
			logrus.Info("Info client disconnected")
			return stream.Context().Err()
		case <-server.ctx.Done():
			return server.ctx.Err()
		case <-changedCh:
		}
	}
}
