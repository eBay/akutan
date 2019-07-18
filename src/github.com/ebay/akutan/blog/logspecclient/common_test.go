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

package logspecclient

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/config"
	"github.com/ebay/akutan/discovery"
	"github.com/ebay/akutan/logspec"
	"github.com/ebay/akutan/util/clocks"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type mockServer struct {
	t         *testing.T
	clock     *clocks.Mock
	listener  net.Listener
	grpc      *grpc.Server
	stopClock func()
	next      struct {
		redirect int // 3 = invalid host, 2 = local addr, 1 = empty, 0 = don't redirect
		unknown  bool
		err      error
		full     bool
	}
	// If nil, Read will do the default of taking data from entries. Otherwise
	// the test can set this and perform specific behavior. The redirect check
	// using 'next' above is performed before this is called.
	read func(*mockServer, logspec.Log_ReadServer, blog.Index) error

	// mutex protects startIndex, entries, and changed.
	mutex      sync.Mutex
	startIndex blog.Index
	entries    []*blog.Entry
	// changed is closed and re-initialized every time 'startIndex' or 'entries' changes.
	changed chan struct{}
}

func setup(t *testing.T) (*mockServer, *Log, func()) {
	log.SetLevel(log.DebugLevel)
	server := newMockServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	endpoint := &discovery.Endpoint{
		Network: "tcp",
	}
	var err error
	endpoint.Host, endpoint.Port, err = net.SplitHostPort(server.listener.Addr().String())
	assert.NoError(t, err)
	log, err := newLog(ctx,
		&config.Akutan{
			AkutanLog: config.AkutanLog{
				Type: "logspec",
			},
		},
		discovery.NewStaticLocator([]*discovery.Endpoint{endpoint}),
		internalOptions{
			clock:           server.clock,
			appendBatchSize: 100,
			startGoroutines: false,
		})
	assert.NoError(t, err)
	return server, log, func() {
		cancel()
		server.Close()
	}
}

func newMockServer(t *testing.T) *mockServer {
	server := &mockServer{
		t:          t,
		clock:      clocks.NewMock(),
		startIndex: 1,
		changed:    make(chan struct{}),
	}
	maxMsgSize := 1024 * 1024 * 1024

	var err error
	server.listener, err = net.Listen("tcp", "127.0.0.1:")
	require.NoError(server.t, err, "Listen")

	server.grpc = grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize))
	logspec.RegisterLogServer(server.grpc, server)
	go server.grpc.Serve(server.listener)
	return server
}

func (server *mockServer) startClock() {
	if server.stopClock == nil {
		var ctx context.Context
		ctx, server.stopClock = context.WithCancel(context.Background())
		go func() {
			for ctx.Err() == nil {
				server.clock.Advance(time.Second)
				time.Sleep(100 * time.Microsecond)
			}
		}()
	}
}

func (server *mockServer) Close() {
	server.listener.Close()
	server.grpc.Stop()
	if server.stopClock != nil {
		server.stopClock()
		server.stopClock = nil
	}
}

func (server *mockServer) append(proposals [][]byte) []blog.Index {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	nextIndex := server.startIndex + uint64(len(server.entries))
	indexes := make([]blog.Index, len(proposals))
	for i := range proposals {
		indexes[i] = nextIndex
		nextIndex++
		server.entries = append(server.entries, &blog.Entry{
			Index: indexes[i],
			Data:  proposals[i],
			Skip:  false,
		})
	}
	close(server.changed)
	server.changed = make(chan struct{})
	return indexes
}

func (server *mockServer) Append(streams logspec.Log_AppendServer) error {
	seq := uint64(0)
	for {
		seq++
		req, err := streams.Recv()
		if err != nil {
			return err
		}
		if req.Sequence != seq {
			panic("Out of sequence")
		}

		var res logspec.AppendReply
		switch {
		case server.next.redirect != 0:
			res.Reply = &logspec.AppendReply_Redirect{
				Redirect: server.redirect(),
			}
		case server.next.unknown:
			server.next.unknown = false
		case server.next.full:
			server.next.full = false
			res.Reply = &logspec.AppendReply_Full{
				Full: true,
			}
		case server.next.err != nil:
			err := server.next.err
			server.next.err = nil
			return err
		default:
			indexes := server.append(req.Proposals)
			res.Reply = &logspec.AppendReply_Ok{
				Ok: &logspec.AppendReply_OK{
					Sequence: seq,
					Indexes:  indexes,
				},
			}
		}
		err = streams.Send(&res)
		if err != nil {
			return err
		}
	}
}

func (server *mockServer) discard(firstIndex blog.Index) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if firstIndex > server.startIndex {
		diff := firstIndex - server.startIndex
		if uint64(len(server.entries)) < diff {
			server.entries = nil
		} else {
			server.entries = append([]*blog.Entry(nil), server.entries[diff:]...)
		}
		server.startIndex = firstIndex
		close(server.changed)
		server.changed = make(chan struct{})
	}
}

func (server *mockServer) Discard(ctx context.Context, req *logspec.DiscardRequest) (*logspec.DiscardReply, error) {
	switch {
	case server.next.redirect != 0:
		return &logspec.DiscardReply{
			Reply: &logspec.DiscardReply_Redirect{
				Redirect: server.redirect(),
			},
		}, nil
	case server.next.unknown:
		server.next.unknown = false
		return &logspec.DiscardReply{}, nil
	case server.next.err != nil:
		err := server.next.err
		server.next.err = nil
		return nil, err
	default:
		server.discard(req.FirstIndex)
		return &logspec.DiscardReply{
			Reply: &logspec.DiscardReply_Ok{
				Ok: &logspec.DiscardReply_OK{},
			},
		}, nil
	}
}

func (server *mockServer) get(index blog.Index) *logspec.Entry {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if index < server.startIndex {
		return nil
	}
	i := index - server.startIndex
	if i < uint64(len(server.entries)) {
		return server.entries[i]
	}
	return nil
}

func (server *mockServer) Read(req *logspec.ReadRequest, stream logspec.Log_ReadServer) error {
	ctx := stream.Context()
	nextIndex := req.NextIndex
	for {
		if nextIndex < server.startIndex {
			log.Debugf("truncated: need %v, have %v", nextIndex, server.startIndex)
			return stream.Send(&logspec.ReadReply{
				Reply: &logspec.ReadReply_Truncated{
					Truncated: true,
				},
			})
		}
		lastIndex := server.startIndex + uint64(len(server.entries)) - 1
		if nextIndex > lastIndex { // block
			<-ctx.Done()
			return ctx.Err()
		}

		switch {
		case server.next.redirect != 0:
			return stream.Send(&logspec.ReadReply{
				Reply: &logspec.ReadReply_Redirect{
					Redirect: server.redirect(),
				},
			})
		case server.next.unknown:
			server.next.unknown = false
			return stream.Send(&logspec.ReadReply{})
		case server.next.err != nil:
			err := server.next.err
			server.next.err = nil
			return err
		default:
			var err error
			if server.read != nil {
				err = server.read(server, stream, nextIndex)
			} else {
				err = stream.Send(&logspec.ReadReply{
					Reply: &logspec.ReadReply_Ok{
						Ok: &logspec.ReadReply_OK{
							Entries: []*logspec.Entry{
								server.get(nextIndex),
							},
						},
					},
				})
			}
			if err != nil {
				return err
			}
			nextIndex++
		}
	}
}

func (server *mockServer) info() *blog.Info {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return &logspec.InfoReply_OK{
		FirstIndex: server.startIndex,
		LastIndex:  server.startIndex + uint64(len(server.entries)) - 1,
	}
}

func (server *mockServer) Info(context.Context, *logspec.InfoRequest) (*logspec.InfoReply, error) {
	switch {
	case server.next.redirect != 0:
		return &logspec.InfoReply{
			Reply: &logspec.InfoReply_Redirect{
				Redirect: server.redirect(),
			},
		}, nil
	case server.next.unknown:
		server.next.unknown = false
		return &logspec.InfoReply{}, nil
	case server.next.err != nil:
		err := server.next.err
		server.next.err = nil
		return nil, err
	default:
		return &logspec.InfoReply{
			Reply: &logspec.InfoReply_Ok{
				Ok: server.info(),
			},
		}, nil
	}
}

func (server *mockServer) InfoStream(req *logspec.InfoRequest, stream logspec.Log_InfoStreamServer) error {
	ctx := stream.Context()
	for {
		res, err := server.Info(ctx, req)
		if err != nil {
			return err
		}
		err = stream.Send(res)
		if err != nil {
			return err
		}
		server.mutex.Lock()
		serverChangedCh := server.changed
		server.mutex.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-serverChangedCh:
		}
	}
}

func (server *mockServer) redirect() *logspec.Redirect {
	defer func() {
		server.next.redirect--
	}()
	switch server.next.redirect {
	case 3:
		return &logspec.Redirect{
			Host: "127.0.0.1:9999999999", // invalid addr
		}
	case 2:
		return &logspec.Redirect{
			Host: server.listener.Addr().String(),
		}
	case 1:
		return &logspec.Redirect{}
	default:
		log.Panicf("redirect called with unexpected %v", server.next.redirect)
		return nil
	}
}
