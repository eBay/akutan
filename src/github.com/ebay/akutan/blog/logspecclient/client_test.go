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
	"strings"
	"testing"
	"time"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/discovery"
	"github.com/ebay/akutan/logspec"
	"github.com/ebay/akutan/util/parallel"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/connectivity"
)

// Ensures that Log satisfies the intended interfaces.
var (
	_ blog.AkutanLog    = (*Log)(nil)
	_ blog.Disconnector = (*Log)(nil)
)

func TestDiscard_success(t *testing.T) {
	test := func(mode string) func(t *testing.T) {
		return func(t *testing.T) {
			assert := assert.New(t)
			server, log, cleanup := setup(t)
			defer cleanup()
			switch mode {
			case "ok":
			case "redirect":
				server.startClock()
				server.next.redirect = 3
			case "unknown":
				server.startClock()
				server.next.unknown = true
			}
			err := log.Discard(context.Background(), 3)
			require.NoError(t, err)
			assert.Equal(uint64(3), server.startIndex)
			assert.Equal(0, server.next.redirect)
			assert.False(server.next.unknown)
		}
	}
	t.Run("ok", test("ok"))
	t.Run("redirect", test("redirect"))
	t.Run("unknown", test("unknown"))
}

func TestDiscard_expired(t *testing.T) {
	assert := assert.New(t)
	server, log, cleanup := setup(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := log.Discard(ctx, 3)
	assert.Equal(ctx.Err(), err)
	assert.Equal(uint64(1), server.startIndex)
}

func TestDiscard_closed(t *testing.T) {
	assert := assert.New(t)
	server, log, cleanup := setup(t)
	cleanup()
	err := log.Discard(context.Background(), 3)
	assert.True(blog.IsClosedError(err))
	assert.Equal(uint64(1), server.startIndex)
}

func TestRead_success(t *testing.T) {
	test := func(mode string) func(t *testing.T) {
		return func(t *testing.T) {
			assert := assert.New(t)
			server, log, cleanup := setup(t)
			defer cleanup()
			server.append([][]byte{[]byte("hello"), []byte("world")})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entriesCh := make(chan []blog.Entry)
			switch mode {
			case "ok":
			case "redirect":
				server.startClock()
				server.next.redirect = 3
			case "unknown":
				server.startClock()
				server.next.unknown = true
			}
			var allEntries []blog.Entry
			wait := parallel.Go(func() {
				for entries := range entriesCh {
					allEntries = append(allEntries, entries...)
					if len(allEntries) >= 2 {
						cancel()
					}
				}
			})
			err := log.Read(ctx, 1, entriesCh)
			wait()
			assert.Error(ctx.Err())
			assert.Equal(ctx.Err(), err)
			if assert.Len(allEntries, 2) {
				assert.Equal(blog.Index(1), allEntries[0].Index)
				assert.Equal("hello", string(allEntries[0].Data))
				assert.False(allEntries[0].Skip)
				assert.Equal(blog.Index(2), allEntries[1].Index)
				assert.Equal("world", string(allEntries[1].Data))
				assert.False(allEntries[1].Skip)
			}
			assert.Equal(0, server.next.redirect)
			assert.False(server.next.unknown)
		}
	}
	t.Run("ok", test("ok"))
	t.Run("redirect", test("redirect"))
	t.Run("unknown", test("unknown"))
}

func TestRead_truncated(t *testing.T) {
	assert := assert.New(t)
	server, log, cleanup := setup(t)
	defer cleanup()
	server.append([][]byte{[]byte("hello"), []byte("world")})
	entriesCh := make(chan []blog.Entry, 4)
	err := log.Read(context.Background(), 0, entriesCh)
	assert.True(blog.IsTruncatedError(err))
}

func TestRead_expired(t *testing.T) {
	assert := assert.New(t)
	server, log, cleanup := setup(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	server.append([][]byte{[]byte("hello"), []byte("world")})
	entriesCh := make(chan []blog.Entry, 4)
	err := log.Read(ctx, 1, entriesCh)
	assert.Equal(ctx.Err(), err)
}

func TestRead_closed(t *testing.T) {
	assert := assert.New(t)
	server, log, cleanup := setup(t)
	server.append([][]byte{[]byte("hello"), []byte("world")})
	cleanup()
	entriesCh := make(chan []blog.Entry, 4)
	err := log.Read(context.Background(), 1, entriesCh)
	assert.True(blog.IsClosedError(err))
}

func TestRead_closedMidstream(t *testing.T) {
	assert := assert.New(t)
	server, log, cleanup := setup(t)
	defer cleanup()
	server.append([][]byte{[]byte("hello"), []byte("world")})
	var allEntries []blog.Entry
	entriesCh := make(chan []blog.Entry)
	wait := parallel.Go(func() {
		for entries := range entriesCh {
			allEntries = append(allEntries, entries...)
			if len(allEntries) >= 2 {
				cleanup()
			}
		}
	})
	err := log.Read(context.Background(), 1, entriesCh)
	wait()
	assert.True(blog.IsClosedError(err))
}

// TestRead_panicsOnBadIndex checks that Read() will panic in the event the
// Log service sent us data with an unexpected log index.
func TestRead_panicsOnBadIndex(t *testing.T) {
	type testcase struct {
		name     string
		entries  [][]*logspec.Entry
		expPanic string
	}
	testcases := []testcase{{
		name: "rewind",
		entries: [][]*logspec.Entry{{
			{Index: 1},
			{Index: 2},
			{Index: 3},
			{Index: 2},
		}},
		expPanic: `
received invalid log index from Log service server: tcp://{server}
expected log index 4, but got 2
log store read started at log index 1
ReadReply from Log service has entries:
	idx=1 skip=false data=[]
	idx=2 skip=false data=[]
	idx=3 skip=false data=[]
	idx=2 skip=false data=[]
`,
	}, {
		name: "hole",
		entries: [][]*logspec.Entry{{
			{Index: 1},
			{Index: 2},
			{Index: 4},
		}},
		expPanic: `
received invalid log index from Log service server: tcp://{server}
expected log index 3, but got 4
log store read started at log index 1
ReadReply from Log service has entries:
	idx=1 skip=false data=[]
	idx=2 skip=false data=[]
	idx=4 skip=false data=[]
`,
	}, {
		name: "repeat",
		entries: [][]*logspec.Entry{{
			{Index: 1},
			{Index: 2},
			{Index: 2},
		}},
		expPanic: `
received invalid log index from Log service server: tcp://{server}
expected log index 3, but got 2
log store read started at log index 1
ReadReply from Log service has entries:
	idx=1 skip=false data=[]
	idx=2 skip=false data=[]
	idx=2 skip=false data=[]
`,
	}, {
		name: "rewind_2_msgs",
		entries: [][]*logspec.Entry{{
			{Index: 1},
			{Index: 2},
			{Index: 3},
		}, {
			{Index: 2},
		}},
		expPanic: `
received invalid log index from Log service server: tcp://{server}
expected log index 4, but got 2
log store read started at log index 1
ReadReply from Log service has entries:
	idx=2 skip=false data=[]
`,
	}, {
		name: "hole_2_msgs",
		entries: [][]*logspec.Entry{{
			{Index: 1},
			{Index: 2},
		}, {
			{Index: 4},
			{Index: 5},
		}},
		expPanic: `
received invalid log index from Log service server: tcp://{server}
expected log index 3, but got 4
log store read started at log index 1
ReadReply from Log service has entries:
	idx=4 skip=false data=[]
	idx=5 skip=false data=[]
`,
	}, {
		name: "repeat_2_msg",
		entries: [][]*logspec.Entry{{
			{Index: 1},
			{Index: 2},
		}, {
			{Index: 2},
			{Index: 3},
		}},
		expPanic: `
received invalid log index from Log service server: tcp://{server}
expected log index 3, but got 2
log store read started at log index 1
ReadReply from Log service has entries:
	idx=2 skip=false data=[]
	idx=3 skip=false data=[]
`}}
	for _, test := range testcases {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			server, log, cleanup := setup(t)
			defer cleanup()
			server.append([][]byte{[]byte("hello")})
			server.read = func(_ *mockServer, stream logspec.Log_ReadServer, idx uint64) error {
				for _, entries := range test.entries {
					stream.Send(&logspec.ReadReply{
						Reply: &logspec.ReadReply_Ok{
							Ok: &logspec.ReadReply_OK{
								Entries: entries,
							},
						},
					})
				}
				return nil
			}
			resCh := make(chan []logspec.Entry, 10)
			expPanic := strings.Replace(test.expPanic,
				"{server}", server.listener.Addr().String(), -1)
			assert.PanicsWithValue(t, strings.TrimPrefix(expPanic, "\n"), func() {
				log.Read(ctx, 1, resCh)
			})
			// Verify that anything that made it onto resCh is valid.
			nextIdx := uint64(1)
			for c := range resCh {
				for _, e := range c {
					assert.Equal(t, nextIdx, e.Index)
					nextIdx++
				}
			}
		})
	}
}

func TestInfo_success(t *testing.T) {
	test := func(mode string) func(t *testing.T) {
		return func(t *testing.T) {
			assert := assert.New(t)
			server, log, cleanup := setup(t)
			defer cleanup()
			switch mode {
			case "ok":
			case "redirect":
				server.startClock()
				server.next.redirect = 3
			case "unknown":
				server.startClock()
				server.next.unknown = true
			}
			info, err := log.Info(context.Background())
			require.NoError(t, err)
			assert.Equal(uint64(1), info.FirstIndex)
			assert.Equal(0, server.next.redirect)
			assert.False(server.next.unknown)
		}
	}
	t.Run("ok", test("ok"))
	t.Run("redirect", test("redirect"))
	t.Run("unknown", test("unknown"))
}

func TestInfo_expired(t *testing.T) {
	assert := assert.New(t)
	_, log, cleanup := setup(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := log.Info(ctx)
	assert.Equal(ctx.Err(), err)
}

func TestInfo_closed(t *testing.T) {
	assert := assert.New(t)
	_, log, cleanup := setup(t)
	cleanup()
	_, err := log.Info(context.Background())
	assert.True(blog.IsClosedError(err))
}

func TestInfoStream_success(t *testing.T) {
	test := func(mode string) func(t *testing.T) {
		return func(t *testing.T) {
			assert := assert.New(t)
			server, log, cleanup := setup(t)
			defer cleanup()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			infoCh := make(chan *blog.Info)
			switch mode {
			case "ok":
			case "redirect":
				server.startClock()
				server.next.redirect = 3
			case "unknown":
				server.startClock()
				server.next.unknown = true
			}
			var infos []*blog.Info
			wait := parallel.Go(func() {
				for info := range infoCh {
					infos = append(infos, info)
					if len(infos) >= 3 {
						cancel()
					}
					server.append([][]byte{[]byte("hi")})
				}
			})
			err := log.InfoStream(ctx, infoCh)
			wait()
			assert.Error(ctx.Err())
			assert.Equal(ctx.Err(), err)
			if assert.Len(infos, 3) {
				assert.Equal(blog.Index(0), infos[0].LastIndex)
				assert.Equal(blog.Index(1), infos[1].LastIndex)
				assert.Equal(blog.Index(2), infos[2].LastIndex)
			}
			assert.Equal(0, server.next.redirect)
			assert.False(server.next.unknown)
		}
	}
	t.Run("ok", test("ok"))
	t.Run("redirect", test("redirect"))
	t.Run("unknown", test("unknown"))
}

func TestInfoStream_expired(t *testing.T) {
	assert := assert.New(t)
	_, log, cleanup := setup(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	infoCh := make(chan *blog.Info, 4)
	err := log.InfoStream(ctx, infoCh)
	assert.Equal(ctx.Err(), err)
}

func TestInfoStream_closed(t *testing.T) {
	assert := assert.New(t)
	_, log, cleanup := setup(t)
	cleanup()
	infoCh := make(chan *blog.Info, 4)
	err := log.InfoStream(context.Background(), infoCh)
	assert.True(blog.IsClosedError(err))
}

func TestInfoStream_closedMidstream(t *testing.T) {
	assert := assert.New(t)
	_, log, cleanup := setup(t)
	defer cleanup()
	infoCh := make(chan *blog.Info)
	wait := parallel.Go(func() {
		for range infoCh {
			cleanup()
		}
	})
	err := log.InfoStream(context.Background(), infoCh)
	wait()
	assert.True(blog.IsClosedError(err))
}

// Tests that connectAnyLocked selects servers randomly and assigns endpoint
// strings back to the clientConn.
func Test_connectAnyLocked_random(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // clean up any background dialers left over
	endpoints := []*discovery.Endpoint{
		{Network: "unknown", Host: "example.com", Port: "80"},
		{Network: "unknown", Host: "example.com", Port: "81"},
		{Network: "unknown", Host: "example.com", Port: "82"},
		{Network: "unknown", Host: "example.com", Port: "83"},
		{Network: "unknown", Host: "example.com", Port: "84"},
	}
	counts := make(map[string]int, len(endpoints))
	for _, endpoint := range endpoints {
		counts[endpoint.String()] = 0
	}
	zeros := len(endpoints)
	for i := 0; i < 100; i++ {
		log := &Log{ctx: ctx, logger: logrus.NewEntry(logrus.New())}
		address := connectAddress(ctx, t, log, endpoints)
		count, found := counts[address]
		if !found {
			assert.FailNow(t, "Unexpected address",
				"address: %v", address)
		}
		counts[address]++
		if count == 0 {
			zeros--
			if zeros == 0 {
				return
			}
		}
	}
	assert.Fail(t, "connectAnyLocked isn't very random",
		"counts %v", counts)
}

func Test_connectAnyLocked_ignoresLastEndpoint(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // clean up any background dialers left over
	endpoints := []*discovery.Endpoint{
		{Network: "unknown", Host: "example.com", Port: "80"},
		{Network: "unknown", Host: "example.com", Port: "81"},
		{Network: "unknown", Host: "example.com", Port: "82"},
		{Network: "unknown", Host: "example.com", Port: "83"},
		{Network: "unknown", Host: "example.com", Port: "84"},
	}
	counts := make(map[string]int, len(endpoints))
	for _, endpoint := range endpoints {
		counts[endpoint.String()] = 0
	}
	log := &Log{ctx: ctx, logger: logrus.NewEntry(logrus.New())}
	prev := ""
	for i := 0; i < 100; i++ {
		address := connectAddress(ctx, t, log, endpoints)
		_, found := counts[address]
		if !found {
			assert.FailNow(t, "Unexpected address",
				"address: %v", address)
		}
		counts[address]++
		if address == prev {
			assert.FailNow(t, "connectAnyLocked shouldn't have selected last used endpoint")
		}
		prev = address
	}
	for endpoint, count := range counts {
		assert.True(t, count > 0, "endpoint %s was never used", endpoint)
	}
}

func Test_connectAnyLocked_HandleDuplicatedEndpoints(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // clean up any background dialers left over
	endpoints := []*discovery.Endpoint{
		{Network: "unknown", Host: "example.com", Port: "80"},
		{Network: "unknown", Host: "example.com", Port: "80"},
	}
	log := &Log{ctx: ctx, logger: logrus.NewEntry(logrus.New())}
	log.locked.lastEndpoint = endpoints[1].String()
	// endpoints has 2 entries that are the same, and are the failed endpoint
	// this is a broken configuration, but shouldn't cause connectAnyLocked to barf.
	address := connectAddress(ctx, t, log, endpoints)
	assert.Equal(t, endpoints[0].String(), address)
}

// Used in Test_connectAnyLocked_* and Test_disconnectFrom_flagsEndpointAsFailed
func connectAddress(ctx context.Context, t *testing.T, log *Log, endpoints []*discovery.Endpoint) string {
	assert := assert.New(t)
	log.servers = discovery.NewStaticLocator(endpoints)
	log.lock.Lock()
	log.connectAnyLocked()
	client := log.locked.client
	log.lock.Unlock()
	// This loop waits until client.conn has failed (the dialer for the "unknown"
	// Network won't succeed).
	i := 0
	for {
		state := client.conn.GetState()
		if state == connectivity.TransientFailure {
			break
		}
		i++
		if i == 100 { // give up
			assert.Equal(connectivity.TransientFailure.String(), state.String())
			break
		}
		ctx, cancel := context.WithTimeout(ctx, time.Millisecond)
		client.conn.WaitForStateChange(ctx, state)
		cancel()
	}
	// Since the net.Dialer failed, the address must be set already.
	return client.server()
}
