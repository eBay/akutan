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

// Command grpcbench is a small benchmark tool for gRPC.
package main

import (
	"context"
	"io"
	"net"
	"time"

	docopt "github.com/docopt/docopt-go"
	"github.com/ebay/akutan/util/debuglog"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const usage = `grpcbench is a small benchmark for gRPC.

Usage:
  grpcbench client [-t=DUR] SERVERADDR
  grpcbench server SERVERADDR

Options:
  -t=DUR, --time=DUR   Time to run each test [default: 1s]
`

var options struct {
	Client     bool
	Server     bool
	ServerAddr string `docopt:"SERVERADDR"`
	Time       time.Duration
	TimeString string `docopt:"--time"`
}

const maxMsgSize = 1024 * 1024 * 1024

func main() {
	debuglog.Configure(debuglog.Options{})
	opts, err := docopt.ParseDoc(usage)
	if err != nil {
		log.Fatalf("Error parsing command-line arguments: %v", err)
	}
	err = opts.Bind(&options)
	if err != nil {
		log.Fatalf("Error binding command-line arguments: %v\nfrom: %+v", err, opts)
	}
	if options.TimeString != "" {
		options.Time, err = time.ParseDuration(options.TimeString)
		if err != nil {
			log.Fatalf("Unable to parse time value: %v", err)
		}
	}
	log.Infof("Options: %+v", options)

	switch {
	case options.Server:
		runServer()
	case options.Client:
		runClient()
	}
}

func runServer() {
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize))

	listener, err := net.Listen("tcp", options.ServerAddr)
	if err != nil {
		log.Fatalf("Could not listen at %v: %v", options.ServerAddr, err)
	}
	RegisterBenchServer(server, &benchServerImpl{})
	server.Serve(listener)
}

type benchServerImpl struct {
}

func (server *benchServerImpl) Single(ctx context.Context, req *SingleRequest) (*SingleResult, error) {
	data := make([]byte, req.RespSize)
	fill(data)
	return &SingleResult{
		Data: data,
	}, nil
}

func (server *benchServerImpl) Streaming(req *StreamRequest, out Bench_StreamingServer) error {
	data := make([]byte, req.RespSize*req.Count)
	fill(data)
	for i := uint64(0); i < req.Count; i++ {
		err := out.Send(&StreamResult{Data: data[i*req.RespSize : (i+1)*req.RespSize]})
		if err != nil {
			return err
		}
	}
	return nil
}

func fill(data []byte) {
	for i := 0; i+4 < len(data); i += 4 {
		data[i+0] = byte(i)
		data[i+1] = byte(i)
		data[i+2] = byte(i)
		data[i+3] = byte(i)
	}
}

func runClient() {
	conn, err := grpc.Dial(options.ServerAddr,
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*1024)),
	)
	if err != nil {
		log.Fatalf("Could not connect to server at %v: %v", options.ServerAddr, err)
	}
	client := NewBenchClient(conn)

	sizes := []uint64{1, 256, 512, 1024, 1024 * 10, 1024 * 100,
		1024 * 1024, 1024 * 1024 * 10, 1024 * 1024 * 100}
	counts := []uint64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000, 10000, 100000}
	for _, size := range sizes {
		for i := 0; i < 3; i++ {
			benchSingle(client, size)
		}
	}
	for _, size := range sizes {
		for _, count := range counts {
			if size*count > 1e8 {
				continue
			}
			for i := 0; i < 3; i++ {
				benchStream(client, size, count)
			}
		}
	}
}

func benchSingle(client BenchClient, respSize uint64) {
	start := time.Now()
	var elapsed time.Duration
	i := 0
	for elapsed < options.Time {
		i++
		_, err := client.Single(context.Background(),
			&SingleRequest{RespSize: respSize})
		if err != nil {
			log.Fatalf("Could not call Single RPC: %v", err)
		}
		elapsed = time.Since(start)
	}
	mib := float64(respSize) / 1024 / 1024
	log.Printf("Single % 10dB             = % 12.1fus RTT  %7.1fMiB/s  (% 6v iter)",
		respSize,
		float64(elapsed.Nanoseconds())/1000/float64(i),
		mib/(float64(elapsed.Nanoseconds())/1e9/float64(i)),
		i)
}

func benchStream(client BenchClient, respSize uint64, count uint64) {
	start := time.Now()
	var elapsed time.Duration
	i := 0
	for elapsed < options.Time {
		i++
		resp, err := client.Streaming(context.Background(),
			&StreamRequest{RespSize: respSize, Count: count})
		if err != nil {
			log.Fatalf("Could not call Streaming RPC: %v", err)
		}
		for {
			_, err := resp.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("Could not receive Streaming RPC: %v", err)
			}
		}
		elapsed = time.Since(start)
	}
	mib := float64(respSize) * float64(count) / 1024 / 1024
	log.Printf("Stream % 10dB x % 8d  = % 12.1fus RTT  %7.1fMiB/s  (% 6v iter)",
		respSize, count,
		float64(elapsed.Nanoseconds())/1000/float64(i),
		mib/(float64(elapsed.Nanoseconds())/1e9/float64(i)),
		i)
}
