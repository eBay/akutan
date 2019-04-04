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
	"fmt"
	"io"
	"strings"

	"github.com/ebay/beam/partitioning"
	"github.com/ebay/beam/rpc"
	grpcclientutil "github.com/ebay/beam/util/grpc/client"
)

type singleSource struct {
	opts *options
	stub rpc.CarouselClient
}

func newSingleSource(o *options) (carouselSource, error) {
	if !strings.Contains(o.host, ":") {
		o.host += ":9980"
	}
	s := singleSource{opts: o}
	conn := grpcclientutil.InsecureDialContext(context.TODO(), o.host)
	s.stub = rpc.NewCarouselClient(conn)
	return &s, nil
}

func (s *singleSource) status() error {
	res, err := s.stub.LogStatus(context.Background(), &rpc.LogStatusRequest{})
	if err != nil {
		return err
	}
	fmtr.Printf("Next Position:   %v\n", res.NextPosition)
	fmtr.Printf("Replay Position: %v\n", res.ReplayPosition)
	return nil
}

func (s *singleSource) ride(partition, numPartitions int) error {

	status, err := s.stub.LogStatus(context.Background(), &rpc.LogStatusRequest{})
	if err != nil {
		return fmt.Errorf("unable to call LogStatus: %v", err)
	}

	req := rpc.CarouselRequest{
		Dedicated:     s.opts.dedicated,
		MaxIndex:      status.ReplayPosition.Index - 1,
		HashPartition: partitioning.CarouselHashPartition(partitioning.NewHashSubjectPredicatePartition(partition, numPartitions)),
	}
	if !s.opts.quiet {
		fmt.Printf("Ride request: %#v\n", req)
	}
	sink, err := newRideSink(s.opts)
	if err != nil {
		return err
	}
	defer sink.close()
	res, err := s.stub.Ride(context.Background(), &req)
	if err != nil {
		return fmt.Errorf("error calling Ride: %v", err)
	}
	for {
		next, err := res.Recv()
		if err == io.EOF {
			sink.printResults()
			return nil
		}
		if err != nil {
			return fmt.Errorf("error during ride: %v", err)
		}
		sink.add(next)
	}
}
