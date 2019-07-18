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
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/cheggaaa/pb"
	"github.com/ebay/akutan/logspec"
	"github.com/ebay/akutan/util/cmp"
	grpcclientutil "github.com/ebay/akutan/util/grpc/client"
)

func diffLog(ctx context.Context, options *options, w io.Writer) error {
	logServers := make([]logServer, len(options.Servers))
	for i, svr := range options.Servers {
		logServers[i].init(ctx, svr)
	}

	infoResp, err := logServers[0].client.Info(ctx, &logspec.InfoRequest{})
	if err != nil {
		return fmt.Errorf("error fetching log info: %s", err)
	}
	infoRes := infoResp.GetOk()
	fmt.Fprintf(w, "Log Info: start:%d end:%d\n", infoRes.FirstIndex, infoRes.LastIndex)
	startIdx := cmp.MaxUint64(options.StartIndex, infoRes.FirstIndex)
	endIdx := infoRes.LastIndex
	if options.EndIndex != 0 {
		endIdx = cmp.MinUint64(endIdx, options.EndIndex)
	}
	err = verifyEqual(options, logServers, startIdx, endIdx, w)
	if err != nil {
		return err
	}
	fmt.Fprintln(w, "No diffs found")
	return nil
}

func verifyEqual(options *options, servers []logServer, startIdx, endIdx uint64, w io.Writer) error {
	pbs := make([]*pb.ProgressBar, len(servers)+1)
	for i := range servers {
		pbs[i] = pb.New(int(endIdx - startIdx)).Prefix(fmt.Sprintf("Log %d range %d-%d ", i, startIdx, endIdx))
		pbs[i].SetMaxWidth(100)
		pbs[i].ShowCounters = startIdx == 1
		pbs[i].ShowPercent = true
	}
	pbs[len(servers)] = pb.New(0).Prefix("Errors")
	pbs[len(servers)].SetMaxWidth(100)
	pool, _ := pb.StartPool(pbs...)
	defer pool.Stop()

	wg := sync.WaitGroup{}
	stopCh := make(chan struct{})
	errCh := make(chan error, len(servers))
	entriesCh := make([]chan logspec.Entry, len(servers))
	for i := range entriesCh {
		entriesCh[i] = make(chan logspec.Entry, 1024)
	}
	for i := range servers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := servers[i].read(pbs[i], startIdx, endIdx, entriesCh[i], stopCh)
			if err != nil {
				errCh <- err
			}
		}(i)
	}
	errWg := sync.WaitGroup{}
	errors := make([]error, 0, len(servers))
	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for e := range errCh {
			pbs[len(servers)].Increment()
			errors = append(errors, e)
			if len(errors) >= options.MaxErrors {
				close(stopCh)
			}
		}
	}()

	currentIdx := startIdx
	entries := make([]logspec.Entry, len(servers))
	for currentIdx <= endIdx {
		open := true
		for i, ch := range entriesCh {
			var chOpen bool
			entries[i], chOpen = <-ch
			open = open && chOpen
		}
		if !open {
			close(stopCh)
			break
		}
		for i, e := range entries {
			if currentIdx != e.Index {
				errCh <- fmt.Errorf("%v has unexpected index, expecting %d got %d",
					servers[i], currentIdx, e.Index)
			}
			if i == 0 {
				continue
			}
			if entries[0].Skip != e.Skip {
				errCh <- fmt.Errorf("%v and %v have different skip values for index %d: %v / %v",
					servers[0], servers[i], currentIdx, entries[0].Skip, e.Skip)
			}
			if !bytes.Equal(entries[0].Data, e.Data) {
				err := strings.Builder{}
				fmt.Fprintf(&err, "%v and %v have different values at index %d\n",
					servers[0], servers[i], currentIdx)
				for j := range entries {
					fmt.Fprintf(&err, "\t%v\n", entries[j].Data)
				}
				errCh <- fmt.Errorf("%s", err.String())
			}
		}
		currentIdx++
	}
	wg.Wait()
	close(errCh)
	errWg.Wait()
	pool.Stop()
	for _, err := range errors {
		fmt.Fprintln(w, err.Error())
	}
	if len(errors) > 0 {
		return errors[0]
	}
	return nil
}

type logServer struct {
	addr   string
	client logspec.LogClient
}

func (l logServer) String() string {
	return l.addr
}

func (l *logServer) init(ctx context.Context, addr string) {
	l.addr = addr
	conn := grpcclientutil.InsecureDialContext(ctx, addr)
	l.client = logspec.NewLogClient(conn)
}

type incrementer interface {
	Increment() int
}

func (l *logServer) read(pbar incrementer, startIdx, endIdx uint64, entriesCh chan<- logspec.Entry, stopCh <-chan struct{}) error {
	defer close(entriesCh)
	req := logspec.ReadRequest{
		NextIndex: startIdx,
	}
	read, err := l.client.Read(context.Background(), &req)
	if err != nil {
		return fmt.Errorf("%s: error with Read() %s", l, err)
	}

	currentIdx := req.NextIndex
	for {
		reply, err := read.Recv()
		if io.EOF == err {
			return nil // all done
		} else if err != nil {
			return fmt.Errorf("%s: error from read.Recv() %v", l, err)
		}
		ok, isOk := reply.Reply.(*logspec.ReadReply_Ok)
		if !isOk {
			return fmt.Errorf("%v: received unexpected reply of %T %v", l, reply.Reply, reply.Reply)
		}
		for _, e := range ok.Ok.GetEntries() {
			if e.Index != currentIdx {
				return fmt.Errorf("%v: expecting log index %d, but got %d", l, currentIdx, e.Index)
			}
			select {
			case <-stopCh:
				return nil
			case entriesCh <- *e:
			}
			pbar.Increment()
			currentIdx++
			if e.Index == endIdx {
				return nil
			}
		}
		select {
		case <-stopCh:
			return nil
		default:
		}
	}
}
