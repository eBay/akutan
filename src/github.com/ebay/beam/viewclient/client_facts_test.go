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

package viewclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"testing"

	"github.com/ebay/beam/blog"
	"github.com/ebay/beam/partitioning"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/space"
	"github.com/ebay/beam/util/random"
	"github.com/ebay/beam/viewclient/fanout"
	"github.com/ebay/beam/viewclient/viewreg"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// duplicate is a special value used to indicate that a fact should be filtered
// from the result set.
const duplicate = 0xdead

// Fact is a short alias for api.ResolvedFact.
type Fact = rpc.Fact

// mockStreamBase implements grpc.ClientStream by panicking.
type mockStreamBase struct{}

func (stream *mockStreamBase) Header() (metadata.MD, error) {
	panic("mockStreamBase.Header should not be called")
}

func (stream *mockStreamBase) Trailer() metadata.MD {
	panic("mockStreamBase.Trailer should not be called")
}

func (stream *mockStreamBase) CloseSend() error {
	panic("mockStreamBase.CloseSend should not be called")
}

func (stream *mockStreamBase) Context() context.Context {
	panic("mockStreamBase.Context should not be called")
}

func (stream *mockStreamBase) SendMsg(m interface{}) error {
	panic("mockStreamBase.SendMsg should not be called")
}

func (stream *mockStreamBase) RecvMsg(m interface{}) error {
	panic("mockStreamBase.RecvMsg should not be called")
}

// mockLookupChunkStream implements the identical interfaces named
// rpc.ReadFactsSP_LookupSClient, rpc.ReadFactsPO_LookupPOClient, etc.
type mockLookupChunkStream struct {
	mockStreamBase
	// FIFO queue of chunks to return from Recv().
	chunks []*rpc.LookupChunk
	// Once chunks is empty, error to return from Recv().
	err error
}

func (stream *mockLookupChunkStream) Recv() (*rpc.LookupChunk, error) {
	if len(stream.chunks) > 0 {
		chunk := stream.chunks[0]
		stream.chunks = stream.chunks[1:]
		return chunk, nil
	}
	var err error
	if stream.err == nil {
		err = io.EOF
	} else {
		err = stream.err
	}
	stream.err = errors.New("unexpected call to recv")
	return nil, err
}

// mockSPViewStub implements rpc.ReadFactsSPClient.
type mockSPViewStub struct {
	view      *ReadFactsSPClient
	lookupS   func(view *ReadFactsSPClient, req *rpc.LookupSRequest) *mockLookupChunkStream
	lookupSP  func(view *ReadFactsSPClient, req *rpc.LookupSPRequest) *mockLookupChunkStream
	lookupSPO func(view *ReadFactsSPClient, req *rpc.LookupSPORequest) *mockLookupChunkStream
}

func (stub *mockSPViewStub) LookupS(
	ctx context.Context, in *rpc.LookupSRequest, _ ...grpc.CallOption,
) (rpc.ReadFactsSP_LookupSClient, error) {
	return stub.lookupS(stub.view, in), nil
}

func (stub *mockSPViewStub) LookupSP(
	ctx context.Context, in *rpc.LookupSPRequest, _ ...grpc.CallOption,
) (rpc.ReadFactsSP_LookupSPClient, error) {
	return stub.lookupSP(stub.view, in), nil
}

func (stub *mockSPViewStub) LookupSPO(
	ctx context.Context, in *rpc.LookupSPORequest, opts ...grpc.CallOption,
) (rpc.ReadFactsSP_LookupSPOClient, error) {
	return stub.lookupSPO(stub.view, in), nil
}

func (stub *mockSPViewStub) FactStats(
	ctx context.Context, in *rpc.FactStatsRequest, opts ...grpc.CallOption,
) (*rpc.FactStatsResult, error) {
	return nil, nil
}

// mockPOViewStub implements rpc.ReadFactsPOClient.
type mockPOViewStub struct {
	view        *ReadFactsPOClient
	lookupPO    func(view *ReadFactsPOClient, req *rpc.LookupPORequest) *mockLookupChunkStream
	lookupPOCmp func(view *ReadFactsPOClient, req *rpc.LookupPOCmpRequest) *mockLookupChunkStream
}

func (stub *mockPOViewStub) LookupPO(
	ctx context.Context, in *rpc.LookupPORequest, _ ...grpc.CallOption,
) (rpc.ReadFactsPO_LookupPOClient, error) {
	return stub.lookupPO(stub.view, in), nil
}

func (stub *mockPOViewStub) LookupPOCmp(
	ctx context.Context, in *rpc.LookupPOCmpRequest, _ ...grpc.CallOption,
) (rpc.ReadFactsPO_LookupPOCmpClient, error) {
	return stub.lookupPOCmp(stub.view, in), nil
}

func (stub *mockPOViewStub) FactStats(
	ctx context.Context, in *rpc.FactStatsRequest, opts ...grpc.CallOption,
) (*rpc.FactStatsResult, error) {
	return nil, nil
}

// mockSPView constructs a new mockable spView.
func mockSPView(partition, numPartitions int) (*ReadFactsSPClient, *mockSPViewStub) {
	stub := &mockSPViewStub{}
	view := &ReadFactsSPClient{
		View: viewreg.View{
			Partition: partitioning.NewHashSubjectPredicatePartition(partition, numPartitions),
		},
		Stub: stub,
	}
	stub.view = view
	return view, stub
}

// mockPOView constructs a new mockable poView.
func mockPOView(partition, numPartitions int) (*ReadFactsPOClient, *mockPOViewStub) {
	stub := &mockPOViewStub{}
	view := &ReadFactsPOClient{
		View: viewreg.View{
			Partition: partitioning.NewHashPredicateObjectPartition(partition, numPartitions),
		},
		Stub: stub,
	}
	stub.view = view
	return view, stub
}

func init() {
	random.SeedMath()
}

// fillSPData randomly generates subject-predicate pairs that are hosted by the
// given view. For each item in data, a non-zero subject or predicate will be
// retained (if both are set but now owned by view, the behavior is undefined).
func fillSPData(view *ReadFactsSPClient, data []rpc.LookupSPRequest_Item) {
	for i := range data {
		for {
			s := data[i].Subject
			p := data[i].Predicate
			if s == 0 {
				s = rand.Uint64()
			}
			if p == 0 {
				p = rand.Uint64()
			}
			if view.View.Partition.HashRange().Contains(partitioning.HashSP(s, p)) {
				data[i].Subject = s
				data[i].Predicate = p
				break
			}
		}
	}
}

// fillPOData randomly generates predicate-object pairs that are hosted by the
// given view. For each item in data, a non-zero predicate will be retained and
// a string object will be retained with randomly appended printable garbage.
func fillPOData(view *ReadFactsPOClient, data []rpc.LookupPORequest_Item) {
	objects := make(map[string]struct{})
	for i := range data {
		if data[i].Predicate == 0 {
			data[i].Predicate = rand.Uint64()
		}
		var s string
		v, ok := data[i].Object.ValString(), data[i].Object.IsType(rpc.KtString)
		if ok {
			s = v
		}
		for {
			s += string('a' + rand.Intn(26))
			data[i].Object = rpc.AString(s, 0)
			_, found := objects[s]
			if found {
				continue
			}
			if !view.View.Partition.HashRange().Contains(partitioning.HashPO(data[i].Predicate, data[i].Object)) {
				continue
			}
			objects[s] = struct{}{}
			break
		}
	}
}

// chunksStr drains the given channel and returns its contents as a sorted
// slice of human-readable strings.
func chunksStr(ch <-chan *rpc.LookupChunk) []string {
	var lines []string
	for chunk := range ch {
		for _, fact := range chunk.Facts {
			lines = append(lines, fmt.Sprintf(
				"Lookup %d: %v %v %v %v @ %v",
				fact.Lookup,
				fact.Fact.Id,
				fact.Fact.Subject,
				fact.Fact.Predicate,
				fact.Fact.Object,
				fact.Fact.Index))
		}
	}
	sort.Strings(lines)
	return lines
}

func Test_lookupS_2views(t *testing.T) {
	assert := assert.New(t)
	req := &rpc.LookupSRequest{
		Index: 500,
	}
	view1, stub1 := mockSPView(0, 2)
	view2, stub2 := mockSPView(1, 2)
	spData := []rpc.LookupSPRequest_Item{
		{Predicate: 3},
		{Predicate: 5},
	}
	fillSPData(view1, spData[:1])
	fillSPData(view2, spData[1:])
	for _, sp := range spData {
		req.Subjects = append(req.Subjects, sp.Subject)
	}

	// map from subject to Facts
	replies := map[uint64][]Fact{
		req.Subjects[0]: []Fact{
			{Id: 20, Predicate: 3, Object: rpc.AString("Alabama", 0), Index: 10},
			{Id: 40, Predicate: 3, Object: rpc.AString("Alaska", 0), Index: 60},
			{Id: 40, Predicate: 3, Object: rpc.AString("Alaska", 0), Index: 60, Subject: duplicate},
			{Id: 40, Predicate: 3, Object: rpc.AString("AAA", 0), Index: 50, Subject: duplicate},
			{Id: 90, Predicate: 5, Object: rpc.AString("Arkansas", 0), Index: 30},
		},
		req.Subjects[1]: []Fact{
			{Id: 91, Predicate: 3, Object: rpc.AString("Connecticut", 0), Index: 31},
			{Id: 51, Predicate: 5, Object: rpc.AString("California", 0), Index: 21},
		},
	}

	var expect []string
	for i, lookup := range req.Subjects {
		for _, fact := range replies[lookup] {
			if fact.Subject != duplicate {
				expect = append(expect, fmt.Sprintf(
					"Lookup %d: %v %v %v %v @ %v",
					i,
					fact.Id,
					lookup,
					fact.Predicate,
					fact.Object,
					fact.Index))
			}
		}
	}
	sort.Strings(expect)

	server := func(view *ReadFactsSPClient, req *rpc.LookupSRequest) *mockLookupChunkStream {
		assert.Equal(blog.Index(500), req.Index)
		chunk := new(rpc.LookupChunk)
		for offset, lookup := range req.Subjects {
			for _, fact := range replies[lookup] {
				hash := partitioning.HashSP(lookup, fact.Predicate)
				if view.Serves().Contains(space.Hash64(hash)) {
					chunk.Facts = append(chunk.Facts, rpc.LookupChunk_Fact{
						Lookup: uint32(offset),
						Fact:   fact,
					})
				}
			}
		}
		stream := new(mockLookupChunkStream)
		stream.chunks = []*rpc.LookupChunk{chunk}
		return stream
	}
	stub1.lookupS = server
	stub2.lookupS = server
	views := fanout.NewViews([]fanout.View{view1, view2})
	resCh := make(chan *rpc.LookupChunk, 4)
	err := lookupS(context.Background(), views, req, resCh)
	assert.NoError(err)
	assert.Equal(expect, chunksStr(resCh))
}

// It's easier to test that the request offset is part of the cursor filter in
// this test.
func Test_lookupS_1view(t *testing.T) {
	assert := assert.New(t)
	req := &rpc.LookupSRequest{
		Subjects: []uint64{
			800,
			600,
		},
	}
	view, stub := mockSPView(0, 1)
	stub.lookupS = func(view *ReadFactsSPClient, req *rpc.LookupSRequest) *mockLookupChunkStream {
		return &mockLookupChunkStream{chunks: []*rpc.LookupChunk{{Facts: []rpc.LookupChunk_Fact{
			{Lookup: 0, Fact: Fact{Id: 40, Predicate: 3, Object: rpc.AString("zero", 0), Index: 50}},
			{Lookup: 1, Fact: Fact{Id: 60, Predicate: 5, Object: rpc.AString("one", 0), Index: 40}},
			// these should be filtered
			{Lookup: 0, Fact: Fact{Id: 40, Predicate: 3, Object: rpc.AString("x", 0), Index: 40}},
			{Lookup: 1, Fact: Fact{Id: 60, Predicate: 3, Object: rpc.AString("x", 0), Index: 40}},
			{Lookup: 1, Fact: Fact{Id: 60, Predicate: 5, Object: rpc.AString("aaa", 0), Index: 40}},
			// but this one should be returned
			{Lookup: 1, Fact: Fact{Id: 30, Predicate: 6, Object: rpc.AString("two", 0), Index: 60}},
		}}}}
	}
	views := fanout.NewViews([]fanout.View{view})
	resCh := make(chan *rpc.LookupChunk, 4)
	err := lookupS(context.Background(), views, req, resCh)
	assert.NoError(err)
	assert.Equal([]string{
		`Lookup 0: 40 800 3 "zero" @ 50`,
		`Lookup 1: 30 600 6 "two" @ 60`,
		`Lookup 1: 60 600 5 "one" @ 40`,
	}, chunksStr(resCh))
}

func Test_lookupSP(t *testing.T) {
	assert := assert.New(t)
	view1, stub1 := mockSPView(0, 2)
	view2, stub2 := mockSPView(1, 2)
	req := &rpc.LookupSPRequest{
		Index:   500,
		Lookups: make([]rpc.LookupSPRequest_Item, 6),
	}
	fillSPData(view1, req.Lookups[:3])
	fillSPData(view2, req.Lookups[3:])
	// map from SP to Facts
	replies := map[rpc.LookupSPRequest_Item][]Fact{
		req.Lookups[1]: []Fact{
			{Id: 20, Object: rpc.AString("Alabama", 0), Index: 10},
			{Id: 40, Object: rpc.AString("Alaska", 0), Index: 60},
			{Id: 40, Object: rpc.AString("Alaska", 0), Index: 60, Predicate: duplicate},
		},
		req.Lookups[3]: []Fact{
			{Id: 90, Object: rpc.AString("Arkansas", 0), Index: 30},
		},
		req.Lookups[4]: []Fact{
			{Id: 91, Object: rpc.AString("Connecticut", 0), Index: 31},
			{Id: 51, Object: rpc.AString("California", 0), Index: 21, Predicate: duplicate},
		},
		req.Lookups[5]: []Fact{
			{Id: 92, Object: rpc.AString("Delaware", 0), Index: 32},
		},
	}

	var expect []string
	for i, lookup := range req.Lookups {
		for _, fact := range replies[lookup] {
			if fact.Predicate != duplicate {
				expect = append(expect, fmt.Sprintf(
					"Lookup %d: %v %v %v %v @ %v",
					i,
					fact.Id,
					lookup.Subject,
					lookup.Predicate,
					fact.Object,
					fact.Index))
			}
		}
	}
	sort.Strings(expect)

	server := func(view *ReadFactsSPClient, req *rpc.LookupSPRequest) *mockLookupChunkStream {
		assert.Equal(blog.Index(500), req.Index)
		chunk := new(rpc.LookupChunk)
		for offset, lookup := range req.Lookups {
			hash := partitioning.HashSP(lookup.Subject, lookup.Predicate)
			assert.True(view.Serves().Contains(space.Hash64(hash)))
			for _, fact := range replies[lookup] {
				chunk.Facts = append(chunk.Facts, rpc.LookupChunk_Fact{
					Lookup: uint32(offset),
					Fact:   fact,
				})
			}
		}
		stream := new(mockLookupChunkStream)
		stream.chunks = []*rpc.LookupChunk{chunk}
		return stream
	}
	stub1.lookupSP = server
	stub2.lookupSP = server

	views := fanout.NewViews([]fanout.View{view1, view2})
	resCh := make(chan *rpc.LookupChunk, 4)
	err := lookupSP(context.Background(), views, req, resCh)
	assert.NoError(err)
	assert.Equal(expect, chunksStr(resCh))
}

func Test_lookupSPO(t *testing.T) {
	assert := assert.New(t)
	view1, stub1 := mockSPView(0, 2)
	view2, stub2 := mockSPView(1, 2)
	req := &rpc.LookupSPORequest{
		Index: 500,
		Lookups: []rpc.LookupSPORequest_Item{
			{Object: rpc.AString("Alabama", 0)},
			{Object: rpc.AString("Alaska", 0)},
			{Object: rpc.AString("Arizona", 0)},
			{Object: rpc.AString("Arkansas", 0)},
		},
	}
	spData := make([]rpc.LookupSPRequest_Item, 4)
	fillSPData(view1, spData[:2])
	fillSPData(view2, spData[2:])
	for i := range spData {
		req.Lookups[i].Subject = spData[i].Subject
		req.Lookups[i].Predicate = spData[i].Predicate
	}

	hashSP := func(sp rpc.LookupSPORequest_Item) space.Hash64 {
		return partitioning.HashSP(sp.Subject, sp.Predicate)
	}
	// map from hash to Facts
	replies := map[space.Hash64][]Fact{
		hashSP(req.Lookups[1]): []Fact{
			{Id: 20, Index: 10},
			{Id: 20, Index: 10, Predicate: duplicate},
		},
		hashSP(req.Lookups[3]): []Fact{
			{Id: 90, Index: 30},
		},
	}

	var expect []string
	for i, lookup := range req.Lookups {
		for _, fact := range replies[hashSP(lookup)] {
			if fact.Predicate != duplicate {
				expect = append(expect, fmt.Sprintf(
					"Lookup %d: %v %v %v %v @ %v",
					i,
					fact.Id,
					lookup.Subject,
					lookup.Predicate,
					lookup.Object,
					fact.Index))
			}
		}
	}
	sort.Strings(expect)

	server := func(view *ReadFactsSPClient, req *rpc.LookupSPORequest) *mockLookupChunkStream {
		assert.Equal(blog.Index(500), req.Index)
		chunk := new(rpc.LookupChunk)
		for offset, lookup := range req.Lookups {
			hash := hashSP(lookup)
			assert.True(view.Serves().Contains(space.Hash64(hash)))
			for _, fact := range replies[hash] {
				chunk.Facts = append(chunk.Facts, rpc.LookupChunk_Fact{
					Lookup: uint32(offset),
					Fact:   fact,
				})
			}
		}
		stream := new(mockLookupChunkStream)
		stream.chunks = []*rpc.LookupChunk{chunk}
		return stream
	}
	stub1.lookupSPO = server
	stub2.lookupSPO = server

	views := fanout.NewViews([]fanout.View{view1, view2})
	resCh := make(chan *rpc.LookupChunk, 4)
	err := lookupSPO(context.Background(), views, req, resCh)
	assert.NoError(err)
	assert.Equal(expect, chunksStr(resCh))
}

func Test_lookupPO(t *testing.T) {
	assert := assert.New(t)
	view1, stub1 := mockPOView(0, 2)
	view2, stub2 := mockPOView(1, 2)
	req := &rpc.LookupPORequest{
		Index: 500,
		Lookups: []rpc.LookupPORequest_Item{
			{Object: rpc.AString("view1-", 0)},
			{Object: rpc.AString("view1-", 0)},
			{Object: rpc.AString("view1-", 0)},
			{Object: rpc.AString("view2-", 0)},
			{Object: rpc.AString("view2-", 0)},
			{Object: rpc.AString("view2-", 0)},
		},
	}
	fillPOData(view1, req.Lookups[:3])
	fillPOData(view2, req.Lookups[3:])
	hashPO := func(po rpc.LookupPORequest_Item) space.Hash64 {
		return partitioning.HashPO(po.Predicate, po.Object)
	}
	// map from hash to Facts
	replies := map[space.Hash64][]Fact{
		hashPO(req.Lookups[1]): []Fact{
			{Id: 20, Subject: 70, Index: 10},
			{Id: 40, Subject: 90, Index: 60},
			{Id: 40, Subject: 50, Index: 60, Predicate: duplicate},
		},
		hashPO(req.Lookups[3]): []Fact{
			{Id: 90, Subject: 20, Index: 30},
		},
		hashPO(req.Lookups[4]): []Fact{
			{Id: 91, Subject: 21, Index: 31},
			{Id: 91, Subject: 21, Index: 31, Predicate: duplicate},
		},
		hashPO(req.Lookups[5]): []Fact{
			{Id: 92, Subject: 22, Index: 32},
		},
	}

	var expect []string
	for i, lookup := range req.Lookups {
		for _, fact := range replies[hashPO(lookup)] {
			if fact.Predicate != duplicate {
				expect = append(expect, fmt.Sprintf(
					"Lookup %d: %v %v %v %v @ %v",
					i,
					fact.Id,
					fact.Subject,
					lookup.Predicate,
					lookup.Object,
					fact.Index))
			}
		}
	}
	sort.Strings(expect)

	server := func(view *ReadFactsPOClient, req *rpc.LookupPORequest) *mockLookupChunkStream {
		assert.Equal(blog.Index(500), req.Index)
		chunk := new(rpc.LookupChunk)
		for offset, lookup := range req.Lookups {
			hash := hashPO(lookup)
			assert.True(view.Serves().Contains(space.Hash64(hash)))
			for _, fact := range replies[hash] {
				chunk.Facts = append(chunk.Facts, rpc.LookupChunk_Fact{
					Lookup: uint32(offset),
					Fact:   fact,
				})
			}
		}
		stream := new(mockLookupChunkStream)
		stream.chunks = []*rpc.LookupChunk{chunk}
		return stream
	}
	stub1.lookupPO = server
	stub2.lookupPO = server

	views := fanout.NewViews([]fanout.View{view1, view2})
	resCh := make(chan *rpc.LookupChunk, 4)
	err := lookupPO(context.Background(), views, req, resCh)
	assert.NoError(err)
	assert.Equal(expect, chunksStr(resCh))
}

func Test_lookupPOCmp(t *testing.T) {
	assert := assert.New(t)
	view1, stub1 := mockPOView(0, 2)
	view2, stub2 := mockPOView(1, 2)
	data := []rpc.LookupPORequest_Item{
		{Predicate: 3, Object: rpc.AString("fjord", 0)},
		{Predicate: 3, Object: rpc.AString("gorilla", 0)},
		{Predicate: 5, Object: rpc.AString("secondary", 0)},
		{Predicate: 5, Object: rpc.AString("second place", 0)},
	}
	fillPOData(view1, data[:2])
	fillPOData(view2, data[2:])
	req := &rpc.LookupPOCmpRequest{
		Lookups: []rpc.LookupPOCmpRequest_Item{
			{
				Predicate: 3,
				Operator:  rpc.OpRangeIncInc,
				Object:    rpc.AString("first", 0),
				EndObject: rpc.AString("hello", 0),
			},
			{
				Predicate: 5,
				Operator:  rpc.OpPrefix,
				Object:    rpc.AString("second", 0),
			},
		},
	}
	stub1.lookupPOCmp = func(view *ReadFactsPOClient, req *rpc.LookupPOCmpRequest) *mockLookupChunkStream {
		return &mockLookupChunkStream{chunks: []*rpc.LookupChunk{{Facts: []rpc.LookupChunk_Fact{
			// ok
			{Lookup: 0, Fact: Fact{Id: 40, Subject: 500, Object: data[0].Object, Index: 50}},
			{Lookup: 0, Fact: Fact{Id: 60, Subject: 600, Object: data[1].Object, Index: 40}},
			{Lookup: 0, Fact: Fact{Id: 70, Subject: 700, Object: data[1].Object, Index: 41}},
			// duplicates
			{Lookup: 0, Fact: Fact{Id: 40, Subject: 500, Object: data[0].Object, Index: 50}},
			{Lookup: 0, Fact: Fact{Id: 60, Subject: 600, Object: data[1].Object, Index: 40}},
		}}}}
	}
	stub2.lookupPOCmp = func(view *ReadFactsPOClient, req *rpc.LookupPOCmpRequest) *mockLookupChunkStream {
		return &mockLookupChunkStream{chunks: []*rpc.LookupChunk{{Facts: []rpc.LookupChunk_Fact{
			// ok
			{Lookup: 1, Fact: Fact{Id: 50, Subject: 200, Object: data[2].Object, Index: 30}},
			// duplicate
			{Lookup: 1, Fact: Fact{Id: 70, Subject: 800, Object: data[3].Object, Index: 60}},
		}}}}
	}
	views := fanout.NewViews([]fanout.View{view1, view2})
	resCh := make(chan *rpc.LookupChunk, 4)
	err := lookupPOCmp(context.Background(), views, req, resCh)
	assert.NoError(err)
	assert.Equal([]string{
		fmt.Sprintf("Lookup 0: 40 500 3 %v @ 50", data[0].Object),
		fmt.Sprintf("Lookup 0: 60 600 3 %v @ 40", data[1].Object),
		fmt.Sprintf("Lookup 0: 70 700 3 %v @ 41", data[1].Object),
		fmt.Sprintf("Lookup 1: 50 200 5 %v @ 30", data[2].Object),
	}, chunksStr(resCh))
}

func Test_kgObjectLess(t *testing.T) {
	assert := assert.New(t)
	foo := rpc.AString("foo", 0)
	bar := rpc.AString("bar", 0)
	baz := rpc.AKID(12)
	bay := rpc.AKID(13)

	// our ordering needs to match the one in diskview/keys as thats the
	// order we'll get from the diskview
	//ktNil       kgObjectType = 0
	//ktString    kgObjectType = 1
	//ktFloat64   kgObjectType = 2
	//ktInt64     kgObjectType = 3
	//ktTimestamp kgObjectType = 4
	//ktBool      kgObjectType = 5
	//ktKID       kgObjectType = 6

	assert.True(kgObjectLess(bar, foo))
	assert.False(kgObjectLess(foo, bar))
	assert.False(kgObjectLess(bar, bar))
	assert.False(kgObjectLess(foo, foo))

	assert.True(kgObjectLess(baz, bay))
	assert.False(kgObjectLess(bay, baz))

	assert.True(kgObjectLess(foo, baz))
	assert.True(kgObjectLess(bar, bay))
	assert.False(kgObjectLess(baz, foo))
	assert.False(kgObjectLess(bay, bar))
}

func Test_drainStream(t *testing.T) {
	assert := assert.New(t)
	count := 0
	in := func() (fanout.Result, error) {
		count++
		switch count {
		case 1, 2, 3:
			return count, nil
		case 4:
			return nil, io.EOF
		case 5, 6:
			return count, nil
		case 7:
			return nil, errors.New("broken input")
		}
		assert.Fail("shouldn't get here", "count=%v", count)
		return nil, nil
	}
	var out []int
	outFn := func(r fanout.Result) {
		out = append(out, r.(int))
	}
	err := drainStream(in, outFn)
	assert.NoError(err)
	assert.Equal([]int{1, 2, 3}, out)
	out = nil
	err = drainStream(in, outFn)
	assert.Error(err)
	assert.EqualError(err, "broken input")
	assert.Equal([]int{5, 6}, out)
}
