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

package parallel

import (
	"context"
	"errors"
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

// It's easy to implement a concurrent map operation using InvokeN.
func ExampleInvokeN_map() {
	ctx := context.Background()
	// A concurrent map: res = map(in, isEven).
	isEven := func(x int) bool {
		return x%2 == 0
	}
	in := []int{5, 6, 7}
	res := make([]bool, 3)
	_ = InvokeN(ctx, len(in), func(ctx context.Context, i int) error {
		res[i] = isEven(in[i])
		return nil
	})
	fmt.Printf("result: %v\n", res)
	// Output:
	// result: [false true false]
}

// More realistically, we'll want to call some server(s) for the results.
func ExampleInvokeN_mapRPC() {
	type Request struct {
		x       int
		replyCh chan bool
	}
	requestCh := make(chan Request)
	go func() { // server
		for req := range requestCh {
			isEven := req.x%2 == 0
			req.replyCh <- isEven
		}
	}()
	rpc := func(ctx context.Context, x int) (bool, error) {
		req := Request{x: x, replyCh: make(chan bool, 1)}
		select {
		case requestCh <- req:
			select {
			case res := <-req.replyCh:
				return res, nil
			case <-ctx.Done():
				return false, ctx.Err()
			}
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	in := []int{10, 11, 12}
	res := make([]bool, 3)
	ctx := context.Background()
	err := InvokeN(ctx, len(res), func(ctx context.Context, i int) error {
		var err error
		res[i], err = rpc(ctx, in[i])
		return err
	})
	close(requestCh) // stop the server
	if err != nil {
		panic("Unexpected error: " + err.Error())
	}
	fmt.Printf("result: %v\n", res)
	// Output:
	// result: [true false true]
}

func Test_Invoke_basic(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	err := Invoke(ctx)
	assert.NoError(err)

	res := make([]int, 3)
	err = Invoke(ctx,
		func(ctx context.Context) error { res[0] = 5; return ctx.Err() },
		func(ctx context.Context) error { res[1] = 6; return errors.New("roar") },
		func(ctx context.Context) error { res[2] = 7; return ctx.Err() },
	)
	assert.Error(err)
	assert.Equal("roar", err.Error())
	assert.Equal([]int{5, 6, 7}, res)

	res = make([]int, 3)
	err = Invoke(ctx,
		func(ctx context.Context) error { res[0] = 3; return ctx.Err() },
		func(ctx context.Context) error { res[1] = 6; return ctx.Err() },
		func(ctx context.Context) error { res[2] = 5; return ctx.Err() },
	)
	assert.NoError(err)
	assert.Equal([]int{3, 6, 5}, res)
}

func Test_InvokeN_basic(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	err := InvokeN(ctx, 0, func(ctx context.Context, idx int) error {
		assert.Fail("should not be called")
		return errors.New("failed")
	})
	assert.NoError(err)

	res := make([]int, 3)
	err = InvokeN(ctx, 3, func(ctx context.Context, idx int) error {
		switch idx {
		case 0:
			res[0] = 5
		case 1:
			res[1] = 6
			return errors.New("roar")
		case 2:
			res[2] = 7
		default:
			assert.Fail("unexpected idx", "idx: %v", idx)
		}
		return ctx.Err()
	})
	assert.Error(err)
	assert.Equal("roar", err.Error())
	assert.Equal([]int{5, 6, 7}, res)

	res = make([]int, 3)
	err = InvokeN(ctx, 3, func(ctx context.Context, idx int) error {
		res[idx] = idx + 3
		return ctx.Err()
	})
	assert.NoError(err)
	assert.Equal([]int{3, 4, 5}, res)
}

func Test_InvokeN_earlyExit(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := InvokeN(ctx, 10, func(ctx context.Context, i int) error {
		if i == 0 {
			return errors.New("failure")
		}
		<-ctx.Done()
		return ctx.Err()
	})
	assert.EqualError(err, "failure")
	assert.NoError(ctx.Err())
}

func ExampleGo() {
	x := 3
	wait := Go(func() {
		x++
	})
	// do some other work
	wait()
	log.Print(x)
	// Outputs: 3
}

func Test_Go(t *testing.T) {
	assert := assert.New(t)
	x := 3
	assert.Equal(3, x)
	wait := Go(func() {
		x++
	})
	wait()
	assert.Equal(4, x)
	// Extra calls to wait are ok.
	wait()
	wait()
	assert.Equal(4, x)
}

func Test_GoCaptureError(t *testing.T) {
	e := errors.New("something went wrong")
	wait := GoCaptureError(func() error {
		return nil
	})
	assert.Nil(t, wait())
	assert.Nil(t, wait())
	ch := make(chan string)
	wait = GoCaptureError(func() error {
		ch <- "started"
		defer func() { ch <- "stopped" }()
		return e
	})
	// GoCaptureError should return before the started routine has finished
	assert.Equal(t, "started", <-ch)
	assert.Equal(t, "stopped", <-ch)
	// Extra calls to wait are ok.
	assert.Equal(t, e, wait())
	assert.Equal(t, e, wait())
}
