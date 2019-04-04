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

// Package parallel is a utility package for running parallel/concurrent tasks.
package parallel

import "context"

// Invoke runs the given callbacks concurrently. All the callbacks are run in a
// child of 'ctx'. If any of the callbacks returns an error, Invoke cancels this
// child context, waits for the remaining callbacks to complete, and returns the
// first error. Otherwise, Invoke waits for all the callbacks to complete, then
// returns nil.
func Invoke(ctx context.Context, calls ...func(ctx context.Context) error) error {
	return InvokeN(ctx, len(calls),
		func(ctx context.Context, i int) error {
			return calls[i](ctx)
		})
}

// InvokeN runs the given callback 'n' times concurrently. It invokes the
// callbacks with i=0, i=1, ..., i=n-1 in a child of 'ctx'. If any of the
// callbacks returns an error, InvokeN cancels this child context, waits for the
// remaining callbacks to complete, and returns the first error. Otherwise,
// InvokeN waits for all the callbacks to complete, then returns nil.
func InvokeN(ctx context.Context, n int, call func(ctx context.Context, i int) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			ch <- call(ctx, i)
		}(i)
	}
	var firstErr error
	for i := 0; i < n; i++ {
		err := <-ch
		if err != nil && firstErr == nil {
			firstErr = err
			cancel()
		}
	}
	return firstErr
}

// Go is like the 'go' keyword but returns a function that blocks until the
// goroutine exits. Its safe to call the returned wait function multiple times
func Go(run func()) (wait func()) {
	done := make(chan struct{})
	go func() {
		run()
		close(done)
	}()
	return func() {
		<-done
	}
}

// GoCaptureError is like the go keyword but returns a function that blocks until the
// goroutine exits, the returned error from the goroutine function is available as
// the result of calling the retuned wait() function. Its safe to call the returned
// wait function mutliple times, it'll always report the same result
func GoCaptureError(run func() error) (wait func() error) {
	done := make(chan error, 1)
	go func() {
		done <- run()
		close(done)
	}()
	var resultErr error
	return func() error {
		err, open := <-done
		if open {
			resultErr = err
		}
		return resultErr
	}
}
