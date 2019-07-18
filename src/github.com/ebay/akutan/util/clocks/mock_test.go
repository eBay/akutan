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

package clocks

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ebay/akutan/util/parallel"
	"github.com/stretchr/testify/assert"
)

func ExampleMock() {
	source := NewMock()
	fmt.Printf("start: %v\n", source.Now().UnixNano())
	source.Advance(time.Second)
	fmt.Printf("then: %v\n", source.Now().UnixNano())
	// Output:
	// start: 0
	// then: 1000000000
}

func ExampleMock_alarm() {
	source := NewMock()
	alarm := source.NewAlarm()
	defer alarm.Stop()
	alarm.Set(source.Now().Add(10 * time.Millisecond))
	wait := parallel.Go(func() {
		<-alarm.WaitCh()
		fmt.Printf("Beep!\n")
	})
	fmt.Printf("No beep yet!\n")
	source.Advance(12 * time.Millisecond)
	// The mock alarm will fire really soon, but it needs some CPU time to process.
	wait()
	// Output:
	// No beep yet!
	// Beep!
}

func TestMock_sleep(t *testing.T) {
	assert := assert.New(t)
	source := NewMock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		for ctx.Err() == nil {
			source.Advance(time.Second)
		}
	}()
	start := time.Now()
	err := source.SleepUntil(ctx, source.Now().Add(10*time.Second))
	elapsed := time.Since(start)
	assert.NoError(err)
	assert.True(elapsed < 500*time.Millisecond, elapsed.String())
}

func TestMock_sleep_reallySleep_timeout(t *testing.T) {
	if testing.Short() {
		return
	}
	assert := assert.New(t)
	source := NewMock()
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := source.SleepUntil(ctx, source.Now().Add(10*time.Second))
	elapsed := time.Since(start)
	assert.Equal(context.DeadlineExceeded, err)
	assert.True(elapsed >= 10*time.Millisecond, "elapsed expected to be greater than 10ms but was %v", elapsed)
	assert.True(elapsed < 500*time.Millisecond, "elapsed expected to be less than 500ms but was %v", elapsed)
}
