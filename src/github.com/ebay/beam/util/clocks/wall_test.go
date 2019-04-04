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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_Wall_Now(t *testing.T) {
	assert := assert.New(t)
	before := time.Now()
	x := Wall.Now()
	after := time.Now()
	assert.True(before.Before(x))
	assert.True(x.Before(after))
}

func Test_Wall_Alarm(t *testing.T) {
	assert := assert.New(t)
	alarm := Wall.NewAlarm()
	select {
	case <-alarm.WaitCh():
		assert.Fail("channel shouldn't be ready")
	default:
	}
	alarm.Stop()
	alarm.Stop()
	select {
	case <-alarm.WaitCh():
		assert.Fail("channel shouldn't be ready")
	default:
	}
	alarm.Set(Wall.Now().Add(time.Hour))
	select {
	case <-alarm.WaitCh():
		assert.Fail("channel shouldn't be ready")
	default:
	}
	alarm.Set(Wall.Now().Add(-time.Millisecond))
	<-alarm.WaitCh()
	select {
	case <-alarm.WaitCh():
		assert.Fail("channel shouldn't be ready")
	default:
	}
	alarm.Set(Wall.Now())
	<-alarm.WaitCh()
	select {
	case <-alarm.WaitCh():
		assert.Fail("channel shouldn't be ready")
	default:
	}
	alarm.Stop()
}

func Test_Wall_Alarm_reallySleep(t *testing.T) {
	if testing.Short() {
		return
	}
	assert := assert.New(t)
	alarm := Wall.NewAlarm()
	defer alarm.Stop()
	start := time.Now()
	alarm.Set(Wall.Now().Add(40 * time.Millisecond))
	time.Sleep(6 * time.Millisecond)
	select {
	case <-alarm.WaitCh():
		assert.Fail("channel shouldn't be ready, elapsed time %v", time.Since(start))
		return // we've consumed from the channel, the subsequent select won't ever find anything
	default:
	}
	select {
	case <-alarm.WaitCh():
	case <-time.After(time.Millisecond * 500):
		assert.Fail("channel should have been ready, elapsed time %v", time.Since(start))
	}
}

func Test_Wall_Sleep_expired(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := Wall.Now()
	err := Wall.SleepUntil(ctx, start.Add(time.Second))
	elapsed := Wall.Now().Sub(start)
	assert.Equal(context.Canceled, err)
	assert.True(elapsed < 100*time.Millisecond)
}

func Test_Wall_Sleep_notAtAll(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := Wall.Now()
	err := Wall.SleepUntil(ctx, start)
	elapsed := Wall.Now().Sub(start)
	assert.NoError(err)
	assert.True(elapsed < 100*time.Millisecond)
}

func Test_Wall_Sleep_reallySleep(t *testing.T) {
	if testing.Short() {
		return
	}
	assert := assert.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := Wall.Now()
	err := Wall.SleepUntil(ctx, start.Add(10*time.Millisecond))
	elapsed := Wall.Now().Sub(start)
	assert.NoError(err)
	assert.True(elapsed >= 10*time.Millisecond)
	assert.True(elapsed < 100*time.Millisecond)
}

func Test_Wall_Sleep_reallySleepInterrupted(t *testing.T) {
	if testing.Short() {
		return
	}
	assert := assert.New(t)
	start := Wall.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := Wall.SleepUntil(ctx, start.Add(200*time.Millisecond))
	elapsed := Wall.Now().Sub(start)
	assert.Equal(context.DeadlineExceeded, err)
	assert.True(elapsed >= 10*time.Millisecond, "elapsed expected to be >= 10ms but was %v", elapsed)
	assert.True(elapsed < 500*time.Millisecond, "elapsed expected to be <= 500ms but was %v", elapsed)
}
