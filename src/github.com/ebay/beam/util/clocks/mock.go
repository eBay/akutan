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
	"sync"
	"time"
)

// Mock is a Source that does not advance on its own. It can be used to control
// a clock for unit tests.
type Mock struct {
	// Protects all the fields below.
	lock sync.Mutex
	now  Time
	// All these channels are notified each time 'now' changes.
	timers  map[int]chan<- struct{}
	counter int
}

// Ensures that Mock implements Source.
var _ Source = NewMock()

// NewMock returns a new mock clock that is initialized to the Unix epoch.
// Note that this is not the zero value for time.Time.
func NewMock() *Mock {
	return &Mock{
		now:    time.Unix(0, 0),
		timers: make(map[int]chan<- struct{}),
	}
}

// Now implements Source.Now.
func (c *Mock) Now() Time {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.now
}

// SleepUntil implements Source.SleepUntil. Note that a deadline/timeout on the
// context is measured in wall time, not mocked time.
func (c *Mock) SleepUntil(ctx context.Context, wake time.Time) error {
	alarm := c.NewAlarm()
	defer alarm.Stop()
	alarm.Set(wake)
	select {
	case <-alarm.WaitCh():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Advance moves the clock forward by the given amount. Note that this may cause
// alarms to beep shortly thereafter.
func (c *Mock) Advance(amount time.Duration) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.now = c.now.Add(amount)
	for _, timer := range c.timers {
		select {
		case timer <- struct{}{}:
		default: // timer is already scheduled to wake up
		}
	}
}

// registerTimer returns a channel that will be notified when the clock's time changes,
// as well as a function to unregister this channel and reclaim space.
func (c *Mock) registerTimer() (<-chan struct{}, func()) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.counter++
	key := c.counter
	ch := make(chan struct{}, 1) // buffer a single interrupt
	c.timers[key] = ch
	return ch, func() {
		c.lock.Lock()
		defer c.lock.Unlock()
		delete(c.timers, key)
	}
}

// NewAlarm implements Source.NewAlarm.
func (c *Mock) NewAlarm() Alarm {
	alarm := &mockAlarm{
		clock:  c,
		beepCh: make(chan struct{}),
	}
	return alarm
}

type mockAlarm struct {
	clock  *Mock
	beepCh chan struct{}
	// Protects changes to setCh field.
	lock sync.Mutex
	// Sent to by alarm to tell timer to reschedule. Closed to tell timer to exit.
	// If nil, no timer is running.
	setCh chan Time
}

func (alarm *mockAlarm) WaitCh() <-chan struct{} {
	return alarm.beepCh
}

func (alarm *mockAlarm) Set(wake Time) {
	alarm.lock.Lock()
	defer alarm.lock.Unlock()
	if alarm.setCh == nil {
		alarm.setCh = make(chan Time)
		go mockTimer(alarm.clock, alarm.setCh, alarm.beepCh)
	}
	alarm.setCh <- wake
}

func (alarm *mockAlarm) Stop() {
	alarm.lock.Lock()
	defer alarm.lock.Unlock()
	if alarm.setCh != nil {
		close(alarm.setCh)
		alarm.setCh = nil
	}
}

// mockTimer notifies beep when a mock alarm fires. mockTimer reschedules itself
// when receiving from alarmSet and exits when alarmSet is closed.
func mockTimer(clock *Mock, alarmSet <-chan Time, beep chan<- struct{}) {
	clockSet, unregister := clock.registerTimer()
	defer unregister()
top: // await instructions on when to wake
	wake, ok := <-alarmSet
	if !ok {
		return
	}
gotSet: // clock or alarm was set
	now := clock.Now()
	if !wake.After(now) { // beep!
		select {
		case wake, ok = <-alarmSet:
			if !ok {
				return
			}
			goto gotSet
		case beep <- struct{}{}:
			goto top
		}
	}
	// Wait around.
	select {
	case wake, ok = <-alarmSet:
		if !ok {
			return
		}
		goto gotSet
	case <-clockSet:
		goto gotSet
	}
}
