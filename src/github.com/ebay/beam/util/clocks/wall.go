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

// Package clocks provides a mockable way to measure time and set timers. It
// also has a nicer timer interface than time.Timer.
package clocks

import (
	"context"
	"sync"
	"time"
)

// Time is a convenient alias for time.Time.
type Time = time.Time

// A Source tell the passage of time. This package provides two sources: Wall
// and Mock.
type Source interface {
	// Now returns the current time.
	Now() Time
	// NewAlarm creates an alarm that won't yet fire.
	NewAlarm() Alarm
	// Sleep blocks until at least the given time or a context error, whichever
	// comes first. If the context expires, Sleep returns the context error.
	// Otherwise, it waits until Now() returns at least the given time and
	// returns nil.
	//
	// Historical note: this method used to be defined as:
	//     Sleep(context.Context, time.Duration) error
	// but this resulted in a race when using the mock clock. Consider this
	// example:
	//     now := clocks.Mock.Now()
	//     if now.Before(wake) {
	//         clocks.Mock.Sleep(ctx, wake.Sub(now))
	//     }
	// If the mock time advanced between the Now() and the Sleep(), the code
	// would still wait for the mock time to advance again. Switching to
	// SleepUntil with a deadline eliminates this race, and it isn't too
	// inconvenient for other callers.
	SleepUntil(ctx context.Context, wake time.Time) error
}

// An Alarm alerts the user when a given time is reached.
type Alarm interface {
	// Set schedules the alarm to beep at or shortly after the given time. Any
	// previously scheduled wakeups are lost. Set is thread-safe.
	//
	// After calling Set, the caller must use Stop to reclaim resources, even after
	// the alarm beeps.
	Set(wake Time)
	// Stop unschedules the alarm. Upon returning from Stop, the alarm will not beep
	// anymore (unless Set is subsequently called). Stop is thread-safe, and it is
	// safe to call Stop multiple times.
	Stop()
	// WaitCh returns a channel on which the caller may receive an empty value each
	// time the alarm beeps. The alarm will send on the channel up to once per call
	// to Set. The alarm will never close this channel.
	WaitCh() <-chan struct{}
}

type wallClock struct{}

// Wall is the normal clock, as provided by time.Now().
var Wall Source = wallClock{}

func (wallClock) Now() Time {
	return time.Now()
}

func (wallClock) NewAlarm() Alarm {
	return &wallAlarm{
		beep: make(chan struct{}),
	}
}

func (source wallClock) SleepUntil(ctx context.Context, wake time.Time) error {
	ctx, cancel := context.WithDeadline(ctx, wake)
	defer cancel()
	<-ctx.Done()
	if source.Now().Before(wake) {
		return ctx.Err()
	}
	return nil
}

type wallAlarm struct {
	beep      chan struct{}
	lock      sync.Mutex
	interrupt chan interface{} // Time or exitSentinel("exit")
}

type exitSentinel string

func (alarm *wallAlarm) WaitCh() <-chan struct{} {
	return alarm.beep
}

func (alarm *wallAlarm) Set(wake Time) {
	alarm.lock.Lock()
	if alarm.interrupt == nil {
		alarm.interrupt = make(chan interface{})
		go wallTimer(wake, alarm.beep, alarm.interrupt)
	} else {
		alarm.interrupt <- wake
	}
	alarm.lock.Unlock()
}

func (alarm *wallAlarm) Stop() {
	alarm.lock.Lock()
	if alarm.interrupt != nil {
		alarm.interrupt <- exitSentinel("exit")
		alarm.interrupt = nil
	}
	alarm.lock.Unlock()
}

func wallTimer(wake Time, beep chan<- struct{}, interrupt <-chan interface{}) {
	timer := time.NewTimer(wake.Sub(Wall.Now()))
	var intr interface{}

	for {
	sleep:
		select {
		case <-timer.C:
			now := Wall.Now()
			if wake.Before(now) || wake.Equal(now) {
				select {
				case beep <- struct{}{}:
				case intr = <-interrupt:
					goto interrupted
				}
			}
			goto sleep
		case intr = <-interrupt:
			goto interrupted
		}

	interrupted:
		timer.Stop()
		for len(timer.C) > 0 {
			<-timer.C
		}
		if _, exit := intr.(exitSentinel); exit {
			return
		}
		wake = intr.(Time)
		timer.Reset(wake.Sub(Wall.Now()))
	}
}
