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

package diskview

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ebay/akutan/diskview/keys"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/util/clocks"
	proto "github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

// statsThreshold is the minimum number of counts of fact subsets required to be registered as a count
// e.g. if there are more than 'statsThreshold' number of predicate=1 object='Product' facts, then the
// count will get recorded in the final stats
const statsThreshold = 100

type countTracker struct {
	// If 0, no changes have been made: don't need to tally up the stats again. Accessed using atomic ops.
	written   uint32
	triggerCh chan struct{}
	// closing stopCh will stop all background work
	stopCh chan struct{}
	clock  clocks.Source

	lock   sync.RWMutex // protects all fields below
	locked struct {
		last GraphMeta
	}
}

// storage defines an abstraction for reading and writing protobuf encoded types as a key's value.
type storage interface {
	readKeyValue(keySpec keys.Spec, val proto.Unmarshaler) error
	writeKeyValue(keySpec keys.Spec, val proto.Marshaler) error
}

func (t *countTracker) init(carousel *carouselConductor, clock clocks.Source, store storage, interval time.Duration) {
	t.clock = clock
	t.triggerCh = make(chan struct{}, 1)
	t.stopCh = make(chan struct{})
	existing := GraphMeta{}
	if err := store.readKeyValue(keys.StatsKey{}, &existing); err != nil {
		log.Warnf("failed to read stats key (there won't be useful stats until the first scan is complete): %v", err)
		existing = GraphMeta{}
	}
	t.lock.Lock()
	t.locked.last = existing
	t.lock.Unlock()
	t.haveWritten() // we have no idea what happened since the stats were last persisted
	go func() {
		recalc := t.clock.NewAlarm()
		for {
			t.update()
			recalc.Set(t.clock.Now().Add(interval))
			select {
			case <-t.stopCh:
				recalc.Stop()
				return
			case <-recalc.WaitCh():
			}
			recalc.Stop()
		}
	}()
	go func() {
		for {
			select {
			case <-t.stopCh:
				return
			case <-t.triggerCh:
				if !atomic.CompareAndSwapUint32(&t.written, 1, 0) {
					continue
				}
				start := time.Now()
				log.Info("Start carousel ride to calculate statistics")
				counts, err := t.countNow(carousel)
				dur := time.Since(start)
				if err == nil {
					t.lock.Lock()
					t.locked.last = counts
					t.lock.Unlock()
					if err := store.writeKeyValue(keys.StatsKey{}, &counts); err != nil {
						log.Warnf("Unable to save stats: %v", err)
					}
					log.Debugf("Updating store counts to %v, took %v", counts, dur)
				} else {
					log.Warnf("Unable to calculate counts: %v", err)
				}
			}
		}
	}()
}

// stop will asynchronously stop any background goroutines that were created.
func (t *countTracker) stop() {
	close(t.stopCh)
}

func (t *countTracker) haveWritten() {
	atomic.StoreUint32(&t.written, 1)
}

func (t *countTracker) update() {
	select {
	case t.triggerCh <- struct{}{}:
	default:
	}
}

func (t *countTracker) current() GraphMeta {
	t.lock.RLock()
	c := t.locked.last
	t.lock.RUnlock()
	return c
}

// countNow will brute force calculate s,p,o counts by iterating all the keys via a carousel
func (t *countTracker) countNow(carousel *carouselConductor) (GraphMeta, error) {
	var lastKey []byte

	stats := statsAccumulator{}
	c := GraphMeta{
		AsOf: t.clock.Now(),
	}

	rider := carousel.addRider()
	finishedRide := false
	for !finishedRide {
		select {
		case items, ok := <-rider.dataCh:
			if !ok {
				finishedRide = true

			} else {
				for _, i := range items {
					sameKey := keys.FactKeysEqualIgnoreIndex(lastKey, i.key)

					switch fk := i.parsedKey.(type) {
					case keys.FactKey:
						c.FactVersions++
						if !sameKey {
							c.FactCount++
						}
						stats.add(&fk)
					}
					lastKey = i.key
				}
			}

		case err := <-rider.errorCh:
			rider.dismount()
			return c, err

		case <-t.stopCh:
			rider.dismount()
			return c, errors.New("in progress count stopped by external caller")
		}
	}
	stats.finalizeResults()
	c.Stats = stats.results
	return c, nil
}

type statsAccumulator struct {
	predicates        rpc.PredicateStats
	predicateObjects  rpc.PredicateObjectStats
	subjects          rpc.SubjectStats
	subjectPredicates rpc.SubjectPredicateStats

	results rpc.FactStatsResult
}

func (a *statsAccumulator) finalizeResults() {
	a.flushPredicate()
	a.flushPredicateObject()
	a.flushSubject()
	a.flushSubjectPredicate()
}

func (a *statsAccumulator) flushPredicate() {
	if a.predicates.Count > statsThreshold {
		a.results.Predicates = append(a.results.Predicates, a.predicates)
	}
	a.predicates.Reset()
}

func (a *statsAccumulator) flushPredicateObject() {
	if a.predicateObjects.Count > statsThreshold {
		a.results.PredicateObjects = append(a.results.PredicateObjects, a.predicateObjects)
	}
	a.predicateObjects.Reset()
}

func (a *statsAccumulator) flushSubject() {
	if a.subjects.Count > statsThreshold {
		a.results.Subjects = append(a.results.Subjects, a.subjects)
	}
	a.subjects.Reset()
}

func (a *statsAccumulator) flushSubjectPredicate() {
	if a.subjectPredicates.Count > statsThreshold {
		a.results.SubjectPredicates = append(a.results.SubjectPredicates, a.subjectPredicates)
	}
	a.subjectPredicates.Reset()
}

func (a *statsAccumulator) add(fk *keys.FactKey) {
	a.results.NumFacts++
	switch fk.Encoding {
	case rpc.KeyEncodingPOS:
		if fk.Fact.Predicate != a.predicates.Predicate {
			a.flushPredicate()
			a.predicates.Predicate = fk.Fact.Predicate
			a.results.DistinctPredicates++
		}
		a.predicates.Count++

		if fk.Fact.Predicate != a.predicateObjects.Predicate ||
			!fk.Fact.Object.Equal(a.predicateObjects.Object) {
			a.flushPredicateObject()
			a.predicateObjects.Predicate = fk.Fact.Predicate
			a.predicateObjects.Object = fk.Fact.Object
			a.results.DistinctPredicateObjects++
		}
		a.predicateObjects.Count++

	case rpc.KeyEncodingSPO:
		if fk.Fact.Subject != a.subjects.Subject {
			a.flushSubject()
			a.subjects.Subject = fk.Fact.Subject
			a.results.DistinctSubjects++
		}
		a.subjects.Count++

		if fk.Fact.Subject != a.subjectPredicates.Subject || fk.Fact.Predicate != a.subjectPredicates.Predicate {
			a.flushSubjectPredicate()
			a.subjectPredicates.Subject = fk.Fact.Subject
			a.subjectPredicates.Predicate = fk.Fact.Predicate
			a.results.DistinctSubjectPredicates++
		}
		a.subjectPredicates.Count++
	}
}
