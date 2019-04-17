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

// Command db-scan reads all keys from a Rocks database.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"sync"
	"time"

	"github.com/ebay/beam/config"
	"github.com/ebay/beam/diskview/database"
	"github.com/ebay/beam/diskview/keys"
	"github.com/ebay/beam/diskview/rocksdb"
	"github.com/ebay/beam/util/debuglog"
	log "github.com/sirupsen/logrus"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type stats struct {
	duration time.Duration
	sum      int
	rows     int
	bytes    uint64
}

var fmtr = message.NewPrinter(language.English)

func main() {
	debuglog.Configure(debuglog.Options{})
	cpu := flag.String("cpu", "", "Generate a CPU profile to this file")
	trc := flag.String("tr", "", "Generate an execution trace to this file")
	keys := flag.Bool("keys", false, "Output key values")
	bytes := flag.Bool("bytes", false, "Output raw bytes")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [db_dir ...]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}
	log.SetLevel(log.ErrorLevel)
	stats := make(map[string]*stats, flag.NArg())
	var wg sync.WaitGroup
	for i := 0; i < flag.NArg(); i++ {
		wg.Add(1)
		go func(dir string) {
			defer wg.Done()
			db, err := rocksdb.NewWithDir(dir, &config.DiskView{})
			if err != nil {
				log.Fatalf("Unable to open db: %s: %v", dir, err)
			}
			stats[dir] = iter(db, *cpu, *trc, *keys, *bytes)
		}(flag.Arg(i))
	}
	wg.Wait()
	for database, s := range stats {
		fmtr.Printf("%s: Sum: %d, ", database, s.sum)
		fmtr.Printf("%d Rows read in %s, %dMB, %.2fMB/Sec\n", s.rows, s.duration, s.bytes/1024/1024, float64(s.bytes)/1024/1024/s.duration.Seconds())
	}
}

func iter(db database.DB, cpu, trc string, printKeys bool, printBytes bool) *stats {
	if cpu != "" {
		pf, err := os.Create(cpu)
		if err != nil {
			log.Fatalf("Unable to create file for CPU Profiling: %v", err)
		}
		err = pprof.StartCPUProfile(pf)
		if err != nil {
			log.Fatalf("Unable to begin CPU Profiling: %v", err)
		}
		defer func() {
			pprof.StopCPUProfile()
			pf.Close()
		}()
	}
	if trc != "" {
		tf, err := os.Create(trc)
		if err != nil {
			log.Fatalf("Unable to create file for execution trace: %v", err)
		}
		tw := bufio.NewWriter(tf)
		if err := trace.Start(tw); err != nil {
			log.Fatalf("Unable to start trace: %v", err)
		}
		defer func() {
			trace.Stop()
			tw.Flush()
			tf.Close()
		}()
	}
	count := 0
	sum := 0
	bytes := uint64(0)
	start := time.Now()
	snap := db.Snapshot()
	defer snap.Close()
	printer := func(k, v []byte) {}
	if printKeys {
		printer = func(k, _ []byte) {
			if printBytes {
				fmt.Printf("%s\n", k)
				return
			}
			ik, err := keys.ParseKey(k)
			if err != nil {
				return
			}
			fk, ok := ik.(*keys.FactKey)
			if !ok {
				return
			}
			s := fmt.Sprintf("%d: %d -> %d -> %s", fk.Fact.Id, fk.Fact.Subject, fk.Fact.Predicate, fk.Fact.Object.String())
			if fk.Fact.Object.UnitID() > 0 {
				s += fmt.Sprintf(" (%d)", fk.Fact.Object.UnitID())
			}
			if fk.Fact.Object.LangID() > 0 {
				s += fmt.Sprintf(" (%d)", fk.Fact.Object.LangID())
			}
			fmt.Printf("%s\n", s)
		}
	}
	snap.Enumerate(nil, []byte{0xFF}, func(k, v []byte) error {
		sum = unroll(sum, k)
		sum = unroll(sum, v)
		bytes += uint64(len(k) + len(v))
		count++
		printer(k, v)
		return nil
	})
	return &stats{
		duration: time.Since(start),
		sum:      sum,
		rows:     count,
		bytes:    bytes,
	}
}

func unroll(sum int, data []byte) int {
	for len(data) > 8 {
		sum += int(data[0])
		sum += int(data[1])
		sum += int(data[2])
		sum += int(data[3])
		sum += int(data[4])
		sum += int(data[5])
		sum += int(data[6])
		sum += int(data[7])
		data = data[8:]
	}
	for _, x := range data {
		sum += int(x)
	}
	return sum
}
