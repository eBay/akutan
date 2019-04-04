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

package impl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/cmp"
	"github.com/ebay/beam/util/profiling"
	"github.com/ebay/beam/util/table"
	"github.com/ebay/beam/util/web"
	"github.com/ebay/beam/viewclient"
	"github.com/julienschmidt/httprouter"
	log "github.com/sirupsen/logrus"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

func (s *Server) profile(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	dur, err := parseDuration(r, "d", nil)
	if err != nil {
		web.Write(w, err)
		return
	}
	err = profiling.CPUProfileForDuration("prof.cpu", dur)
	if err != nil {
		web.Write(w, err)
		return
	}
	views := s.source.DiagnosticsViews()
	for i, v := range views {
		v.Stub.Profile(context.Background(), &rpc.ProfileRequest{Filename: fmt.Sprintf("prof_%d.cpu", i), Duration: dur + (time.Second / 10)})
	}
	time.Sleep(dur)
	web.Write(w, "Profiling Complete\n")
}

func (s *Server) setLogLevel(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	viewIdx := p.ByName("viewIndex")
	if viewIdx == "" {
		web.WriteError(w, http.StatusBadRequest, "Need to indicate which view")
		return
	}
	views := s.source.DiagnosticsViews()
	if viewIdx == "*" {
		// do all of them
	} else {
		idx, err := strconv.Atoi(viewIdx)
		if err != nil {
			web.WriteError(w, http.StatusBadRequest, "Unable to parse view index: %v", err)
			return
		}
		if idx >= len(views) {
			web.WriteError(w, http.StatusNotFound, "View %d doesn't exist", idx)
			return
		}
		views = views[idx : idx+1]
	}
	levelName := r.URL.Query().Get("l")
	level, err := log.ParseLevel(levelName)
	if err != nil {
		web.WriteError(w, http.StatusBadRequest, "Unable to parse level name: %s, %v", levelName, err)
		return
	}
	errors := make([]string, 0)
	for _, view := range views {
		_, err = view.Stub.SetLogLevel(context.Background(), &rpc.LogLevelRequest{NewLevel: rpc.LogLevel(level)})
		if err != nil {
			errors = append(errors, fmt.Sprintf("unable to update log level on %v: %v", view.View, err))
		}
	}
	if len(errors) > 0 {
		web.WriteError(w, http.StatusInternalServerError, strings.Join(errors, "\n"))
		return
	}
	if viewIdx == "*" {
		// also update our log level as well in this case
		log.SetLevel(level)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	stats := s.source.Stats(r.Context())
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (s *Server) updateStats(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Infof("Calling UpdateStats on all views")
	err := s.source.UpdateStats(r.Context())
	if err != nil {
		log.Warnf("Error calling UpdateStats: %v", err)
		web.WriteError(w, http.StatusInternalServerError, "unable to update stats on all views: %v", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func createCountRatioString(p *message.Printer, count, versions int64) string {
	if versions == 0 {
		return p.Sprintf("%d", count)
	}
	return p.Sprintf("%d x %.3f", count, float64(versions)/float64(count))
}

func (s *Server) statsTable(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	stats := s.source.Stats(r.Context())
	var withStats, withErrors []viewclient.ViewStats
	for _, s := range stats {
		if s.Stats != nil {
			withStats = append(withStats, s)
		} else {
			withErrors = append(withErrors, s)
		}
	}
	sort.Slice(withStats, func(i, j int) bool {
		return withStats[i].Stats.Hostname < withStats[j].Stats.Hostname
	})
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	st := make([][]string, 0, 1+len(withStats))
	st = append(st, []string{"Hostname", "ViewType", "Partition", "# Facts", "# Txs", "At Index", "Carousel\nPosition", "Carousel\nRiders", "Uptime"})
	errs := make([][]string, 0, 4)
	p := message.NewPrinter(language.English)
	for _, vs := range withStats {
		s := vs.Stats
		hostname := s.Hostname
		if len(hostname) > 0 {
			start := hostname[0]
			if (start >= 'a' && start <= 'z') ||
				(start >= 'A' && start <= 'Z') {
				hostname = strings.SplitN(hostname, ".", 2)[0]
			}
		}
		if len(s.LastCarouselKey) > 20 {
			s.LastCarouselKey = s.LastCarouselKey[:20]
		}
		st = append(st, []string{
			hostname,
			s.ViewType,
			s.Partition,
			createCountRatioString(p, s.Facts, s.FactVersions),
			p.Sprintf("%d", s.Txs),
			p.Sprintf("%d", s.LastIndex),
			string(s.LastCarouselKey),
			p.Sprintf("%d", s.CarouselRiders),
			s.UpTime.Truncate(time.Second).String(),
		})
	}
	for _, vs := range withErrors {
		errs = append(errs, []string{vs.View.Endpoint.String(), vs.Err.Error()})
	}
	padRight(st[1:], 2)
	table.PrettyPrint(w, st, table.HeaderRow|table.RightJustify)
	if len(errs) > 0 {
		headers := []string{"view", "error"}
		errs = append([][]string{headers}, errs...)
		io.WriteString(w, "\n")
		table.PrettyPrint(w, errs, table.HeaderRow|table.RightJustify)
	}
}

func padRight(table [][]string, colToPad int) {
	w := 0
	for _, r := range table {
		w = cmp.MaxInt(w, utf8.RuneCountInString(r[colToPad]))
	}
	for i := range table {
		table[i][colToPad] += strings.Repeat(" ", w-utf8.RuneCountInString(table[i][colToPad]))
	}
}

// dumpViewRegistry is an HTTP endpoint to diagnose the contents of the local viewreg.Registry.
func (s *Server) dumpViewRegistry(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	var b bytes.Buffer
	b.Grow(1024)
	s.source.Registry.Dump(&b)
	w.Write(b.Bytes())
}
