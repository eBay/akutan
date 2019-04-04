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

package main

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/ebay/beam/config"
	"github.com/ebay/beam/partitioning"
	"github.com/ebay/beam/rpc"
	"github.com/ebay/beam/util/table"
	"github.com/ebay/beam/viewclient"
	"github.com/ebay/beam/viewclient/viewreg"
	"github.com/sirupsen/logrus"
)

type clusterSource struct {
	opts     *options
	cfg      *config.Beam
	client   *viewclient.Client
	viewPred viewclient.ViewInfoPredicate
}

func newClusterSource(o *options) (carouselSource, error) {
	cfg, err := config.Load(o.cfgFile)
	if err != nil {
		return nil, err
	}
	client, err := viewclient.New(cfg)
	if err != nil {
		return nil, err
	}

	// Wait until the view registry is ready. For now, we just wait until it
	// hasn't changed for 5s.
	//
	// TODO: Wait until a particular condition is met, like having enough HashPO
	// and HashSP coverage.
	logrus.Info("Waiting until view registry is ready")
	// It's safe to ignore the returned error: the only error from
	// WaitForStable() is ctx.Err(), and context.Background() won't ever expire.
	_ = client.Registry.WaitForStable(context.Background(), 5*time.Second)
	summary := client.Registry.Summary()
	logrus.WithFields(summary).Info("Assuming view registry is ready")

	s := clusterSource{
		opts:     o,
		cfg:      cfg,
		client:   client,
		viewPred: makeViewInfoPredicate(o.exclude),
	}
	return &s, err
}

func makeViewInfoPredicate(exclude string) viewclient.ViewInfoPredicate {
	exclude = strings.TrimSpace(exclude)
	if exclude == "" {
		return func(viewreg.View) bool {
			return true
		}
	}
	hosts := strings.Split(exclude, ",")
	for i := range hosts {
		hosts[i] = strings.TrimSpace(hosts[i])
	}
	return func(v viewreg.View) bool {
		for _, h := range hosts {
			if strings.Contains(v.Endpoint.HostPort(), h) {
				return false
			}
		}
		return true
	}
}

func (s *clusterSource) status() error {
	cc := s.client.CarouselViews(s.viewPred)
	ls, err := s.client.LogStatus(context.Background(), cc)
	if err != nil {
		return err
	}
	lastApplied, _ := ls.LastApplied()
	replayPos, _ := ls.ReplayPosition()
	fmtr.Println("Overall")
	fmtr.Printf("AtIndex:         %*d\n", len(replayPos.String()), lastApplied)
	fmtr.Printf("Replay Position: %s\n", replayPos)
	if !s.opts.quiet {
		fmtr.Printf("\nBy View\n")
		t := make([][]string, len(cc)+1)
		t[0] = []string{"View", "Next Position", "Replay Position", "Error"}
		res, err := ls.Results()
		for i := range cc {
			if err[i] != nil {
				t[i+1] = []string{
					cc[i].View.String(),
					"", "",
					err[i].Error()}
			} else {
				t[i+1] = []string{
					cc[i].View.String(),
					fmtr.Sprintf("%v", res[i].NextPosition.Index),
					fmtr.Sprintf("%v", res[i].ReplayPosition.Index),
					""}
			}
		}
		table.PrettyPrint(os.Stdout, t, table.HeaderRow|table.RightJustify)
	}
	return nil
}

func (s *clusterSource) ride(partition, numPartitions int) error {
	req := rpc.CarouselRequest{
		Dedicated:     s.opts.dedicated,
		HashPartition: partitioning.CarouselHashPartition(partitioning.NewHashSubjectPredicatePartition(partition, numPartitions)),
	}
	sink, err := newRideSink(s.opts)
	if err != nil {
		return err
	}
	defer sink.close()

	cc, err := s.client.RideCarousel(context.Background(), &req, s.viewPred)
	if err != nil {
		return err
	}
	for {
		select {
		case next, ok := <-cc.DataCh():
			if !ok {
				sink.printResults()
				return nil
			}
			sink.add(next)

		case err := <-cc.ErrorsCh():
			return err
		}
	}
}
