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

// Command gen-local writes out files used to run a Beam cluster locally.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ebay/beam/config"
	"github.com/ebay/beam/tools/gen-local/gen"
	"github.com/ebay/beam/util/debuglog"
	"github.com/ebay/beam/util/errors"
	log "github.com/sirupsen/logrus"
)

var options struct {
	cfg              string
	out              string
	hashSPPartitions int
	hashPOPartitions int
	replicas         int
	firstAPIPort     int
	firstViewPort    int
}

func main() {
	debuglog.Configure(debuglog.Options{})
	flag.StringVar(&options.cfg, "cfg", "config.json", "Beam config file to use as template")
	flag.StringVar(&options.out, "out", "local/generated", "Dir for output files")
	flag.IntVar(&options.hashPOPartitions, "hashpo", 3, "Number of hash-PO partitions without replication")
	flag.IntVar(&options.hashSPPartitions, "hashsp", 2, "Number of hash-SP partitions without replication")
	flag.IntVar(&options.replicas, "replicas", 1, "Instances of each service or view partition to run")
	flag.IntVar(&options.firstAPIPort, "api-base-port", 9987, "Assign port numbers to API servers starting here (GRPC then HTTP)")
	flag.IntVar(&options.firstViewPort, "view-base-port", 9950, "Assign port numbers to views starting here (GRPC then HTTP)")
	flag.Parse()

	// Create cluster spec.
	baseCfg, err := config.Load(options.cfg)
	if err != nil {
		panic(fmt.Sprintf("Unable to load base configuration: %v", err))
	}
	numViews := (options.replicas +
		options.hashPOPartitions*options.replicas +
		options.hashSPPartitions*options.replicas)
	spec := gen.Spec{
		BaseCfg: baseCfg,
		API: gen.APISpec{
			Replicas:  options.replicas,
			GRPCPorts: enumeratePorts(options.replicas, options.firstAPIPort, 2),
			HTTPPorts: enumeratePorts(options.replicas, options.firstAPIPort+1, 2),
		},
		Views: gen.ViewsSpec{
			GRPCPorts: enumeratePorts(numViews, options.firstViewPort, 2),
			HTTPPorts: enumeratePorts(numViews, options.firstViewPort+1, 2),
		},
		TxTimeout: gen.TxTimeoutSpec{
			Replicas: options.replicas,
		},
		HashPO: gen.HashPOSpec{
			Partitions: options.hashPOPartitions,
			Replicas:   options.replicas,
		},
		HashSP: gen.HashSPSpec{
			Partitions: options.hashSPPartitions,
			Replicas:   options.replicas,
		},
	}

	// Generate cluster processes.
	cluster, err := spec.Generate()
	if err != nil {
		log.Fatalf("Unable to generate local cluster configuration: %v", err)
	}
	// Patch up DiskView.Dir.
	for _, diskView := range cluster.Filter(gen.DiskViewProcess) {
		base := diskView.Cfg.DiskView.Dir
		if base == "" {
			base = os.TempDir()
		}
		diskView.Cfg.DiskView.Dir = filepath.Join(base,
			fmt.Sprintf("rocksdb-%s", diskView.Name))
	}

	// Clear the output directory.
	err = os.MkdirAll(options.out, 0755)
	if err != nil {
		log.Fatalf("Failed to create directory %s: %s", options.out, err)
	}
	files, err := filepath.Glob(filepath.Join(options.out, "*.json"))
	if err != nil {
		log.Fatalf("Failed to glob directory %s: %s", options.out, err)
	}
	for _, file := range files {
		err := os.Remove(file)
		if err != nil {
			log.Fatalf("Failed to remove %s: %s", file, err)
		}
	}

	// Write out the generated files.
	for _, proc := range cluster {
		proc.CfgPath = filepath.Join(options.out, fmt.Sprintf("%s.json", proc.Name))
		err = config.Write(proc.Cfg, proc.CfgPath)
		if err != nil {
			log.Fatalf("Failed to write config file: %s", err)
		}
	}
	err = writeProcfile(filepath.Join(options.out, "Procfile"), cluster)
	if err != nil {
		log.Fatalf("Failed to create Procfile: %v", err)
	}
}

// writeProcfile generates a Procfile and writes it into 'path'. It assumes the
// processes have their CfgPaths already set.
func writeProcfile(path string, cluster gen.Cluster) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	w := bufio.NewWriter(file)
	fmt.Fprintf(w, "# This file was automatically generated. DO NOT EDIT.\n")
	fmt.Fprintf(w, "# Run it with goreman. See https://github.com/mattn/goreman\n")
	for _, proc := range cluster {
		shortName := strings.TrimPrefix(proc.Name, "beam-")
		shortName = strings.TrimPrefix(shortName, "diskview-")
		fmt.Fprintf(w, "%s: bin/%s --cfg %s\n",
			shortName, proc.Type, proc.CfgPath)
	}
	return errors.Any(
		w.Flush(),
		file.Close(),
	)
}

// Returns n host:ports, where the host is "localhost" and the ports count up
// from start by skip.
func enumeratePorts(n int, start int, skip int) []string {
	ports := make([]string, n)
	for i := range ports {
		ports[i] = fmt.Sprintf("localhost:%d", start+i*skip)
	}
	return ports
}
