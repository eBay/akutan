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

// Package gen is used in generating configurations for an entire Beam cluster.
// It's used by the commands gen-local and gen-kube.
package gen

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/ebay/beam/config"
	"github.com/ebay/beam/partitioning"
	"github.com/ebay/beam/space"
)

// A Spec specifies the parameters of the cluster. It does not describe the Beam
// log or other dependencies; it only describes the Beam servers.
type Spec struct {
	// A configuration to copy as the basis of each server's configuration. This
	// allows users to pass through various configuration options that this
	// package does not need to know about.
	BaseCfg *config.Beam

	// Parameters for various aspects of the cluster.
	API       APISpec
	Views     ViewsSpec
	TxTimeout TxTimeoutSpec
	HashPO    HashPOSpec
	HashSP    HashSPSpec
}

// An APISpec specifies the parameters of the API servers.
type APISpec struct {
	// The number of API servers.
	Replicas int
	// Each API server is assigned an address on which to serve gRPC requests
	// from this list.
	GRPCPorts []string
	// Each API server is assigned an address on which to serve HTTP requests
	// from this list.
	HTTPPorts []string
}

// A ViewsSpec specifies the parameters of the view servers.
type ViewsSpec struct {
	// Each view server is assigned an address on which to serve gRPC requests
	// from this list.
	GRPCPorts []string
	// Each view server is assigned an address on which to serve its metrics
	// from this list.
	HTTPPorts []string
}

// A TxTimeoutSpec specifies the parameters of the transaction timeout view
// servers.
type TxTimeoutSpec struct {
	// The number of TxTimeout servers.
	Replicas int
}

// A HashPOSpec specifies the parameters of the disk view servers for the
// hash(predicate+object) space.
type HashPOSpec struct {
	// The number of ways to split the HashPO space.
	Partitions int
	// The number of HashPO disk view servers per partition.
	Replicas int
}

// A HashSPSpec specifies the parameters of the disk view servers for the
// hash(subject+predicate) space.
type HashSPSpec struct {
	// The number of ways to split the HashSP space.
	Partitions int
	// The number of HashSP disk view servers per partition.
	Replicas int
}

// A Cluster is the output of Spec.Generate. It has one entry for every Beam
// server, in some reasonable order. It does not describe the Beam log or other
// dependencies; it only describes the Beam servers.
type Cluster []*Process

// A Process describes one Beam server.
type Process struct {
	// A human-readable name for the process, like "beam-api-01".
	Name string
	// The role of the server.
	Type ProcessType
	// The dedicated configuration for this server.
	Cfg *config.Beam
	// This is not set by this module but is convenient for users. If non-nil,
	// it is the filepath to where 'Cfg' should be written.
	CfgPath string
}

// ProcessType is used in Process.Type to specify the role of the server. The
// string values are the same as the names of the executable binaries (like
// "beam-api").
type ProcessType string

// Possible values for ProcessType.
const (
	APIProcess      ProcessType = "beam-api"
	DiskViewProcess ProcessType = "beam-diskview"
	TxTimerProcess  ProcessType = "beam-txview"
)

// Generate produces a Cluster based on the given spec. If it cannot, it returns
// an error.
func (spec *Spec) Generate() (Cluster, error) {
	numViews := spec.TxTimeout.Replicas +
		spec.HashPO.Partitions*spec.HashPO.Replicas +
		spec.HashSP.Partitions*spec.HashSP.Replicas
	if len(spec.Views.GRPCPorts) != numViews {
		return nil, fmt.Errorf("need %v gRPC ports, got %v",
			numViews, len(spec.Views.GRPCPorts))
	}
	if len(spec.Views.HTTPPorts) != numViews {
		return nil, fmt.Errorf("need %v HTTP ports, got %v",
			numViews, len(spec.Views.HTTPPorts))
	}

	// Create all the Processes.
	viewCounter := 0
	var processes []*Process
	for _, fn := range []func(viewCounter *int) ([]*Process, error){
		spec.generateAPIs,
		spec.generateTxTimeoutViews,
		spec.generateHashSPViews,
		spec.generateHashPOViews,
	} {
		p, err := fn(&viewCounter)
		if err != nil {
			return nil, err
		}
		processes = append(processes, p...)
	}

	// Fill in the GRPC addresses of all the other views in every Process.
	var viewGRPCAddresses []string
	for _, proc := range processes {
		switch proc.Type {
		case TxTimerProcess:
			viewGRPCAddresses = append(viewGRPCAddresses,
				proc.Cfg.TxTimeoutView.GRPCAddress)
		case DiskViewProcess:
			viewGRPCAddresses = append(viewGRPCAddresses,
				proc.Cfg.DiskView.GRPCAddress)
		}
	}
	for _, proc := range processes {
		if proc.Cfg.ViewsLocator.Type == "" {
			proc.Cfg.ViewsLocator = config.Locator{Type: "static"}
		}
		if proc.Cfg.ViewsLocator.Type == "static" {
			proc.Cfg.ViewsLocator.Addresses = viewGRPCAddresses
		}
	}

	return processes, nil
}

// generateAPIs is a helper to Generate that produces Processes of type
// "beam-api".
func (spec *Spec) generateAPIs(viewCounter *int) ([]*Process, error) {
	var processes []*Process
	for replica := 0; replica < spec.API.Replicas; replica++ {
		cfg := copyConfig(spec.BaseCfg)
		cfg.DiskView = nil
		cfg.TxTimeoutView = nil
		if cfg.API == nil {
			cfg.API = new(config.API)
		}
		if cfg.API.GRPCAddress == "" {
			cfg.API.GRPCAddress = spec.API.GRPCPorts[replica]
		}
		if cfg.API.HTTPAddress == "" {
			cfg.API.HTTPAddress = spec.API.HTTPPorts[replica]
		}
		processes = append(processes, &Process{
			Type: APIProcess,
			Name: fmt.Sprintf("%s-%02d", APIProcess, replica),
			Cfg:  cfg,
		})
	}
	if len(processes) == 0 {
		return nil, fmt.Errorf("need at least one API server")
	}
	return processes, nil
}

// generateTxTimeoutViews is a helper to Generate that produces Processes of
// type "beam-txview.
func (spec *Spec) generateTxTimeoutViews(viewCounter *int) ([]*Process, error) {
	var processes []*Process
	for replica := 0; replica < spec.TxTimeout.Replicas; replica++ {
		cfg := copyConfig(spec.BaseCfg)
		cfg.API = nil
		cfg.DiskView = nil
		if cfg.TxTimeoutView == nil {
			cfg.TxTimeoutView = new(config.TxTimeoutView)
		}
		if cfg.TxTimeoutView.GRPCAddress == "" {
			cfg.TxTimeoutView.GRPCAddress = spec.Views.GRPCPorts[*viewCounter]
		}
		if cfg.TxTimeoutView.MetricsAddress == "" {
			cfg.TxTimeoutView.MetricsAddress = spec.Views.HTTPPorts[*viewCounter]
		}
		*viewCounter++
		processes = append(processes, &Process{
			Type: TxTimerProcess,
			Name: fmt.Sprintf("%s-%02d", TxTimerProcess, replica),
			Cfg:  cfg,
		})
	}
	if len(processes) == 0 {
		return nil, fmt.Errorf("need at least one TxTimeoutView")
	}
	return processes, nil
}

// generateHashPOViews is a helper to Generate that produces Processes of type
// "beam-diskview" in the hashPO space.
func (spec *Spec) generateHashPOViews(viewCounter *int) ([]*Process, error) {
	var processes []*Process
	count := 0
	for replica := 0; replica < spec.HashPO.Replicas; replica++ {
		for part := 0; part < spec.HashPO.Partitions; part++ {
			cfg := spec.diskViewCfg(viewCounter)
			cfg.DiskView.Space = "hashpo"
			cfg.DiskView.FirstHash, cfg.DiskView.LastHash =
				hashRange(part, spec.HashPO.Partitions)
			processes = append(processes, &Process{
				Type: DiskViewProcess,
				Name: fmt.Sprintf("%s-hashpo-%02d", DiskViewProcess, count),
				Cfg:  cfg,
			})
			count++
		}
	}
	if len(processes) == 0 {
		return nil, fmt.Errorf("need at least one HashPO DiskView instance")
	}
	return processes, nil
}

// generateHashSPViews is a helper to Generate that produces Processes of type
// "beam-diskview" in the hashSP space.
func (spec *Spec) generateHashSPViews(viewCounter *int) ([]*Process, error) {
	var processes []*Process
	count := 0
	for replica := 0; replica < spec.HashSP.Replicas; replica++ {
		for part := 0; part < spec.HashSP.Partitions; part++ {
			cfg := spec.diskViewCfg(viewCounter)
			cfg.DiskView.Space = "hashsp"
			cfg.DiskView.FirstHash, cfg.DiskView.LastHash =
				hashRange(part, spec.HashSP.Partitions)
			processes = append(processes, &Process{
				Type: DiskViewProcess,
				Name: fmt.Sprintf("%s-hashsp-%02d", DiskViewProcess, count),
				Cfg:  cfg,
			})
			count++
		}
	}
	if len(processes) == 0 {
		return nil, fmt.Errorf("need at least one HashSP DiskView instance")
	}
	return processes, nil
}

// diskViewCfg returns a dedicated config with parameters common to all disk
// views already filled in.
func (spec *Spec) diskViewCfg(viewCounter *int) *config.Beam {
	cfg := copyConfig(spec.BaseCfg)
	cfg.API = nil
	cfg.TxTimeoutView = nil
	if cfg.DiskView == nil {
		cfg.DiskView = new(config.DiskView)
	}
	if cfg.DiskView.GRPCAddress == "" {
		cfg.DiskView.GRPCAddress = spec.Views.GRPCPorts[*viewCounter]
	}
	if cfg.DiskView.MetricsAddress == "" {
		cfg.DiskView.MetricsAddress = spec.Views.HTTPPorts[*viewCounter]
	}
	*viewCounter++
	return cfg
}

// Filter returns the processes in the cluster that have a Type matching
// 'procType'.
func (cluster Cluster) Filter(procType ProcessType) []*Process {
	var processes []*Process
	for _, proc := range cluster {
		if proc.Type == procType {
			processes = append(processes, proc)
		}
	}
	return processes
}

// copyConfig returns a deep copy of the given configuration.
func copyConfig(in *config.Beam) *config.Beam {
	bytes, err := json.Marshal(in)
	if err != nil {
		panic(fmt.Sprintf("Failed to marshal Beam config for copy: %v", err))
	}
	out := new(config.Beam)
	err = json.Unmarshal(bytes, out)
	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal Beam config for copy: %v", err))
	}
	return out
}

// hashRange splits a 64-bit hash range into n approximately even partitions,
// and returns the p-th range (counting from 0). The 'first' and 'last' values
// returned are formatted 0x%016x and represent the range from 'first' up
// through and including 'last'.
func hashRange(p, n int) (first, last string) {
	r := partitioning.NewHashPredicateObjectPartition(p, n).HashRange()
	uFirst := uint64(r.Start.(space.Hash64))
	var uLast uint64
	switch end := r.End.(type) {
	case space.Hash64:
		if end == 0 {
			panic("hash range must not end at 0")
		}
		uLast = uint64(end) - 1
	case space.InfinitePoint:
		uLast = math.MaxUint64
	default:
		panic(fmt.Sprintf("Unexpected point type %T", r.End))
	}
	return fmt.Sprintf("0x%016x", uFirst),
		fmt.Sprintf("0x%016x", uLast)
}
