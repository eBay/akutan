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

// Command generate writes out Kubernetes configuration for portions of the Akutan
// cluster.  The diskview, persistent volume and persistent volume claims
// require an amount of boilerplate copy and paste based upon the number of
// partitions, replicas and keyspaces.
//
// Template Dir: cluster/k8s
// Output Dir: cluster/k8s/generated
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"github.com/ebay/akutan/config"
	"github.com/ebay/akutan/tools/gen-local/gen"
)

const (
	logServerCount = 1
	templateRoot   = "cluster/k8s"
	outputRoot     = "cluster/k8s/generated"
)

var options struct {
	cfgFile          string
	akutanImagesPath string
	akutanImagesTag  string
	partitions       int
	replicas         int
}

func main() {
	cluster := &cluster{}
	flag.StringVar(&options.cfgFile, "cfg", "cluster/k8s/config.json",
		"Akutan config file to use as template")
	flag.StringVar(&options.akutanImagesPath, "akutan-images-path", "",
		"Prefix to the core Docker images. If empty, uses locally built images.")
	flag.StringVar(&options.akutanImagesTag, "akutan-images-tag", "latest",
		"Tag to use for core Docker images")
	flag.StringVar(&cluster.Images.Jaeger, "jaeger-image",
		"jaegertracing/all-in-one:1.9",
		"Path to Jaeger all-in-one Docker image")
	flag.StringVar(&cluster.Images.LogService, "logservice-image",
		"akutan-kafka:latest",
		"Path to Log service Docker image")
	flag.BoolVar(&cluster.UsePersistentVolumes, "pv", false,
		"Use persistent volume claims and persistent volumes for Log service and Disk Views")
	flag.IntVar(&options.partitions, "partitions", 2,
		"Number of partitions of each DiskView hash-space")
	flag.IntVar(&options.replicas, "replicas", 1,
		"Number of replicas of each DiskView partition")
	flag.Parse()

	cluster.Images.API = joinImagePath(options.akutanImagesPath, "akutan-api", options.akutanImagesTag)
	cluster.Images.DiskView = joinImagePath(options.akutanImagesPath, "akutan-diskview", options.akutanImagesTag)
	cluster.Images.TxView = joinImagePath(options.akutanImagesPath, "akutan-txview", options.akutanImagesTag)

	baseCfg, err := config.Load(options.cfgFile)
	if err != nil {
		panic(fmt.Sprintf("Unable to load base configuration: %v", err))
	}

	numViews := 1 + options.partitions*options.replicas*2
	spec := gen.Spec{
		BaseCfg: baseCfg,
		API: gen.APISpec{
			Replicas: 1,
		},
		Views: gen.ViewsSpec{
			GRPCPorts: repeat(":9980", numViews),
			HTTPPorts: repeat(":9981", numViews),
		},
		TxTimeout: gen.TxTimeoutSpec{
			Replicas: 1,
		},
		HashPO: gen.HashPOSpec{
			Partitions: options.partitions,
			Replicas:   options.replicas,
		},
		HashSP: gen.HashSPSpec{
			Partitions: options.partitions,
			Replicas:   options.replicas,
		},
	}
	processes, err := spec.Generate()
	if err != nil {
		panic(fmt.Sprintf("Unable to generate local cluster configuration: %v", err))
	}

	// Fill in template variables.
	for _, proc := range processes.Filter(gen.DiskViewProcess) {
		cluster.DiskViews = append(cluster.DiskViews, diskView{
			Process: proc,
			Volume: volume{
				Name:  proc.Name,
				Claim: proc.Name,
			},
		})
	}
	cluster.APICfg = processes.Filter(gen.APIProcess)[0].Cfg
	cluster.TxViewCfg = processes.Filter(gen.TxTimerProcess)[0].Cfg
	cluster.LogServers = makeLogServers()

	// Make sure the directory outputRoot exists and has no children.
	err = os.RemoveAll(outputRoot)
	if err != nil {
		panic(fmt.Sprintf("Failed to remove directory %v: %v", outputRoot, err))
	}
	err = os.MkdirAll(outputRoot, 0755)
	if err != nil {
		panic(fmt.Sprintf("Failed to create directory %v: %v", outputRoot, err))
	}

	// Generate ${outputRoot}/%.yaml from ${templateRoot}/%.tpl.yaml.
	files, err := ioutil.ReadDir(templateRoot)
	if err != nil {
		panic(fmt.Sprintf("Failed to list template directory '%v': %v", templateRoot, err))
	}
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".tpl.yaml") {
			err := execTemplate(
				filepath.Join(templateRoot, file.Name()),
				filepath.Join(outputRoot, strings.TrimSuffix(file.Name(), ".tpl.yaml")+".yaml"),
				cluster)
			if err != nil {
				panic(fmt.Sprintf("Failed to execute template %v: %v", file.Name(), err))
			}
		}
	}
}

// repeat returns a slice of length 'n' with each element equal to 'x'.
func repeat(x string, n int) []string {
	slice := make([]string, n)
	for i := range slice {
		slice[i] = x
	}
	return slice
}

func makeLogServers() []logServer {
	logServers := make([]logServer, logServerCount)
	for ls := 0; ls < logServerCount; ls++ {
		logServers[ls] = logServer{
			Number: ls + 1,
			Count:  logServerCount,
		}
		logServers[ls].Volume = volume{
			Name:  logServers[ls].Name(),
			Claim: logServers[ls].Name(),
		}
	}
	return logServers
}

// execTemplate reads the file inputName, evaluates it, and writes the result to
// outputName.
func execTemplate(inputName, outputName string, data interface{}) error {
	input, err := ioutil.ReadFile(inputName)
	if err != nil {
		return err
	}
	tmpl := template.New(filepath.Base(inputName))
	tmpl.Funcs(map[string]interface{}{
		"join": strings.Join,
		"json": func(indent string, v interface{}) (string, error) {
			data, err := json.MarshalIndent(v, indent, "  ")
			if err != nil {
				return "", fmt.Errorf("json.Marshal failed in %s: %s", inputName, err)
			}
			return string(data), nil
		},
	})
	tmpl, err = tmpl.Parse(string(input))
	if err != nil {
		return err
	}
	outFile, err := os.Create(outputName)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(outFile)
	fmt.Fprintf(w, "# This file was automatically generated on %s from %s.\n",
		time.Now().UTC().Format("2006-01-02 15:04:05 MST"),
		inputName)
	err = tmpl.Execute(w, data)
	if err != nil {
		outFile.Close()
		return err
	}
	err = w.Flush()
	if err != nil {
		return err
	}
	return outFile.Close()
}

// joinImagePath returns a fully-qualified path to a Docker image. base should
// contain no slashes or colons. path is the prefix leading up to base. path and
// tag are optional.
func joinImagePath(path, base, tag string) string {
	var b strings.Builder
	if path != "" {
		b.WriteString(path)
		if !strings.HasSuffix(path, "/") {
			b.WriteString("/")
		}
	}
	b.WriteString(base)
	if tag != "" {
		b.WriteString(":")
		b.WriteString(tag)
	}
	return b.String()
}

// cluster is the object handed to the Kubernetes YAML templates.
type cluster struct {
	// Fully-qualified paths to Docker images, optionally including tags.
	Images struct {
		API        string
		DiskView   string
		Jaeger     string
		LogService string
		TxView     string
	}
	DiskViews []diskView
	// The current templates only handle one API server an one Tx Timer View.
	APICfg               *config.Akutan
	TxViewCfg            *config.Akutan
	LogServers           []logServer
	UsePersistentVolumes bool
}

func (c *cluster) Volumes() []volume {
	if !c.UsePersistentVolumes {
		return nil
	}
	volumes := make([]volume, len(c.LogServers)+len(c.DiskViews))
	i := 0
	for _, ls := range c.LogServers {
		volumes[i] = ls.Volume
		i++
	}
	for _, dv := range c.DiskViews {
		volumes[i] = dv.Volume
		i++
	}
	return volumes
}

type diskView struct {
	*gen.Process
	Volume volume
}

type logServer struct {
	Number int
	Count  int
	Volume volume
}

func (ls logServer) Name() string {
	return fmt.Sprintf("log-store-%d", ls.Number)
}

type volume struct {
	Name  string
	Claim string
}
