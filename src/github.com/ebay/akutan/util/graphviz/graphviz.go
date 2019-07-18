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

// Package graphviz generates diagrams from dot input.
package graphviz

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	log "github.com/sirupsen/logrus"
)

// Filetype is the file format of output image.
type Filetype int

// Supported Filetypes.
const (
	PDF Filetype = 1
	PNG Filetype = 2
	SVG Filetype = 3
)

// Options to Create.
type Options struct {
	// Unless provided, Create will attempt to autodetect this from the filename.
	Filetype Filetype
}

// Create writes an image file from a Graphviz spec. It invokes the "dot"
// program internally. 'generate' should write the Graphviz spec into the given
// writer; it may safely ignore errors from the writer.
func Create(filename string, generate func(io.Writer), options Options) error {
	if options.Filetype == 0 {
		split := strings.Split(filename, ".")
		var suffix string
		if len(split) > 1 {
			suffix = strings.ToLower(split[len(split)-1])
		}
		switch suffix {
		case "pdf":
			options.Filetype = PDF
		case "png":
			options.Filetype = PNG
		case "svg":
			options.Filetype = SVG
		default:
			return fmt.Errorf("could not determine filetype from filename: %v", filename)
		}
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	cmd := exec.Command("dot")
	switch options.Filetype {
	case PDF:
		cmd.Args = append(cmd.Args, "-Tpdf")
	case PNG:
		cmd.Args = append(cmd.Args, "-Tpng")
	case SVG:
		cmd.Args = append(cmd.Args, "-Tsvg")
	default:
		log.Panicf("Unknown file type: %v", options.Filetype)
	}
	cmd.Stdout = file
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	go func() {
		defer stdin.Close()
		generate(stdin)
	}()
	var errOut strings.Builder
	cmd.Stderr = &errOut
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("error executing dot. Stderr: %v", errOut.String())
	}
	return nil
}
