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
	"io/ioutil"
	"os"
	"time"

	"github.com/ebay/akutan/api"
	log "github.com/sirupsen/logrus"
)

// insert invokes the store.Insert RPC.
func insert(ctx context.Context, store api.FactStoreClient, options *options) error {
	input, err := readFile(options.Filename)
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(ctx, options.Timeout)
	defer cancelFunc()
	req := api.InsertRequest{
		Format: options.Format,
		Facts:  input,
	}
	log.WithFields(log.Fields{
		"format":   options.Format,
		"filename": options.Filename,
		"bytes":    len(input),
	}).Info("Invoking Insert RPC")
	start := time.Now()
	resp, err := store.Insert(ctx, &req)
	if err != nil {
		return err
	}
	log.Infof("Insert returned: %+v", resp)
	log.Infof("Insert took %s", time.Since(start))
	return nil
}

// readFile returns the contents of the file as a string. filename may be "-" to
// read from stdin.
func readFile(filename string) (string, error) {
	if filename == "-" {
		input, err := ioutil.ReadAll(os.Stdin)
		return string(input), err
	}
	input, err := ioutil.ReadFile(filename)
	return string(input), err
}
