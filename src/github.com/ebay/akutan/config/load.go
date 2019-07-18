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

package config

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/ebay/akutan/util/errors"
)

// Load parses the configuration from the given JSON file. Upon success, it
// returns a non-nil configuration. Otherwise, it returns an error, which
// already includes the filename.
func Load(filename string) (*Akutan, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	cfg := new(Akutan)
	// This **Akutan double-pointer appears to be required to detect an invalid
	// input of "null". See Test_Load/file_contains_null test.
	err = decoder.Decode(&cfg)
	if err != nil {
		return nil, fmt.Errorf("error decoding JSON value in %v: %v", filename, err)
	}
	if cfg == nil {
		return nil, fmt.Errorf("loading %v resulted in nil config", filename)
	}
	if decoder.More() {
		return nil, fmt.Errorf("found unexpected data after config in %v", filename)
	}
	return cfg, nil
}

// Write marshalls the configuration as JSON to the given file. It truncates the
// file if it already exists. It returns nil upon success. Otherwise, it returns
// an error, which already includes the filename.
func Write(cfg *Akutan, filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	writer := bufio.NewWriter(f)
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "\t")
	err = errors.Any(
		encoder.Encode(cfg),
		writer.Flush(),
		f.Close(),
	)
	if err != nil {
		return fmt.Errorf("failed to write %v: %v", filename, err)
	}
	return nil
}
