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
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_KidPrinterFormat(t *testing.T) {
	printer := kidPrinter{kidToXID: map[uint64]string{
		1: "Brand",
		2: "rdf:type",
	}}

	var tests = []struct {
		name string
		in   uint64
		exp  string
	}{
		{"kid_maps_to_entity", 1, "1: <Brand>"},
		{"kid_maps_to_qname", 2, "2: rdf:type"},
		{"kid_no_mapping", 3, "3"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.exp, printer.format(test.in))
		})
	}
}
