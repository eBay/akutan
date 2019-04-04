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

// +build debug

package parser

import (
	"os"

	"github.com/vektah/goparsify"
)

func init() {
	// If you need to debug what the parser is doing, you can enable goparsify's
	// built in debug support by building with -tags debug and calling
	// EnableLogging(os.Stdout). See the docs for more details
	// https://github.com/vektah/goparsify#debugging-parsers
	//
	// This file has a debug build tag, so we'll automatically enable the parser
	// debug output logging when you build with -tags debug.
	goparsify.EnableLogging(os.Stdout)
}
