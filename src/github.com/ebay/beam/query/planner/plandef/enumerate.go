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

package plandef

import (
	"fmt"
	"strings"
)

// An Enumerate Operator emits a literal set of values.
type Enumerate struct {
	// Output is a Variable or Binding that the values are associated with in
	// the results
	Output Term
	// Only Literals or OIDs are allowed in the list (Bindings).
	Values []FixedTerm
}

func (*Enumerate) anOperator() {}

// String returns a string like "Enumerate ?foo {a, b, c}".
// If the set of values is too large, it returns something like
// "Enumerate ?foo {a, b, c, ...}" instead.
func (op *Enumerate) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Enumerate %v {", op.Output.String())
	for i, v := range op.Values {
		if i == 3 && len(op.Values) > 4 {
			fmt.Fprintf(&b, ", ...")
			break
		}
		if i == 0 {
			fmt.Fprintf(&b, "%v", v.String())
		} else {
			fmt.Fprintf(&b, ", %v", v.String())
		}
	}
	fmt.Fprintf(&b, "}")
	return b.String()
}

// Key implements cmp.Key.
func (op *Enumerate) Key(b *strings.Builder) {
	b.WriteString("Enumerate ")
	op.Output.Key(b)
	for _, v := range op.Values {
		b.WriteByte(' ')
		v.Key(b)
	}
}
