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

// Package bytes aids in manipulating byte slices and writing bytes and strings.
package bytes

// Copy will return a new byte slice with a copy of 'src's contents
func Copy(src []byte) []byte {
	if src == nil {
		return nil
	}
	r := make([]byte, len(src))
	copy(r, src)
	return r
}

// StringWriter defines an abstract set of functions used when writing strings.
// bytes.Buffer, bufio.Writer & strings.Builder all implement this interface.
//
// Although these methods expose the standard Go writer semantics of returning
// errors, users of StringWriter generally do not check for errors. Constructors
// of this interface should arrange to detect errors externally if needed (e.g.
// by checking the result of Flush on a bufio.Writer).
type StringWriter interface {
	Write([]byte) (int, error)
	WriteByte(byte) error
	WriteRune(r rune) (int, error)
	WriteString(s string) (int, error)
}
