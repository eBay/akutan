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

package rpc

import (
	"io"

	"github.com/ebay/akutan/util/cmp"
)

// functions in this package are to support custom marshaling of KGObject over protobuf/gRPC
// As the underlying value is the encoded version of the Object, this is most about just copying
// the data into the protobuf types.
//
// One difference is for KtNil, in go this is the zero value of the KGObject type (i.e. KGObject{value:""})
// but for marshalling/persistence we want to explicitly record it's nilness, so we marshal that as
// []byte{ktNil}

// Size returns the number of bytes required to serialize an instance of this KGObject
func (o *KGObject) Size() int {
	return cmp.MaxInt(1, len(o.value))
}

// Marshal will return the bytes required to later unmarshal an instance of this KGObject
// back into the same value. This never returns an error.
func (o *KGObject) Marshal() ([]byte, error) {
	d := make([]byte, o.Size())
	_, err := o.MarshalTo(d)
	return d, err
}

// MarshalTo will copy a serialized version of this KGObject into the supplied 'data' byte slice.
// The number of bytes copied are returned, if the supplied slice is too small, it'll end up with
// just the prefix that fits. This never returns an error.
func (o *KGObject) MarshalTo(data []byte) (n int, err error) {
	if o.value == "" {
		if len(data) > 0 {
			data[0] = byte(KtNil)
			return 1, nil
		}
		return 0, nil
	}
	n += copy(data, o.value)
	return n, nil
}

// Unmarshal will update this KGObject with marshaled value in the supplied 'data' bytes.
func (o *KGObject) Unmarshal(data []byte) error {
	if len(data) == 0 {
		return io.ErrUnexpectedEOF
	}
	if data[0] == byte(KtNil) {
		o.value = ""
	} else {
		o.value = string(data)
	}
	return nil
}
