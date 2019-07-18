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
	"fmt"

	"github.com/ebay/akutan/api"
)

// ToAPIFact converts an internal Fact representation into the
// format used in the external API. The difference between the
// two is around the type of the Object field.
func (f *Fact) ToAPIFact() api.ResolvedFact {
	return api.ResolvedFact{
		Index:     int64(f.Index),
		Id:        f.Id,
		Subject:   f.Subject,
		Predicate: f.Predicate,
		Object:    f.Object.ToAPIObject(),
	}
}

func (f Fact) String() string {
	return fmt.Sprintf("{idx:%d id:%d s:%d p:%d o:%v}", f.Index, f.Id, f.Subject, f.Predicate, f.Object)
}
