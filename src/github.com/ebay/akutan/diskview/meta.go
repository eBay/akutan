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

package diskview

import (
	"fmt"
	"strings"
)

func (gm *GraphMeta) String() string {
	if gm == nil {
		return "nil"
	}
	s := strings.Join([]string{`&GraphMeta{`,
		`FactCount:` + fmt.Sprintf("%v", gm.FactCount) + `,`,
		`FactVersions:` + fmt.Sprintf("%v", gm.FactVersions) + `,`,
		`Stats:` + fmt.Sprintf("{Subjects: %v, SubjectPredicates: %v, "+
			"Predicates: %v, PredicateObjects: %v}, ",
			len(gm.Stats.Subjects),
			len(gm.Stats.SubjectPredicates),
			len(gm.Stats.Predicates),
			len(gm.Stats.PredicateObjects)),
		`AsOf:` + strings.Replace(strings.Replace(gm.AsOf.String(), "Timestamp", "google_protobuf1.Timestamp", 1), `&`, ``, 1) + `,`,
		`}`,
	}, "")
	return s
}
