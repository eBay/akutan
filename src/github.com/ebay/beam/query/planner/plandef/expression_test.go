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
	"testing"

	"github.com/ebay/beam/query/parser"
	"github.com/ebay/beam/util/cmp"
	"github.com/stretchr/testify/assert"
)

func Test_ExprBindingStringer(t *testing.T) {
	e := &ExprBinding{
		Expr: &AggregateExpr{Func: parser.AggCount, Of: WildcardExpr{}},
		Out:  &Variable{"y"},
	}
	assert.Equal(t, "(COUNT(*) AS ?y)", e.String())
	assert.Equal(t, "bind(COUNT(*) ?y)", cmp.GetKey(e))
}

func Test_AggregateExprStringer(t *testing.T) {
	e := &AggregateExpr{
		Func: parser.AggCount,
		Of:   WildcardExpr{},
	}
	assert.Equal(t, "COUNT(*)", e.String())
	assert.Equal(t, "COUNT(*)", cmp.GetKey(e))
	e.Of = &Variable{"foo"}
	assert.Equal(t, "COUNT(?foo)", e.String())
	assert.Equal(t, "COUNT(?foo)", cmp.GetKey(e))
}
