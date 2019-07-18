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
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Load(t *testing.T) {
	dir, err := ioutil.TempDir("", "config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	t.Run("file not found", func(t *testing.T) {
		_, err = Load(filepath.Join(dir, "404.json"))
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "404.json")
		}
	})

	t.Run("file contains garbage", func(t *testing.T) {
		err = ioutil.WriteFile(filepath.Join(dir, "garbage.json"), []byte("koala"), 0644)
		require.NoError(t, err)
		_, err = Load(filepath.Join(dir, "garbage.json"))
		if assert.Error(t, err) {
			assert.Regexp(t, `^error decoding JSON value in .*/garbage\.json: `, err.Error())
		}
	})

	t.Run("file contains null", func(t *testing.T) {
		err = ioutil.WriteFile(filepath.Join(dir, "null.json"), []byte("null"), 0644)
		require.NoError(t, err)
		_, err = Load(filepath.Join(dir, "null.json"))
		if assert.Error(t, err) {
			assert.Regexp(t, `^loading .*/null\.json resulted in nil config$`, err.Error())
		}
	})

	t.Run("unknown field", func(t *testing.T) {
		err = ioutil.WriteFile(filepath.Join(dir, "unknown.json"), []byte(`{
			"roflcopter": true
		}`), 0644)
		require.NoError(t, err)
		_, err = Load(filepath.Join(dir, "unknown.json"))
		if assert.Error(t, err) {
			assert.Regexp(t, `^error decoding JSON value in .*/unknown\.json: `, err.Error())
		}
	})

	t.Run("more", func(t *testing.T) {
		err = ioutil.WriteFile(filepath.Join(dir, "more.json"), []byte("{}{}"), 0644)
		require.NoError(t, err)
		_, err = Load(filepath.Join(dir, "more.json"))
		if assert.Error(t, err) {
			assert.Regexp(t, `^found unexpected data after config in .*/more\.json$`, err.Error())
		}
	})

	t.Run("ok", func(t *testing.T) {
		err = ioutil.WriteFile(filepath.Join(dir, "ok.json"), []byte(`{
			"akutanLog": {"type": "logspec"}
		}`), 0644)
		require.NoError(t, err)
		cfg, err := Load(filepath.Join(dir, "ok.json"))
		if assert.NoError(t, err) {
			assert.Equal(t, "logspec", cfg.AkutanLog.Type)
		}
	})
}

func Test_Write(t *testing.T) {
	dir, err := ioutil.TempDir("", "config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Happy path.
	err = Write(&Akutan{}, filepath.Join(dir, "ok.json"))
	assert.NoError(t, err)

	// Simulate an error from encoder.Encode().
	marshalJSONErr = errors.New("ants in pants")
	err = Write(&Akutan{Tracing: &Tracing{}}, filepath.Join(dir, "ants.json"))
	marshalJSONErr = nil
	if assert.Error(t, err) {
		assert.Regexp(t, `^failed to write .*/ants\.json: .*ants in pants`,
			err.Error())
	}

	// Errors from os.Create already include the filename.
	err = os.MkdirAll(filepath.Join(dir, "subdir"), 0755)
	require.NoError(t, err)
	err = Write(&Akutan{}, filepath.Join(dir, "subdir"))
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "subdir")
	}
}

// Controls the returned error of Tracing.MarshalJSON.
var marshalJSONErr error

// This is a custom marshaller for Tracing (used only in unit tests). It
// normally encodes itself successfully, but if 'marshalJSONErr' is non-nil, it
// returns this error instead.
func (t Tracing) MarshalJSON() ([]byte, error) {
	if marshalJSONErr != nil {
		return nil, marshalJSONErr
	}
	return json.Marshal(struct {
		Type    string  `json:"type"`
		Locator Locator `json:"locator"`
	}{
		Type:    t.Type,
		Locator: t.Locator,
	})
}
