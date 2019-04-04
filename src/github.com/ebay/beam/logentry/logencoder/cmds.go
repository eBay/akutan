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

// Package logencoder handles serialization and deserialization of logentry.*Command
// in to/from bytes. The resulting bytes should be written/read from the log.
package logencoder

// commandType describes the different types of log entries. commandType is used
// as the first byte in a serialized log entry.
type commandType byte

const (
	// The values for these commandType constants all get serialized as part of the
	// log entry. They should not be changed.

	// insertTx entries contain an InsertTxCommand.
	insertTx commandType = 'T'

	// txDecision entries contain a TxDecisionCommand.
	txDecision commandType = 'D'

	// wipe entries contain a WipeCommand.
	wipe commandType = 'W'

	// version entries contain a VersionCommand.
	version commandType = 'V'

	// ping entries contain a PingCommand.
	ping commandType = 'P'

	// skipped indicates that the log intentionally skipped this log entry
	// and it's empty. These are not serialized into the log but during
	// reading they are mapped back to this for consistency in how code
	// consumes the output from Decode.
	skipped commandType = 'S'
)
