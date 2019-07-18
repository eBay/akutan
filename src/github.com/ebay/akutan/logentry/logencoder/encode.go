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

package logencoder

import (
	"github.com/ebay/akutan/logentry"
	"github.com/sirupsen/logrus"
)

// ProtobufCommand defines a base interface that all protobuf generated types
// implement
type ProtobufCommand interface {
	ProtoMessage()
	Size() int
	MarshalTo([]byte) (int, error)
	Unmarshal([]byte) error
}

// Encode will take the supplied command and serialize into a format
// suitable for persisting to the log. cmd should be one of the
// *Command types from the logentry package. If its unable to serialize
// the Command, that implies a programmer error somewhere, and it will
// panic.
func Encode(cmd ProtobufCommand) []byte {
	res := make([]byte, cmd.Size()+1)
	if _, err := cmd.MarshalTo(res[1:]); err != nil {
		logrus.Panicf("Unable to MarshalTo logentry Command %T: %v", cmd, err)
	}
	switch cmd.(type) {
	case *logentry.InsertTxCommand:
		res[0] = byte(insertTx)
	case *logentry.TxDecisionCommand:
		res[0] = byte(txDecision)
	case *logentry.WipeCommand:
		res[0] = byte(wipe)
	case *logentry.SkippedCommand:
		res[0] = byte(skipped)
	case *logentry.VersionCommand:
		res[0] = byte(version)
	case *logentry.PingCommand:
		res[0] = byte(ping)
	default:
		logrus.Panicf("Encode received an unexpected type %T", cmd)
	}
	return res
}
