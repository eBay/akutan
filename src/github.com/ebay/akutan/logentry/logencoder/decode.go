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
	"fmt"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/logentry"
	"github.com/ebay/akutan/rpc"
	"github.com/sirupsen/logrus"
)

// DefaultLogVersion is the log version a new empty log should start at.
const DefaultLogVersion = 1

// Version represents a version of the log entry format/semantics. Details about
// each version are in the commands.proto file in the parent package.
type Version = int32

// MaxVersionSupported contains the maximum log version that the running
// process supports.
const MaxVersionSupported = 1

// const except for tests
var maxVersion = int32(MaxVersionSupported)

// LogInitialPosition returns a log position that should be used to start
// reading the log for an new empty log.
func LogInitialPosition() rpc.LogPosition {
	return rpc.LogPosition{
		Index:   1,
		Version: DefaultLogVersion,
	}
}

// Entry represent a log entry that has been decoded back to its Command
// structure.
type Entry struct {
	// Position contains the log index & version that applies for this entry.
	Position rpc.LogPosition
	// Command is a pointer to one of logentry.*Command types. Typically you'd
	// type switch on this to process the entry. Its never nil for an Entry
	// returned from Decode.
	Command ProtobufCommand
}

// Decode will parse the supplied log entry and apply any adjustments required
// by the supplied version. It returns the decoded Entry and the next position
// that should be read.
//
// It will apply versioning checks to the log entry if its a VersionCommand
//      *) It will not allow the version to be set higher than maxVersion.
//         If it detects this it will log details & exit the process.
//
//      *) It will not allow the version to be set lower than the version of
//         the log entry that contained the VersionCommand. i.e. versions
//         can not go backwards. If this is detected the VersionCommand is
//         ignored and the previously active version remains inforce.
//         e.g. if there are log entries to move the version to 3, then 4, then 3
//         the last attempt to change to version 3 is ignored, and the active
//         version remains at version 4.
//
// The caller will need to remember the returned nextPos in order that it can
// provide the version for the call to decode for the next log entry.
//
// Returns an error in the event the entry cannot be correctly decoded.
func Decode(ver Version, e blog.Entry) (decoded Entry, nextPos rpc.LogPosition, err error) {
	decoded = Entry{
		Position: rpc.LogPosition{
			Index:   e.Index,
			Version: ver,
		},
	}
	nextPos = rpc.LogPosition{
		Index:   e.Index + 1,
		Version: ver,
	}
	if e.Skip {
		decoded.Command = new(logentry.SkippedCommand)
		return decoded, nextPos, nil
	}
	if len(e.Data) == 0 {
		return Entry{}, nextPos, fmt.Errorf("invalid command: data has zero length at LogIndex:%d", e.Index)
	}
	msgType := commandType(e.Data[0])
	switch msgType {
	case insertTx:
		decoded.Command = new(logentry.InsertTxCommand)
	case wipe:
		decoded.Command = new(logentry.WipeCommand)
	case txDecision:
		decoded.Command = new(logentry.TxDecisionCommand)
	case skipped:
		decoded.Command = new(logentry.SkippedCommand)
	case version:
		decoded.Command = new(logentry.VersionCommand)
	case ping:
		decoded.Command = new(logentry.PingCommand)
	default:
		return decoded, nextPos, fmt.Errorf("unexpected command type of %#x at LogIndex:%d", msgType, e.Index)
	}
	if err := decoded.Command.Unmarshal(e.Data[1:]); err != nil {
		return Entry{}, nextPos, err
	}
	applyVersionRulesToEntry(&decoded)
	if msgType == version {
		nextPos.Version = verifyVersionChangeValid(&decoded)
	}
	return decoded, nextPos, nil
}

// fatalf is a function that can be used to capture & log fatal conditions and
// exit the process. This is extracted for unit testing purposes only.
var fatalf = logrus.Fatalf

// verifyVersionChangeValid will process a decoded VersionCommand and verify
// that the requested new version is supportable. In the event the
// VersionCommand specifies a version that's not supported by this instance, it
// will log & exit. If it detect and attempt to move to an earlier version, it
// will log and ignore it, leaving the previously current version active. The
// Version that should apply for the next log entry is returned.
func verifyVersionChangeValid(e *Entry) Version {
	cmd := e.Command.(*logentry.VersionCommand)
	if cmd.MoveToVersion > maxVersion {
		fatalf("VersionCommand moveTo:%d from log @%d, but maxVersion for this process is %d, and can't support the requested version", cmd.MoveToVersion, e.Position.Index, maxVersion)
	}
	if cmd.MoveToVersion < e.Position.Version {
		logrus.WithFields(logrus.Fields{
			"moveToVersion":  cmd.MoveToVersion,
			"currentVersion": e.Position.Version,
			"logIndex":       e.Position.Index,
		}).Warnf("Ignoring VersionCommand that is attempt to move the Version# backwards")
		return e.Position.Version
	}
	return cmd.MoveToVersion
}

// applyVersionRulesToEntry should mutate 'e' to be consistent with the version
// it indicates it is.
func applyVersionRulesToEntry(e *Entry) {
	// as versions are added, the processing of the version specific changes to
	// log entries should be applied here. Mostly this should be about ensuring
	// that protobuf instances look like the prior version. see the versioning
	// notes in logentry/commands.proto
}
