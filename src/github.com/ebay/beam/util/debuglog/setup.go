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

// Package debuglog configures Logrus. It configures Logrus to print out file
// and line info, to use UTC timestamps with subsecond precision, etc.
//
// This should be used from every main package. When you import this package, it
// will run Configure with default options. Users should document that in their
// import lines or call Configure again explicitly.
package debuglog

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/sirupsen/logrus"
)

func init() {
	Configure(Options{})
}

// Options are used to control the debug logger's behavior. The default Options
// are represented by the zero value.
type Options struct {
	// If true, the logger will highlight some output with ANSI colors. This
	// might not work in all situations, such as when writing into a file.
	//
	// This may be overridden by setting the environment variable
	// "CLICOLOR_FORCE" to "1".
	ForceColors bool

	// If not nil, this will set up the given logger. If nil, it will set up the
	// default Logrus logger (see logrus.StandardLogger()).
	//
	// This is primarily used for unit testing.
	Logger *logrus.Logger
}

// Configure sets up the debug logger. It's safe to call more than once. It
// should not be called concurrently (results are undefined if called
// concurrently with different Options).
func Configure(opts Options) {
	if opts.Logger == nil {
		opts.Logger = logrus.StandardLogger()
	}
	opts.Logger.SetReportCaller(true)
	opts.Logger.AddHook(utcHook{})
	opts.Logger.AddHook(newFilenameHook())
	opts.Logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:             true,
		TimestampFormat:           "2006-01-02 15:04:05.000000 MST",
		ForceColors:               opts.ForceColors,
		EnvironmentOverrideColors: true,
	})
	opts.Logger.WithFields(logrus.Fields{
		"forceColors": opts.ForceColors,
	}).Info("Initialized Logrus")
}

// utcHook implements logrus.Hook. Its purpose is to convert the timestamp to
// UTC.
type utcHook struct{}

func (utcHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (utcHook) Fire(entry *logrus.Entry) error {
	entry.Time = entry.Time.UTC()
	return nil
}

// filenameHook implements logrus.Hook. Its purpose is to strip the prefix of
// the file path up to the root of the Beam repo, which is otherwise repeated
// with just about every log message.
type filenameHook struct {
	prefix string
}

func newFilenameHook() filenameHook {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return filenameHook{
			prefix: "",
		}
	}
	localPath := "src/github.com/ebay/beam/util/debuglog/setup.go"
	if !strings.HasSuffix(file, localPath) {
		panic(fmt.Sprintf("Trying to calculate filename prefix for logging "+
			"but this code got moved. Got %v which doesn't end in %v",
			file, localPath))
	}
	return filenameHook{
		prefix: file[:len(file)-len(localPath)],
	}
}

func (hook filenameHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (hook filenameHook) Fire(entry *logrus.Entry) error {
	if entry.HasCaller() {
		entry.Caller.File = strings.TrimPrefix(entry.Caller.File, hook.prefix)
	}
	return nil
}
