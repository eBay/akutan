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

package impl

import (
	"fmt"
	"sort"

	"github.com/ebay/beam/api"
	"github.com/ebay/beam/logentry"
	"github.com/ebay/beam/logentry/logread"
	"github.com/ebay/beam/logentry/logwrite"
	"github.com/ebay/beam/util/unicode"
	log "github.com/sirupsen/logrus"
)

type varStatus uint8

const (
	varDeclared varStatus = 1 << iota
	varUsed
	varFactID
)

// validateInsertFactsRequest performs basic validation on the insert facts
// structure. It's just cross-checking data inside the request struct, it's not
// doing any validations that require access to the currently stored data.
// It will ensure that the number of new KIDs required does not exceed the
// maximum for a single log entry, and it will verify that the usage of variables
// is valid
//		1) every variable in newSubjectVars is used in a fact.
//		2) every variable that captures a FactID is used in some other fact.
//		3) a field that is a variable doesn't have an empty variable name.
//		4) a variable that's in NewSubjectVars isn't also used to capture a FactID.
func validateInsertFactsRequest(i *api.InsertFactsRequest) error {
	if len(i.Facts)+len(i.NewSubjectVars) >= logread.MaxOffset {
		return fmt.Errorf("a single insert is limited to %d new Ids, this request has %d new Subjects & %d new Facts which is too many", logread.MaxOffset-1, len(i.NewSubjectVars), len(i.Facts))
	}
	vars := make(map[string]varStatus)
	for _, v := range i.NewSubjectVars {
		vars[v] = varDeclared
	}
	checkVar := func(factIdx int, varName, field string, canBeFactID bool) error {
		if varName == "" {
			return nil
		}
		if _, exists := vars[varName]; !exists {
			return fmt.Errorf("fact[%d] specifies a %s variable '%s', but that isn't delcared", factIdx, field, varName)
		}
		vars[varName] |= varUsed
		if !canBeFactID {
			if vars[varName]&varFactID != 0 {
				return fmt.Errorf("fact[%d] specifies a %s variable '%s', but that variable is a fact_id and can't be bound to %s", factIdx, field, varName, field)
			}
		}
		return nil
	}
	// check that used variables exist.
	for idx, f := range i.Facts {
		if f.FactIDVar != "" {
			// the FactID can only be bound for reading later on, each fact that declares a var must declare a new unique var
			if _, exists := vars[f.FactIDVar]; exists {
				return fmt.Errorf("fact[%d] specifies a FactID Variable '%s', but that was already declared as variable", idx, f.FactIDVar)
			}
			vars[f.FactIDVar] = varDeclared + varFactID
		}
		if err := checkVar(idx, f.Subject.GetVar(), "subject", true); err != nil {
			return err
		}
		if err := checkVar(idx, f.Predicate.GetVar(), "predicate", false); err != nil {
			return err
		}
		if err := checkVar(idx, f.Object.GetVar(), "object", false); err != nil {
			return err
		}
		if f.Object.GetVar() == "" && f.Object.GetObject() == nil {
			return fmt.Errorf("fact[%d] Object doesn't specify a variable or an Object value, one or the other is required", idx)
		}
	}
	// ensure that all declared vars were actually used.
	unused := make([]string, 0, 4)
	for v, st := range vars {
		if st&varUsed == 0 {
			unused = append(unused, v)
		}
	}
	if len(unused) > 0 {
		sort.Strings(unused)
		return fmt.Errorf("the following variables were declared but not used, all variables must get used: %v", unused)
	}
	return nil
}

// convertAPIInsertToLogCommand will take an api.InsertFactsRequest and convert
// it into a logentry.InsertTxCommand. This function assumes that
// validateInsertFactsRequest has already been called on the supplied
// InsertFactsRequest, and that it reported no errors.
//
// In the logentry version variables have been replaced with offsets which can
// be resolved to a KID given a log index that the message was written to.
//
// We do this so that we only assign ids in a single place, and record that in
// the log entry. This means if we need to change how this assignment happens in
// the future we won't have to deal with trying to get the API & DiskView
// instances to switch calculation at the same time.
//
// In addition it returns a map of variable name to offset for all the variables
// used in the insert. This map can be used in conjunction with the KID function
// to determine the final KIDs assigned to the variables once you know the log
// index the InsertTxCommand was written at.
func convertAPIInsertToLogCommand(i *api.InsertFactsRequest) (logentry.InsertTxCommand, map[string]int32) {
	// dont access this directly, use the nextOffset function
	nextOffsetToUse := int32(1)
	nextOffset := func() int32 {
		r := nextOffsetToUse
		nextOffsetToUse++
		return r
	}

	// The offsets are assigned sequentially from the insert request. We assign
	// all new subjects first, then assign offsets for each factID. If
	// subsequently existing facts are filtered out of the resulting
	// InsertTxCommand, that'll leave holes in the used offsets, and thats ok.
	vars := make(map[string]int32, len(i.NewSubjectVars))
	for _, v := range i.NewSubjectVars {
		vars[v] = nextOffset()
	}

	res := logentry.InsertTxCommand{
		Facts: make([]logentry.InsertFact, len(i.Facts)),
	}
	for idx, f := range i.Facts {
		factIDOffset := nextOffset()
		if f.FactIDVar != "" {
			vars[f.FactIDVar] = factIDOffset
		}
		logFact := logentry.InsertFact{
			FactIDOffset: factIDOffset,
			Subject:      convertKidOrVar(&f.Subject, vars),
			Predicate:    convertKidOrVar(&f.Predicate, vars),
			Object:       convertKGObjectOrVar(&f.Object, vars),
		}
		res.Facts[idx] = logFact
	}
	if nextOffsetToUse >= logread.MaxOffset {
		// validateInsertFactsRequest should of already caught this condition
		// so something has gone wrong somewhere.
		log.Panicf("convertAPIInsertToLogCommand consumed too many offsets: %d, which should of been caught by validateInsertFactsRequest", nextOffsetToUse-1)
	}
	return res, vars
}

// convertKidOrVar maps from the api to log types for the KidOrVar, primarily by
// converting vars to offsets. The offset describes a KID relative to a later
// assigned log index.
func convertKidOrVar(in *api.KIDOrVar, vars map[string]int32) logentry.KIDOrOffset {
	switch t := in.Value.(type) {
	case *api.KIDOrVar_Kid:
		return logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Kid{Kid: t.Kid}}
	case *api.KIDOrVar_Var:
		varOffset, exists := vars[t.Var]
		if !exists {
			log.Panicf("convertKidOrVar called with var '%s', which doesn't exist, this should never happen", t.Var)
		}
		return logentry.KIDOrOffset{Value: &logentry.KIDOrOffset_Offset{Offset: varOffset}}
	}
	log.Panicf("Received unexpected type for api.KIDOrVar value %T %v", in.Value, in.Value)
	return logentry.KIDOrOffset{} // never gets here
}

// convertKGObjectOrVar maps from the api to log types for the KGObjectOrVar,
// primarily by converting vars to offsets. The offset describes a KID relative
// to a later assigned log index.
func convertKGObjectOrVar(in *api.KGObjectOrVar, vars map[string]int32) logentry.KGObject {
	switch t := in.Value.(type) {
	case *api.KGObjectOrVar_Var:
		return logwrite.AKIDOffset(vars[t.Var])

	case *api.KGObjectOrVar_Object:
		switch lv := t.Object.Value.(type) {
		case *api.KGObject_AString:
			return logwrite.AString(lv.AString, t.Object.LangID)
		case *api.KGObject_AInt64:
			return logwrite.AInt64(lv.AInt64, t.Object.UnitID)
		case *api.KGObject_AFloat64:
			return logwrite.AFloat64(lv.AFloat64, t.Object.UnitID)
		case *api.KGObject_ABool:
			return logwrite.ABool(lv.ABool, t.Object.UnitID)
		case *api.KGObject_AKID:
			return logwrite.AKID(lv.AKID)
		case *api.KGObject_ATimestamp:
			// Precision is currently defined identically in both api &
			// logentry. This function will need to change if that changes. The
			// unit test Test_PrecisionSame verifies that the values are
			// currently equal.
			return logwrite.ATimestamp(lv.ATimestamp.Value, logentry.TimestampPrecision(lv.ATimestamp.Precision), t.Object.UnitID)
		}
		log.Panicf("Received unexpected type for api.KGObject value %T %v", t.Object.Value, t.Object.Value)
	}
	log.Panicf("Received unexpected type for api.KGObjectOrVar value %T %v", in.Value, in.Value)
	return logentry.KGObject{} // never gets here
}

// applyUnicodeNormalizationToInsertFactsRequest rewrites string variables and
// Object string literals fields to Unicode normalized string to the
// InsertFactRequest
func applyUnicodeNormalizationToInsertFactsRequest(req *api.InsertFactsRequest) {
	for i := range req.NewSubjectVars {
		req.NewSubjectVars[i] = unicode.Normalize(req.NewSubjectVars[i])
	}
	for i, fact := range req.Facts {
		req.Facts[i].FactIDVar = unicode.Normalize(fact.FactIDVar)
		if subject, ok := fact.Subject.Value.(*api.KIDOrVar_Var); ok {
			subject.Var = unicode.Normalize(subject.Var)
		}
		if predicate, ok := fact.Predicate.Value.(*api.KIDOrVar_Var); ok {
			predicate.Var = unicode.Normalize(predicate.Var)
		}
		switch object := fact.Object.Value.(type) {
		case *api.KGObjectOrVar_Var:
			object.Var = unicode.Normalize(object.Var)
		case *api.KGObjectOrVar_Object:
			if object, ok := object.Object.Value.(*api.KGObject_AString); ok {
				object.AString = unicode.Normalize(object.AString)
			}
		}
	}
}
