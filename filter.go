/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package simdjson

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

// MatchResult is the outcome of evaluating a Filter against a single record.
type MatchResult uint8

const (
	// NoMatch means that every field the filter reads was present, but the
	// record did not satisfy the filter.
	NoMatch MatchResult = iota
	// Match means that the record satisfied the filter.
	Match
	// MissingField means that at least one field the filter needs was absent
	// (or was not an object where an object was required), so the record
	// could not be evaluated.
	MissingField
)

// String returns the result as a string.
func (m MatchResult) String() string {
	switch m {
	case NoMatch:
		return "no match"
	case Match:
		return "match"
	case MissingField:
		return "missing field"
	}
	return "(invalid)"
}

// Filter is a typed, composable predicate over a single JSON record.
//
// A Filter is evaluated through Iter.Match, ParsedJson.ForEachFiltered or
// ParsedJson.WriteMatches. Implementations must not modify the supplied
// iterator. The built-in filters only read the tape; they never build a
// generic map representation of the record.
type Filter interface {
	// Match evaluates the record that i points at.
	// It returns Match when the record satisfies the filter, NoMatch when the
	// record was fully evaluated but does not, and MissingField when a field
	// the filter reads is absent.
	Match(i *Iter) MatchResult
}

// FilterFunc is an adapter that allows ordinary functions to be used as
// Filters. A nil FilterFunc matches every record.
type FilterFunc func(i *Iter) MatchResult

// Match calls f(i).
func (f FilterFunc) Match(i *Iter) MatchResult {
	if f == nil {
		return Match
	}
	return f(i)
}

// Op is a comparison operator used by the typed field filters.
type Op uint8

const (
	// OpEqual reports whether the value is equal to the argument.
	OpEqual Op = iota
	// OpNotEqual reports whether the value is not equal to the argument.
	OpNotEqual
	// OpLess reports whether the value is less than the argument.
	OpLess
	// OpLessEqual reports whether the value is less than or equal to the argument.
	OpLessEqual
	// OpGreater reports whether the value is greater than the argument.
	OpGreater
	// OpGreaterEqual reports whether the value is greater than or equal to the argument.
	OpGreaterEqual
)

// String returns the operator as a string.
func (op Op) String() string {
	switch op {
	case OpEqual:
		return "=="
	case OpNotEqual:
		return "!="
	case OpLess:
		return "<"
	case OpLessEqual:
		return "<="
	case OpGreater:
		return ">"
	case OpGreaterEqual:
		return ">="
	}
	return "(invalid)"
}

// And returns a Filter that matches when all the supplied filters match.
//
// The result is NoMatch if any filter reports NoMatch, otherwise MissingField
// if any filter reports MissingField, otherwise Match. In other words a
// definite miss wins over a missing field: a record that provably fails one
// clause is reported as NoMatch even if another clause could not be evaluated.
// Nil filters are ignored. And() matches every record.
func And(filters ...Filter) Filter {
	return andFilter(filters)
}

// Or returns a Filter that matches when any of the supplied filters match.
//
// The result is Match if any filter reports Match, otherwise MissingField if
// any filter reports MissingField, otherwise NoMatch. A record that satisfies
// one clause matches even if another clause could not be evaluated. Nil
// filters are ignored. Or() matches no record.
func Or(filters ...Filter) Filter {
	return orFilter(filters)
}

// Not returns a Filter that inverts f.
// Match becomes NoMatch and NoMatch becomes Match. MissingField is not
// invertible and is returned unchanged.
func Not(f Filter) Filter {
	return notFilter{f}
}

type andFilter []Filter

func (a andFilter) Match(i *Iter) MatchResult {
	res := Match
	for _, f := range a {
		if f == nil {
			continue
		}
		switch f.Match(i) {
		case NoMatch:
			return NoMatch
		case MissingField:
			res = MissingField
		}
	}
	return res
}

type orFilter []Filter

func (o orFilter) Match(i *Iter) MatchResult {
	res := NoMatch
	for _, f := range o {
		if f == nil {
			continue
		}
		switch f.Match(i) {
		case Match:
			return Match
		case MissingField:
			res = MissingField
		}
	}
	return res
}

type notFilter struct {
	f Filter
}

func (n notFilter) Match(i *Iter) MatchResult {
	if n.f == nil {
		return NoMatch
	}
	switch n.f.Match(i) {
	case Match:
		return NoMatch
	case NoMatch:
		return Match
	}
	return MissingField
}

// Field returns a selector for the value at the given path inside a record.
// Each path element is an object key, for example Field("payment", "status").
// The path never descends into arrays.
//
// Comparisons on the returned selector are typed: a value of another JSON type
// never satisfies the comparison and is reported as NoMatch. An empty path
// selects the record itself.
func Field(path ...string) FieldSelector {
	return FieldSelector{path: path}
}

// FieldSelector selects a value inside a record.
// Its methods create typed Filters; it holds no state of its own.
type FieldSelector struct {
	path []string
}

// Exists returns a Filter that matches when the field is present.
// A field holding null counts as present.
func (f FieldSelector) Exists() Filter {
	return fieldFilter{path: f.path, kind: kindExists}
}

// String returns a Filter that compares the field with a string.
// The field must be a JSON string; any other type is NoMatch.
// Ordering operators compare the strings byte by byte.
func (f FieldSelector) String(op Op, v string) Filter {
	return fieldFilter{path: f.path, kind: kindString, op: op, strBytes: []byte(v)}
}

// Bool returns a Filter that compares the field with a bool.
// The field must be a JSON boolean; any other type is NoMatch.
// Only OpEqual and OpNotEqual are meaningful.
func (f FieldSelector) Bool(op Op, v bool) Filter {
	return fieldFilter{path: f.path, kind: kindBool, op: op, boolean: v}
}

// Number returns a Filter that compares the field with a number.
// Integers, unsigned integers and floats are compared by value in float64,
// so a threshold above 2^53 may lose precision. Any other type is NoMatch.
// Use Int for an exact comparison against an integer threshold.
func (f FieldSelector) Number(op Op, v float64) Filter {
	return fieldFilter{path: f.path, kind: kindNumber, op: op, num: v}
}

// Int returns a Filter that compares an integer-valued field with an int64.
// Only JSON integers and unsigned integers are matched; a float, even one with
// an integral value, is NoMatch. The comparison is exact.
// A negative value cannot equal an unsigned integer of the same magnitude.
func (f FieldSelector) Int(op Op, v int64) Filter {
	return fieldFilter{path: f.path, kind: kindInt, op: op, integer: v}
}

// Uint returns a Filter that compares a non-negative integer-valued field with
// a uint64. Only JSON integers and unsigned integers are matched; floats are
// NoMatch. The comparison is exact.
func (f FieldSelector) Uint(op Op, v uint64) Filter {
	return fieldFilter{path: f.path, kind: kindUint, op: op, unsigned: v}
}

type valueKind uint8

const (
	kindExists valueKind = iota
	kindString
	kindBool
	kindNumber
	kindInt
	kindUint
)

type fieldFilter struct {
	path []string
	op   Op
	kind valueKind

	strBytes []byte
	num      float64
	integer  int64
	unsigned uint64
	boolean  bool
}

func (f fieldFilter) Match(i *Iter) MatchResult {
	v, ok := fieldValue(i, f.path)
	if !ok {
		return MissingField
	}
	switch f.kind {
	case kindExists:
		return Match
	case kindString:
		if v.t != TagString {
			return NoMatch
		}
		s, err := v.StringBytes()
		if err != nil {
			return NoMatch
		}
		return result(compareBytes(s, f.op, f.strBytes))
	case kindBool:
		switch v.t {
		case TagBoolTrue, TagBoolFalse:
		default:
			return NoMatch
		}
		got := v.t == TagBoolTrue
		switch f.op {
		case OpEqual:
			return result(got == f.boolean)
		case OpNotEqual:
			return result(got != f.boolean)
		}
		return NoMatch
	case kindNumber:
		switch v.t {
		case TagInteger, TagUint, TagFloat:
		default:
			return NoMatch
		}
		n, err := v.Float()
		if err != nil {
			return NoMatch
		}
		return result(compareFloat(n, f.op, f.num))
	case kindInt:
		switch v.t {
		case TagInteger:
			got, err := v.Int()
			if err != nil {
				return NoMatch
			}
			return result(compareInt64(got, f.op, f.integer))
		case TagUint:
			got, err := v.Uint()
			if err != nil {
				return NoMatch
			}
			return result(compareUint64ToInt64(got, f.op, f.integer))
		default:
			return NoMatch
		}
	case kindUint:
		switch v.t {
		case TagInteger:
			got, err := v.Int()
			if err != nil {
				return NoMatch
			}
			return result(compareInt64ToUint64(got, f.op, f.unsigned))
		case TagUint:
			got, err := v.Uint()
			if err != nil {
				return NoMatch
			}
			return result(compareUint64(got, f.op, f.unsigned))
		default:
			return NoMatch
		}
	}
	return NoMatch
}

func result(ok bool) MatchResult {
	if ok {
		return Match
	}
	return NoMatch
}

func compareBytes(got []byte, op Op, want []byte) bool {
	switch op {
	case OpEqual:
		return bytes.Equal(got, want)
	case OpNotEqual:
		return !bytes.Equal(got, want)
	case OpLess:
		return bytes.Compare(got, want) < 0
	case OpLessEqual:
		return bytes.Compare(got, want) <= 0
	case OpGreater:
		return bytes.Compare(got, want) > 0
	case OpGreaterEqual:
		return bytes.Compare(got, want) >= 0
	}
	return false
}

func compareFloat(got float64, op Op, want float64) bool {
	switch op {
	case OpEqual:
		return got == want
	case OpNotEqual:
		return got != want
	case OpLess:
		return got < want
	case OpLessEqual:
		return got <= want
	case OpGreater:
		return got > want
	case OpGreaterEqual:
		return got >= want
	}
	return false
}

func compareInt64(got int64, op Op, want int64) bool {
	switch op {
	case OpEqual:
		return got == want
	case OpNotEqual:
		return got != want
	case OpLess:
		return got < want
	case OpLessEqual:
		return got <= want
	case OpGreater:
		return got > want
	case OpGreaterEqual:
		return got >= want
	}
	return false
}

func compareUint64(got uint64, op Op, want uint64) bool {
	switch op {
	case OpEqual:
		return got == want
	case OpNotEqual:
		return got != want
	case OpLess:
		return got < want
	case OpLessEqual:
		return got <= want
	case OpGreater:
		return got > want
	case OpGreaterEqual:
		return got >= want
	}
	return false
}

// compareUint64ToInt64 compares an unsigned tape value with a signed argument.
// A value above math.MaxInt64 is greater than every int64.
func compareUint64ToInt64(got uint64, op Op, want int64) bool {
	if want < 0 {
		switch op {
		case OpEqual:
			return false
		case OpNotEqual:
			return true
		case OpLess:
			return false
		case OpLessEqual:
			return false
		case OpGreater:
			return true
		case OpGreaterEqual:
			return true
		}
		return false
	}
	return compareUint64(got, op, uint64(want))
}

// compareInt64ToUint64 compares a signed tape value with an unsigned argument.
// A negative value is less than every uint64.
func compareInt64ToUint64(got int64, op Op, want uint64) bool {
	if got < 0 {
		switch op {
		case OpEqual:
			return false
		case OpNotEqual:
			return true
		case OpLess:
			return true
		case OpLessEqual:
			return true
		case OpGreater:
			return false
		case OpGreaterEqual:
			return false
		}
		return false
	}
	return compareUint64(uint64(got), op, want)
}

// fieldValue returns the value at path within the record i.
// The supplied iterator is not modified. ok is false if the path is absent or
// if a non-object is found where an object is required.
func fieldValue(i *Iter, path []string) (Iter, bool) {
	if i == nil {
		return Iter{}, false
	}
	cp, ok := derefRoot(*i)
	if !ok || cp.t == TagEnd {
		return Iter{}, false
	}
	if len(path) == 0 {
		return cp, true
	}
	var elem Element
	for n, key := range path {
		var obj Object
		if _, err := cp.Object(&obj); err != nil {
			return Iter{}, false
		}
		if obj.FindKey(key, &elem) == nil {
			return Iter{}, false
		}
		cp = elem.Iter
		if n == len(path)-1 {
			return cp, true
		}
		if cp, ok = derefRoot(cp); !ok {
			return Iter{}, false
		}
	}
	return Iter{}, false
}

// derefRoot moves i into a root tag, if it is positioned at one.
// An iterator that has not been advanced yet is advanced into the first root.
func derefRoot(i Iter) (Iter, bool) {
	if i.t == TagEnd {
		if i.AdvanceInto() == TagEnd {
			return i, false
		}
	}
	for i.t == TagRoot {
		var tmp Iter
		if _, _, err := i.Root(&tmp); err != nil {
			return Iter{}, false
		}
		i = tmp
	}
	return i, true
}

// Match evaluates f against the record that i points at.
// A nil Filter matches every record.
// The iterator is not modified.
func (i *Iter) Match(f Filter) MatchResult {
	if f == nil {
		return Match
	}
	return f.Match(i)
}

// MatchStats counts the outcomes of applying a Filter to a set of records.
type MatchStats struct {
	// Matches is the number of records that satisfied the filter.
	Matches int
	// NoMatches is the number of fully evaluated records that did not satisfy
	// the filter.
	NoMatches int
	// MissingFields is the number of records that were missing at least one
	// field the filter needs.
	MissingFields int
}

// Total returns the number of records that were evaluated.
func (s MatchStats) Total() int {
	return s.Matches + s.NoMatches + s.MissingFields
}

func (s *MatchStats) add(res MatchResult) {
	switch res {
	case Match:
		s.Matches++
	case MissingField:
		s.MissingFields++
	default:
		s.NoMatches++
	}
}

// ForEachFiltered evaluates f against every top level record in order and
// calls fn with the record and its result. Records are reported exactly once,
// in the order they appear in the input, whichever result they have.
//
// The callback may read the record through i but must not retain it after the
// call, since the iterator references the parsed data.
// If fn is nil the results are only evaluated, not reported; use CountMatches
// to also count them. A nil Filter matches every record.
func (pj *ParsedJson) ForEachFiltered(f Filter, fn func(i Iter, res MatchResult) error) error {
	i := Iter{tape: *pj}
	var elem, rec Iter
	for {
		t, err := i.AdvanceIter(&elem)
		if err != nil || t != TypeRoot {
			return err
		}
		elem.AdvanceInto()
		// Evaluate on a copy so the callback always receives an iterator
		// positioned at the start of the record. rec is reused across
		// iterations so taking its address does not allocate per record.
		rec = elem
		res := rec.Match(f)
		if fn != nil {
			if err := fn(elem, res); err != nil {
				return err
			}
		}
	}
}

// ForEachMatch evaluates f against every top level record in order and calls
// fn for each matching record only. Records that do not match, or that are
// missing a required field, are skipped silently.
// To observe every result, including NoMatch and MissingField, use
// ForEachFiltered.
func (pj *ParsedJson) ForEachMatch(f Filter, fn func(i Iter) error) error {
	return pj.ForEachFiltered(f, func(i Iter, res MatchResult) error {
		if res != Match {
			return nil
		}
		if fn == nil {
			return nil
		}
		return fn(i)
	})
}

// CountMatches evaluates f against every top level record in order and returns
// how many records matched, did not match, and were missing a required field.
func (pj *ParsedJson) CountMatches(f Filter) (MatchStats, error) {
	var stats MatchStats
	err := pj.ForEachFiltered(f, func(i Iter, res MatchResult) error {
		stats.add(res)
		return nil
	})
	return stats, err
}

// WriteMatches writes every record that satisfies f to w, preserving the order
// of the input. Each matching record is written as JSON followed by a newline,
// so the output is valid NDJSON and can be parsed again with ParseND.
// Records that do not match or that are missing a required field are not
// written, but are counted in the returned MatchStats.
//
// Filtering reads the parsed tape directly; no map representation of a record
// is built. Each matched record is written to w before the next record is
// examined, so a caller can stream the output into the next stage.
func (pj *ParsedJson) WriteMatches(w io.Writer, f Filter) (MatchStats, error) {
	var stats MatchStats
	if w == nil {
		return stats, errors.New("simdjson: nil writer")
	}
	var buf []byte
	walk := Iter{tape: *pj}
	var root Iter
	for {
		t, err := walk.AdvanceIter(&root)
		if err != nil {
			return stats, err
		}
		if t != TypeRoot {
			// TypeNone ends the stream; anything else is not a record.
			return stats, nil
		}
		// root marshals the whole record; content is a copy used to evaluate
		// the filter. Neither MarshalJSONBuffer nor the built-in filters touch
		// the walker, so the next record is read in order.
		content := root
		content.AdvanceInto()
		res := content.Match(f)
		stats.add(res)
		if res != Match {
			continue
		}
		out := root
		b, err := out.MarshalJSONBuffer(buf[:0])
		if err != nil {
			return stats, err
		}
		b = append(b, '\n')
		if _, err := w.Write(b); err != nil {
			return stats, fmt.Errorf("simdjson: writing matched record: %w", err)
		}
		buf = b[:0]
	}
}
