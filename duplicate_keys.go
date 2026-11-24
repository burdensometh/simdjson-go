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
	"errors"
	"fmt"
	"strings"
)

// DuplicateKeyPolicy defines how duplicate keys within a single JSON object
// are handled when JSON is projected onto the parsed representation.
//
// Suppliers occasionally emit the same key twice in one object (for instance
// an old and a new value). This is valid JSON, but ambiguous to read:
// FindKey/FindPath resolve to the first occurrence, while Map/Elements
// resolve to the last, and marshaled output contains all occurrences.
// Declaring a policy normalizes the parsed result at ingestion time,
// so all lookups and all output use the same value.
//
// Policies are applied per object scope: the same key appearing in
// different (nested) objects is not considered a duplicate.
type DuplicateKeyPolicy int

const (
	// DuplicateKeyKeepAll keeps all occurrences of duplicated keys.
	// This is the default and matches historical behavior:
	// FindKey/FindPath return the first occurrence, Map/Elements the last,
	// and marshaled output contains all occurrences.
	DuplicateKeyKeepAll DuplicateKeyPolicy = iota

	// DuplicateKeyKeepFirst keeps the first occurrence of a duplicated key
	// within each object and discards later occurrences of that key.
	DuplicateKeyKeepFirst

	// DuplicateKeyKeepLast keeps the last occurrence of a duplicated key
	// within each object and discards earlier occurrences of that key.
	DuplicateKeyKeepLast

	// DuplicateKeyReject rejects any input in which a single object
	// contains a duplicated key. Parsing fails with a *DuplicateKeyError.
	DuplicateKeyReject
)

// String returns a human-readable representation of the policy.
func (d DuplicateKeyPolicy) String() string {
	switch d {
	case DuplicateKeyKeepAll:
		return "keep-all"
	case DuplicateKeyKeepFirst:
		return "keep-first"
	case DuplicateKeyKeepLast:
		return "keep-last"
	case DuplicateKeyReject:
		return "reject"
	}
	return "unknown"
}

// WithDuplicateKeyPolicy declares how duplicate keys within the same object
// are handled at parse time. See DuplicateKeyPolicy for the available policies.
// The policy is applied to the parsed projection, so key lookups
// (FindKey, FindPath, Map, Elements) and serialized output all observe
// the same result.
// Default: DuplicateKeyKeepAll - duplicates are kept as parsed.
func WithDuplicateKeyPolicy(p DuplicateKeyPolicy) ParserOption {
	return func(pj *internalParsedJson) error {
		switch p {
		case DuplicateKeyKeepAll, DuplicateKeyKeepFirst, DuplicateKeyKeepLast, DuplicateKeyReject:
			pj.duplicateKeyPolicy = p
			return nil
		default:
			return fmt.Errorf("simdjson: invalid duplicate key policy: %d", int(p))
		}
	}
}

// ErrDuplicateKey is returned (wrapped by *DuplicateKeyError) when parsing
// with DuplicateKeyReject and a single object contains a duplicated key.
var ErrDuplicateKey = errors.New("duplicate object key")

// DuplicateKeyError describes a duplicate key found while parsing
// with DuplicateKeyReject.
type DuplicateKeyError struct {
	// Key is the duplicated key.
	Key string

	// Path contains the object keys leading to the object
	// that holds the duplicate, from the outermost object inward.
	// It is empty if the duplicate is in the top-level object.
	Path []string

	// Line is the 0-based index of the root element holding the duplicate.
	// For NDJSON input this corresponds to the line number.
	Line int
}

// Error returns a string representation of the error.
func (e *DuplicateKeyError) Error() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "duplicate object key %q", e.Key)
	if len(e.Path) > 0 {
		fmt.Fprintf(&sb, " in object at %q", strings.Join(e.Path, "/"))
	}
	fmt.Fprintf(&sb, " (line %d)", e.Line)
	return sb.String()
}

// Unwrap returns ErrDuplicateKey.
func (e *DuplicateKeyError) Unwrap() error {
	return ErrDuplicateKey
}

// dedupMaxDepth limits the nesting depth processed by applyDuplicateKeyPolicy.
const dedupMaxDepth = 10_000

// applyDuplicateKeyPolicy normalizes duplicated object keys on the tape
// according to the given policy.
// Kept occurrences are left in place; discarded occurrences are replaced
// by Nop entries (the same mechanism Object.DeleteElems uses), so every
// reader and all serialized output observe the same keys.
// DuplicateKeyReject returns a *DuplicateKeyError for the first duplicate
// found and leaves the tape unmodified.
func (pj *ParsedJson) applyDuplicateKeyPolicy(policy DuplicateKeyPolicy) error {
	d := deduper{pj: pj, policy: policy}
	off := 0
	for off < len(pj.Tape) {
		next, err := d.value(off, 0)
		if err != nil {
			return err
		}
		if next <= off || next > len(pj.Tape) {
			return fmt.Errorf("simdjson: corrupt tape at index %d", off)
		}
		off = next
		d.line++
	}
	return nil
}

// deduper holds the state of a duplicate key normalization pass.
type deduper struct {
	pj     *ParsedJson
	policy DuplicateKeyPolicy
	// path is the chain of object keys to the current object.
	path []string
	// line is the 0-based index of the root element being processed.
	line int
}

// value processes the value whose tag starts at tape index off and
// returns the tape index immediately after the value.
func (d *deduper) value(off, depth int) (int, error) {
	if depth > dedupMaxDepth {
		return 0, fmt.Errorf("simdjson: maximum nesting depth %d exceeded", dedupMaxDepth)
	}
	tape := d.pj.Tape
	if off < 0 || off >= len(tape) {
		return 0, fmt.Errorf("simdjson: tape index %d out of bounds", off)
	}
	tag := Tag(tape[off] >> 56)
	switch tag {
	case TagString, TagInteger, TagUint, TagFloat:
		if off+2 > len(tape) {
			return 0, fmt.Errorf("simdjson: value at tape index %d extends beyond tape", off)
		}
		return off + 2, nil
	case TagNull, TagBoolTrue, TagBoolFalse:
		return off + 1, nil
	case TagArrayStart, TagRoot:
		end := int(tape[off] & JSONVALUEMASK)
		if end <= off || end > len(tape) {
			return 0, fmt.Errorf("simdjson: %v at tape index %d extends beyond tape", TagToType[tag], off)
		}
		pos := off + 1
		for pos < end-1 {
			var err error
			pos, err = d.value(pos, depth+1)
			if err != nil {
				return 0, err
			}
		}
		if pos != end-1 {
			return 0, fmt.Errorf("simdjson: %v at tape index %d is malformed", TagToType[tag], off)
		}
		return end, nil
	case TagObjectStart:
		return d.object(off, depth)
	}
	return 0, fmt.Errorf("simdjson: unexpected tag %v at tape index %d", tag, off)
}

// object processes the object whose start tag is at tape index off and
// returns the tape index immediately after the object.
// Keys are deduplicated within this object only; nested objects and
// objects inside arrays each get their own independent scope.
func (d *deduper) object(off, depth int) (int, error) {
	tape := d.pj.Tape
	end := int(tape[off] & JSONVALUEMASK)
	if end <= off || end > len(tape) {
		return 0, fmt.Errorf("simdjson: object at tape index %d extends beyond tape", off)
	}
	// kept maps each key to the [start, end) tape range of the
	// occurrence of the key kept so far.
	kept := make(map[string][2]int)
	pos := off + 1
	for pos < end-1 {
		if Tag(tape[pos]>>56) != TagString || pos+2 > len(tape) {
			return 0, fmt.Errorf("simdjson: expected object key at tape index %d", pos)
		}
		keyB, err := d.pj.stringByteAt(tape[pos]&JSONVALUEMASK, tape[pos+1])
		if err != nil {
			return 0, fmt.Errorf("simdjson: reading object key: %w", err)
		}
		key := string(keyB)
		// Process the value with the key on the path.
		d.path = append(d.path, key)
		valEnd, err := d.value(pos+2, depth+1)
		d.path = d.path[:len(d.path)-1]
		if err != nil {
			return 0, err
		}
		if valEnd > end {
			return 0, fmt.Errorf("simdjson: value of key %q extends beyond object at tape index %d", key, off)
		}
		if prev, dup := kept[key]; dup {
			switch d.policy {
			case DuplicateKeyKeepFirst:
				// Discard the later occurrence.
				nopTapeRange(tape, pos, valEnd)
			case DuplicateKeyKeepLast:
				// Discard the earlier occurrence, keep this one.
				nopTapeRange(tape, prev[0], prev[1])
				kept[key] = [2]int{pos, valEnd}
			case DuplicateKeyReject:
				return 0, &DuplicateKeyError{
					Key:  key,
					Path: append([]string(nil), d.path...),
					Line: d.line,
				}
			}
		} else {
			kept[key] = [2]int{pos, valEnd}
		}
		pos = valEnd
	}
	if pos != end-1 {
		return 0, fmt.Errorf("simdjson: object at tape index %d is malformed", off)
	}
	return end, nil
}

// nopTapeRange marks tape entries [start, end) as skipped.
// The encoding matches Object.DeleteElems: each entry holds
// the distance from its own index to the end of the range.
func nopTapeRange(tape []uint64, start, end int) {
	for i := start; i < end; i++ {
		tape[i] = uint64(TagNop)<<JSONTAGOFFSET | uint64(end-i)
	}
}
