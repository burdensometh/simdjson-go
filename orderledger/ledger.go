/*
 * MinIO Cloud Storage, (C) 2026 MinIO, Inc.
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

package orderledger

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/minio/simdjson-go"
)

// Default field names.
const (
	DefaultKeyField     = "order_id"
	DefaultVersionField = "version"
)

// Errors reported per record through InvalidRecord. ErrBadVersion is
// reported for malformed versions and is declared in version.go.
var (
	ErrNilParsed      = errors.New("orderledger: nil parsed json")
	ErrNotObject      = errors.New("orderledger: record is not a JSON object")
	ErrMissingKey     = errors.New("orderledger: record has no order key")
	ErrBadKey         = errors.New("orderledger: order key must be a string or an integer")
	ErrMissingVersion = errors.New("orderledger: record has no version")
)

// Config configures a Ledger.
type Config struct {
	// KeyField is the top-level field holding the order key.
	// The value must be a JSON string or integer.
	// Defaults to "order_id".
	KeyField string

	// VersionField is the top-level field holding the version.
	// The value must be a JSON integer or a decimal integer string.
	// Defaults to "version".
	VersionField string

	// IgnoreFields lists top-level fields that are not compared when
	// deciding whether two deliveries carry the same value.
	// Use this for delivery metadata such as ingestion timestamps or
	// broker offsets. Ignored fields are still part of Event.Raw.
	//
	// KeyField and VersionField are always excluded from the comparison.
	IgnoreFields []string
}

func (c Config) withDefaults() Config {
	if c.KeyField == "" {
		c.KeyField = DefaultKeyField
	}
	if c.VersionField == "" {
		c.VersionField = DefaultVersionField
	}
	return c
}

// Ledger merges order updates for a single upstream source.
//
// A Ledger is not safe for concurrent use.
type Ledger struct {
	cfg     Config
	ignored map[string]struct{}
	entries map[string]*entryState
}

type entryState struct {
	key      string
	versions map[Version]*versionState
	max      Version
	hasMax   bool
}

type versionState struct {
	version    Version
	events     map[string]*Event
	deliveries int
}

// Event is one delivery of a record for an order key and version.
//
// Distinct deliveries are identified by their compared content, not by their
// position in a batch. Event.Raw is the canonical JSON of the record as it
// was received and must be treated as read-only.
type Event struct {
	Key     string
	Version Version
	// Raw is the canonical JSON of the record.
	Raw []byte
	// Digest identifies the compared content of the record. Two deliveries
	// with the same key, version and Digest carry the same value.
	Digest string
	// Deliveries is the number of times this exact value was received,
	// including batches before the one that produced the Result.
	Deliveries int
}

// VersionEntry is the state of one version of an order.
type VersionEntry struct {
	Version Version
	// Events holds the distinct values delivered for this version, sorted
	// by Digest. More than one means the version conflicts.
	Events []Event
	// Deliveries is the total number of records received for this version,
	// including batches before the one that produced the Result.
	Deliveries int
	// Final is true when this is the highest version of the order.
	Final bool
	// Superseded is true when a higher version exists for the order.
	Superseded bool
}

// Conflict reports a version that was delivered with more than one value.
// It is left for reconciliation and is never resolved by arrival order.
type Conflict struct {
	Key     string
	Version Version
	// Final is true when the conflicting version is the highest version of
	// the order. In that case the order has no unambiguous final event
	// until the conflict is reconciled.
	Final bool
	// Events holds at least two distinct values, sorted by Digest.
	Events []Event
}

// Entry is a snapshot of the merged state of one order key.
// It is a read-only view; Event.Raw must not be modified.
type Entry struct {
	key       string
	versions  []VersionEntry
	conflicts []Conflict
}

// InvalidRecord reports a record that could not be merged.
// The record is skipped; it does not fail the batch.
type InvalidRecord struct {
	// Index is the zero based record index in the batch. Records of a
	// top-level JSON array are numbered as if the array was flattened.
	Index int
	Err   error
}

// Result is the outcome of one ingested upload batch.
type Result struct {
	// Records is the number of records read from the batch.
	Records int
	// Entries holds the merged state of every order key touched by the
	// batch, sorted by key. Orders that were not part of the batch are not
	// included.
	Entries []*Entry
	// Final holds the final event of every touched order that has an
	// unambiguous highest version. It has at most one event per Entry and
	// the same order as Entries.
	Final []*Event
	// Conflicts holds every outstanding conflict for the touched orders,
	// including conflicts created by earlier batches.
	Conflicts []Conflict
	// Duplicates is the number of records that were exact redeliveries of a
	// value already seen for the same order key and version. Duplicates do
	// not create a version.
	Duplicates int
	// Invalid holds the records that could not be merged.
	Invalid []InvalidRecord
}

// New returns a Ledger applying defaults to any unset Config field.
func New(cfg Config) *Ledger {
	cfg = cfg.withDefaults()
	ignored := make(map[string]struct{}, len(cfg.IgnoreFields))
	for _, f := range cfg.IgnoreFields {
		ignored[f] = struct{}{}
	}
	return &Ledger{
		cfg:     cfg,
		ignored: ignored,
		entries: make(map[string]*entryState),
	}
}

// IngestJSON parses a batch and merges it.
//
// The batch is normally newline delimited JSON. A single JSON document that
// is an array of records is accepted as well. Each record must be an object.
func (l *Ledger) IngestJSON(b []byte) (*Result, error) {
	b = bytes.TrimSpace(b)
	if len(b) == 0 {
		return &Result{}, nil
	}
	pj, err := simdjson.ParseND(b, nil)
	if err != nil {
		// A single document such as a pretty printed array is not valid
		// NDJSON; try it as one document instead.
		pj, err = simdjson.Parse(b, nil)
		if err != nil {
			return nil, err
		}
	}
	return l.IngestParsed(pj)
}

// IngestParsed merges the records of an already parsed batch.
//
// Every top-level value is a record. A top-level array is treated as a
// batch of records. Records that cannot be merged are reported in
// Result.Invalid and do not abort the batch.
func (l *Ledger) IngestParsed(pj *simdjson.ParsedJson) (*Result, error) {
	if pj == nil {
		return nil, ErrNilParsed
	}
	res := &Result{}
	touched := make(map[string]struct{})
	var obj simdjson.Object
	var arr simdjson.Array

	err := pj.ForEach(func(i simdjson.Iter) error {
		switch i.Type() {
		case simdjson.TypeObject:
			o, err := i.Object(&obj)
			if err != nil {
				return err
			}
			l.addObject(res, touched, o)
		case simdjson.TypeArray:
			a, err := i.Array(&arr)
			if err != nil {
				return err
			}
			var fatal error
			a.ForEach(func(elem simdjson.Iter) {
				if fatal != nil {
					return
				}
				if elem.Type() != simdjson.TypeObject {
					res.Records++
					res.Invalid = append(res.Invalid, InvalidRecord{
						Index: res.Records - 1,
						Err:   ErrNotObject,
					})
					return
				}
				o, err := elem.Object(&obj)
				if err != nil {
					fatal = err
					return
				}
				l.addObject(res, touched, o)
			})
			if fatal != nil {
				return fatal
			}
		default:
			res.Records++
			res.Invalid = append(res.Invalid, InvalidRecord{
				Index: res.Records - 1,
				Err:   ErrNotObject,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	res.finish(l, touched)
	return res, nil
}

// addObject merges a single record object.
func (l *Ledger) addObject(res *Result, touched map[string]struct{}, o *simdjson.Object) {
	idx := res.Records
	res.Records++

	m, err := o.Map(nil)
	if err != nil {
		res.Invalid = append(res.Invalid, InvalidRecord{Index: idx, Err: err})
		return
	}

	keyVal, ok := m[l.cfg.KeyField]
	if !ok {
		res.Invalid = append(res.Invalid, InvalidRecord{Index: idx, Err: ErrMissingKey})
		return
	}
	id, err := keyFromValue(keyVal)
	if err != nil {
		res.Invalid = append(res.Invalid, InvalidRecord{Index: idx, Err: err})
		return
	}

	verVal, ok := m[l.cfg.VersionField]
	if !ok {
		res.Invalid = append(res.Invalid, InvalidRecord{Index: idx, Err: ErrMissingVersion})
		return
	}
	version, err := versionFromValue(verVal)
	if err != nil {
		res.Invalid = append(res.Invalid, InvalidRecord{Index: idx, Err: err})
		return
	}

	raw, err := json.Marshal(m)
	if err != nil {
		res.Invalid = append(res.Invalid, InvalidRecord{Index: idx, Err: err})
		return
	}
	digest, err := l.digest(m)
	if err != nil {
		res.Invalid = append(res.Invalid, InvalidRecord{Index: idx, Err: err})
		return
	}

	es := l.entries[id]
	if es == nil {
		es = &entryState{
			key:      id,
			versions: make(map[Version]*versionState),
		}
		l.entries[id] = es
	}
	vs := es.versions[version]
	if vs == nil {
		vs = &versionState{
			version: version,
			events:  make(map[string]*Event),
		}
		es.versions[version] = vs
	}
	if !es.hasMax || version.Compare(es.max) > 0 {
		es.max = version
		es.hasMax = true
	}

	vs.deliveries++
	touched[id] = struct{}{}
	if ev, ok := vs.events[digest]; ok {
		ev.Deliveries++
		res.Duplicates++
		return
	}
	vs.events[digest] = &Event{
		Key:        id,
		Version:    version,
		Raw:        raw,
		Digest:     digest,
		Deliveries: 1,
	}
}

// digest returns an identifier of the compared content of a record.
// The order key, the version and configured ignored fields are excluded, so
// two deliveries that only differ in those fields compare equal.
func (l *Ledger) digest(m map[string]interface{}) (string, error) {
	cmp := make(map[string]interface{}, len(m))
	for k, v := range m {
		if k == l.cfg.KeyField || k == l.cfg.VersionField {
			continue
		}
		if _, skip := l.ignored[k]; skip {
			continue
		}
		cmp[k] = v
	}
	b, err := canonicalValue(nil, cmp)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// keyFromValue returns the identity of an order key.
// The key is its text form, so a JSON string "1" and the integer 1 address
// the same order. Deliveries that disagree about the key type but share the
// text are merged, and any difference in value then surfaces as a conflict
// for reconciliation.
func keyFromValue(v interface{}) (id string, err error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case int64:
		return strconv.FormatInt(t, 10), nil
	case uint64:
		return strconv.FormatUint(t, 10), nil
	}
	return "", fmt.Errorf("%w: got %T", ErrBadKey, v)
}

// finish populates the aggregate fields of a Result.
func (res *Result) finish(l *Ledger, touched map[string]struct{}) {
	ids := make([]string, 0, len(touched))
	for id := range touched {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		e := l.entries[id].snapshot()
		res.Entries = append(res.Entries, e)
		res.Conflicts = append(res.Conflicts, e.conflicts...)
		if ev := e.FinalEvent(); ev != nil {
			res.Final = append(res.Final, ev)
		}
	}
}

// snapshot builds the read-only Entry view.
func (es *entryState) snapshot() *Entry {
	versions := make([]Version, 0, len(es.versions))
	for v := range es.versions {
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].Compare(versions[j]) < 0
	})

	e := &Entry{key: es.key}
	e.versions = make([]VersionEntry, 0, len(versions))
	for _, v := range versions {
		vs := es.versions[v]
		ve := VersionEntry{
			Version:    v,
			Deliveries: vs.deliveries,
			Final:      v.Compare(es.max) == 0,
			Superseded: v.Compare(es.max) < 0,
		}
		digests := make([]string, 0, len(vs.events))
		for d := range vs.events {
			digests = append(digests, d)
		}
		sort.Strings(digests)
		ve.Events = make([]Event, 0, len(digests))
		for _, d := range digests {
			ve.Events = append(ve.Events, *vs.events[d])
		}
		e.versions = append(e.versions, ve)
		if len(ve.Events) > 1 {
			e.conflicts = append(e.conflicts, Conflict{
				Key:     es.key,
				Version: v,
				Final:   ve.Final,
				Events:  ve.Events,
			})
		}
	}
	return e
}

// Key returns the order key as text.
func (e *Entry) Key() string { return e.key }

// Versions returns all versions of the order, in ascending version order.
// The returned slice is a copy; Event.Raw within it is shared and must be
// treated as read-only.
func (e *Entry) Versions() []VersionEntry {
	out := make([]VersionEntry, len(e.versions))
	for i, v := range e.versions {
		out[i] = v
		out[i].Events = append([]Event(nil), v.Events...)
	}
	return out
}

// Superseded returns the versions that were covered by a later version,
// in ascending version order.
func (e *Entry) Superseded() []VersionEntry {
	var out []VersionEntry
	for _, v := range e.versions {
		if v.Superseded {
			out = append(out, v)
		}
	}
	return out
}

// FinalVersion returns the highest version of the order.
func (e *Entry) FinalVersion() (Version, bool) {
	if len(e.versions) == 0 {
		return Version{}, false
	}
	return e.versions[len(e.versions)-1].Version, true
}

// FinalEvent returns the event to upload for this order.
//
// It is the single value of the highest version. It returns nil when the
// highest version conflicts, because then no value can be chosen without
// reconciliation.
func (e *Entry) FinalEvent() *Event {
	if len(e.versions) == 0 {
		return nil
	}
	last := &e.versions[len(e.versions)-1]
	if len(last.Events) != 1 {
		return nil
	}
	return &last.Events[0]
}

// Conflicts returns the conflicting versions of this order.
func (e *Entry) Conflicts() []Conflict {
	out := make([]Conflict, len(e.conflicts))
	for i, c := range e.conflicts {
		out[i] = c
		out[i].Events = append([]Event(nil), c.Events...)
	}
	return out
}

// Len returns the number of distinct order keys known to the ledger.
func (l *Ledger) Len() int { return len(l.entries) }

// Lookup returns the merged state of an order key given as a string.
// It returns nil if the key is unknown.
func (l *Ledger) Lookup(key string) *Entry {
	es := l.entries[key]
	if es == nil {
		return nil
	}
	return es.snapshot()
}

// Reset forgets all merged state.
func (l *Ledger) Reset() {
	l.entries = make(map[string]*entryState)
}
