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
	"fmt"
	"unicode/utf8"
)

// Row is one row produced when an array is expanded into a sequence of rows.
//
// Expanding the "items" array of
//
//	{"order":"A-1","items":[{"sku":"a"},{"sku":"b"}]}
//
// produces two rows. Both rows carry ParentKey "items", EventIndex 0 and the
// item index of the element they were expanded from.
//
// Rows reference the parsed tape. The ParsedJson they were produced from must
// stay allocated (and must not be reused) for as long as the rows are used.
type Row struct {
	// ParentKey is the object key the expanded array was stored under.
	// It is empty when the expanded value is a root array and no
	// ExpandParentKey option was given.
	ParentKey string

	// EventIndex is the zero-based index of the root element (the event)
	// the row was expanded from. It is 0 unless the rows were produced by
	// ParsedJson.Expand/ExpandEach or an ExpandEventIndex option was given.
	EventIndex int

	// ItemIndex is the zero-based index of the element within the expanded
	// array. It is -1 for a row that represents the parent itself rather
	// than an array element: an empty array kept with ExpandKeepEmptyParent,
	// a null value, a missing key, or a value that is not an array.
	ItemIndex int

	// Parent is an iterator positioned at the expanded container (the object
	// or array holding the value). It can be used with Object or FindKey to
	// read the other fields of the parent row.
	Parent Iter

	// Item is an iterator to the array element this row was expanded from.
	// For rows with ItemIndex -1 it holds the original value (the empty
	// array, the null, or the offending value). It is the zero Iter when the
	// key was missing from the parent object.
	Item Iter

	// Err is non-nil if the row is a bad item, see BadItem.
	// Bad rows are still emitted so the caller can report them with an
	// exact location instead of aborting the whole expansion.
	Err error
}

// Bad reports whether the row is a bad item.
func (r Row) Bad() bool { return r.Err != nil }

// BadItem returns the BadItem for a bad row, or nil if the row is valid.
func (r Row) BadItem() *BadItem {
	b, _ := r.Err.(*BadItem)
	return b
}

// Location returns a human readable location of the row, for example
// "event[1].items[3]". It is suitable for error messages and reports.
func (r Row) Location() string {
	return rowLocation(r.EventIndex, r.ParentKey, r.ItemIndex)
}

// Value returns the row item as an interface{}.
// See Iter.Interface for the value mapping.
// Bad rows return the BadItem as an error. Rows with no item return nil.
func (r Row) Value() (interface{}, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	if r.Item.tape.Tape == nil {
		return nil, nil
	}
	return r.Item.Interface()
}

func rowLocation(eventIndex int, parentKey string, itemIndex int) string {
	loc := fmt.Sprintf("event[%d]", eventIndex)
	if parentKey != "" {
		loc += "." + parentKey
	}
	if itemIndex >= 0 {
		loc += fmt.Sprintf("[%d]", itemIndex)
	}
	return loc
}

// BadItem describes a location that could not be expanded because the value
// did not have the expected type.
//
// A bad item is returned as Row.Err for every offending element; expansion
// continues with the remaining elements, so a single bad item does not hide
// the rest of the report.
type BadItem struct {
	// ParentKey is the object key of the value that could not be expanded.
	ParentKey string

	// EventIndex is the zero-based index of the root element (event).
	EventIndex int

	// ItemIndex is the zero-based index of the offending element within the
	// array, or -1 if the value itself was not an array.
	ItemIndex int

	// Want is the type that was expected.
	Want Type

	// Got is the type that was found.
	Got Type

	// Value is a JSON preview of the offending value, truncated to keep
	// error messages readable. It may be empty for values that cannot be
	// serialized.
	Value string
}

// Path returns the location of the bad item, for example "event[1].items[3]".
func (b *BadItem) Path() string {
	return rowLocation(b.EventIndex, b.ParentKey, b.ItemIndex)
}

// Error returns a message with an unambiguous location, the expected type and
// the type that was found.
func (b *BadItem) Error() string {
	msg := fmt.Sprintf("simdjson: bad item at %s: got %s, want %s", b.Path(), b.Got, b.Want)
	// Do not repeat the type name as a value preview.
	if b.Value != "" && b.Value != b.Got.String() {
		msg += " (" + b.Value + ")"
	}
	return msg
}

// ExpandOption configures Expand and ExpandEach.
type ExpandOption func(*expandConfig)

type expandConfig struct {
	keepEmptyParent bool
	wantItem        Type
	eventIndex      int
	parentKey       string
}

func newExpandConfig(opts []ExpandOption) expandConfig {
	var cfg expandConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

// ExpandKeepEmptyParent controls whether a parent row is kept when the array
// being expanded is empty (or null, or missing).
//
// When keep is false (the default) an empty array produces no rows, matching
// an inner join. When keep is true one row with ItemIndex -1 is emitted for
// the parent, so events without items are not lost from the report.
func ExpandKeepEmptyParent(keep bool) ExpandOption {
	return func(cfg *expandConfig) {
		cfg.keepEmptyParent = keep
	}
}

// ExpandItemType requires every expanded array element to have the given
// type, usually TypeObject for nested records.
// Elements of another type produce a BadItem row. TypeNone accepts any type
// and is the default.
func ExpandItemType(t Type) ExpandOption {
	return func(cfg *expandConfig) {
		cfg.wantItem = t
	}
}

// ExpandEventIndex sets the event index recorded on the emitted rows.
// It is used when expanding a single object or array that is known to be
// event number i of a larger document. ParsedJson.Expand and
// ParsedJson.ExpandEach set the event index themselves.
func ExpandEventIndex(i int) ExpandOption {
	return func(cfg *expandConfig) {
		cfg.eventIndex = i
	}
}

// ExpandParentKey sets the parent key recorded on rows expanded from an array
// that is not stored under an object key, for example a root array or an
// array reached by a path. Object.Expand and ParsedJson.Expand override it
// with the actual key.
func ExpandParentKey(key string) ExpandOption {
	return func(cfg *expandConfig) {
		cfg.parentKey = key
	}
}

// WithKeepEmptyParent is an alias for ExpandKeepEmptyParent.
func WithKeepEmptyParent(keep bool) ExpandOption { return ExpandKeepEmptyParent(keep) }

// WithItemType is an alias for ExpandItemType.
func WithItemType(t Type) ExpandOption { return ExpandItemType(t) }

// WithEventIndex is an alias for ExpandEventIndex.
func WithEventIndex(i int) ExpandOption { return ExpandEventIndex(i) }

// WithParentKey is an alias for ExpandParentKey.
func WithParentKey(key string) ExpandOption { return ExpandParentKey(key) }

// Expand appends one row per element of the named array fields of the object.
// It returns the extended destination slice.
//
// If keys is non-empty only the named fields are expanded. A named field that
// is missing, null or not an array is not silently dropped: with
// ExpandKeepEmptyParent it yields a parent row, and a value of the wrong type
// yields a BadItem row. If keys is empty every array-valued field is expanded
// and other fields are ignored.
//
// The object is not advanced and can be used again.
func (o *Object) Expand(dst []Row, keys []string, opts ...ExpandOption) ([]Row, error) {
	err := o.ExpandEach(keys, func(row Row) error {
		dst = append(dst, row)
		return nil
	}, opts...)
	return dst, err
}

// ExpandEach calls fn for each row produced by expanding the named array
// fields of the object. If fn returns an error, expansion stops and the error
// is returned.
//
// See Expand for how keys, missing keys and values of the wrong type are
// handled. The object is not advanced.
func (o *Object) ExpandEach(keys []string, fn func(Row) error, opts ...ExpandOption) error {
	return o.expandEach(keys, fn, newExpandConfig(opts))
}

// Expand appends one row per element of the array and returns the extended
// destination slice.
//
// The array itself has no key; use ExpandParentKey to record the key it was
// stored under. The array is not advanced and can be used again.
func (a *Array) Expand(dst []Row, opts ...ExpandOption) ([]Row, error) {
	err := a.ExpandEach(func(row Row) error {
		dst = append(dst, row)
		return nil
	}, opts...)
	return dst, err
}

// ExpandEach calls fn for each row produced by expanding the array.
// If fn returns an error, expansion stops and the error is returned.
//
// An empty array emits no rows unless ExpandKeepEmptyParent is set, in which
// case a single parent row with ItemIndex -1 is emitted.
func (a *Array) ExpandEach(fn func(Row) error, opts ...ExpandOption) error {
	cfg := newExpandConfig(opts)
	return a.expand(cfg.parentKey, arrayAsIter(a), cfg, fn)
}

// Expand appends one row per element of the named array field of every root
// element (event) in the parsed document, or of the document itself.
// EventIndex is set to the zero-based position of the root element.
//
// See Object.Expand for how keys, missing keys and values of the wrong type
// are handled.
func (pj *ParsedJson) Expand(dst []Row, keys []string, opts ...ExpandOption) ([]Row, error) {
	err := pj.ExpandEach(keys, func(row Row) error {
		dst = append(dst, row)
		return nil
	}, opts...)
	return dst, err
}

// ExpandEach calls fn for each row produced by expanding the named array
// fields of every root element (event) in the parsed document.
//
// A root that is itself an array is expanded as well; its rows have the key
// given with ExpandParentKey (empty by default). A root that is neither an
// object nor an array produces a BadItem row when keys is empty or names a
// field.
func (pj *ParsedJson) ExpandEach(keys []string, fn func(Row) error, opts ...ExpandOption) error {
	base := newExpandConfig(opts)
	var elem Iter
	it := pj.Iter()
	for eventIndex := 0; ; eventIndex++ {
		t, err := it.AdvanceIter(&elem)
		if err != nil {
			return err
		}
		if t == TypeNone {
			return nil
		}
		cfg := base
		cfg.eventIndex = eventIndex

		// Move into the root to get the contained value.
		_, root, err := elem.Root(nil)
		if err != nil {
			return err
		}
		switch root.t.Type() {
		case TypeObject:
			o, err := root.Object(nil)
			if err != nil {
				return err
			}
			if err := o.expandEach(keys, fn, cfg); err != nil {
				return err
			}
		case TypeArray:
			a, err := root.Array(nil)
			if err != nil {
				return err
			}
			if err := a.expand(cfg.parentKey, arrayAsIter(a), cfg, fn); err != nil {
				return err
			}
		default:
			// A scalar root cannot be expanded.
			row := Row{
				ParentKey:  cfg.parentKey,
				EventIndex: eventIndex,
				ItemIndex:  -1,
				Item:       *root,
				Err: &BadItem{
					ParentKey:  cfg.parentKey,
					EventIndex: eventIndex,
					ItemIndex:  -1,
					Want:       TypeArray,
					Got:        root.t.Type(),
					Value:      previewJSON(*root),
				},
			}
			if err := fn(row); err != nil {
				return err
			}
		}
	}
}

// expandEach is the non-exported entry used by ParsedJson so the object's
// event index and parent iterator are set up once.
func (o *Object) expandEach(keys []string, fn func(Row) error, cfg expandConfig) error {
	parent := objectAsIter(o)

	var wanted map[string]struct{}
	if len(keys) > 0 {
		wanted = make(map[string]struct{}, len(keys))
		for _, k := range keys {
			wanted[k] = struct{}{}
		}
	}
	found := make(map[string]struct{}, len(wanted))

	tmp := *o
	var it Iter
	for {
		name, t, err := tmp.NextElementBytes(&it)
		if err != nil {
			return err
		}
		if t == TypeNone {
			break
		}
		key := string(name)
		if wanted != nil {
			if _, ok := wanted[key]; !ok {
				continue
			}
			found[key] = struct{}{}
		} else if t != TypeArray {
			// Without explicit keys only arrays are expanded.
			continue
		}
		if err := expandValue(&it, key, parent, cfg, fn); err != nil {
			return err
		}
	}
	if wanted != nil && cfg.keepEmptyParent {
		seen := make(map[string]struct{}, len(wanted))
		for _, k := range keys {
			if _, ok := wanted[k]; !ok {
				continue
			}
			if _, ok := found[k]; ok {
				continue
			}
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			row := Row{ParentKey: k, EventIndex: cfg.eventIndex, ItemIndex: -1, Parent: parent}
			if err := fn(row); err != nil {
				return err
			}
		}
	}
	return nil
}

// expandValue expands v, which is the value stored under parentKey, into rows.
func expandValue(v *Iter, parentKey string, parent Iter, cfg expandConfig, fn func(Row) error) error {
	switch v.t.Type() {
	case TypeArray:
		a, err := v.Array(nil)
		if err != nil {
			return err
		}
		return a.expand(parentKey, parent, cfg, fn)
	case TypeNull:
		if !cfg.keepEmptyParent {
			return nil
		}
		return fn(Row{
			ParentKey:  parentKey,
			EventIndex: cfg.eventIndex,
			ItemIndex:  -1,
			Parent:     parent,
			Item:       *v,
		})
	default:
		got := v.t.Type()
		return fn(Row{
			ParentKey:  parentKey,
			EventIndex: cfg.eventIndex,
			ItemIndex:  -1,
			Parent:     parent,
			Item:       *v,
			Err: &BadItem{
				ParentKey:  parentKey,
				EventIndex: cfg.eventIndex,
				ItemIndex:  -1,
				Want:       TypeArray,
				Got:        got,
				Value:      previewJSON(*v),
			},
		})
	}
}

// expand emits one row per array element.
func (a *Array) expand(parentKey string, parent Iter, cfg expandConfig, fn func(Row) error) error {
	it := a.Iter()
	var elem Iter
	itemIndex := 0
	for {
		t, err := it.AdvanceIter(&elem)
		if err != nil {
			return err
		}
		if t == TypeNone {
			break
		}
		row := Row{
			ParentKey:  parentKey,
			EventIndex: cfg.eventIndex,
			ItemIndex:  itemIndex,
			Parent:     parent,
			Item:       elem,
		}
		if cfg.wantItem != TypeNone && t != cfg.wantItem {
			row.Err = &BadItem{
				ParentKey:  parentKey,
				EventIndex: cfg.eventIndex,
				ItemIndex:  itemIndex,
				Want:       cfg.wantItem,
				Got:        t,
				Value:      previewJSON(elem),
			}
		}
		if err := fn(row); err != nil {
			return err
		}
		itemIndex++
	}
	if itemIndex == 0 && cfg.keepEmptyParent {
		return fn(Row{
			ParentKey:  parentKey,
			EventIndex: cfg.eventIndex,
			ItemIndex:  -1,
			Parent:     parent,
			Item:       arrayAsIter(a),
		})
	}
	return nil
}

// objectAsIter returns an iterator positioned at the container object.
// It is suitable for Object, FindKey and FindPath.
func objectAsIter(o *Object) Iter {
	return Iter{
		tape: o.tape,
		off:  o.off,
		t:    TagObjectStart,
		cur:  uint64(len(o.tape.Tape)),
	}
}

// arrayAsIter returns an iterator positioned at the first array element.
func arrayAsIter(a *Array) Iter {
	return Iter{
		tape: a.tape,
		off:  a.off,
		t:    TagArrayStart,
		cur:  uint64(len(a.tape.Tape)),
	}
}

// previewJSON renders a short JSON preview of a value for error messages.
// Containers are summarized by kind since serializing them could be large and
// a bad item is usually a scalar.
func previewJSON(i Iter) string {
	switch i.t {
	case TagObjectStart:
		return "object"
	case TagArrayStart:
		return "array"
	}
	const maxLen = 48
	b, err := i.MarshalJSON()
	if err != nil {
		return ""
	}
	if len(b) <= maxLen {
		return string(b)
	}
	// Do not cut in the middle of a UTF-8 sequence.
	cut := maxLen
	for cut > 0 && !utf8.RuneStart(b[cut]) {
		cut--
	}
	return string(b[:cut]) + "..."
}
