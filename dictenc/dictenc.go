// Package dictenc builds cumulative business dictionaries from parsed
// simdjson streams and encodes high-repetition text dimensions
// (channel, region) as unified dictionary IDs.
//
// Problem: events carry channel/region labels that repeat heavily, and
// naive per-batch dictionary encoding gives every batch its own local ID
// space, so the same label gets a different ID in every batch.
//
// dictenc instead keeps one append-only dictionary per column that
// accumulates across batches: a label's unified ID is assigned once and
// never changes. Encoding a batch yields fact columns of unified IDs plus
// a dictionary increment holding only the labels first seen in that batch,
// so downstream stores apply a small delta instead of a full snapshot.
//
// Value semantics, preserved exactly:
//   - JSON null (or a missing key): the fact cell is null (NullID); the
//     dictionary is not touched and no increment entry is emitted.
//   - Empty string: a real label with its own unified ID, distinct from
//     null.
//   - A label seen for the first time: appended to the dictionary and
//     reported in the batch increment.
//
// Determinism: new labels within a batch are sorted before IDs are
// allocated, so re-running the same batch version against the same
// dictionary state always produces the same mapping, regardless of event
// order inside the batch. Re-running a version whose labels were already
// absorbed yields the same fact IDs and an empty increment.
//
// An Encoder is not safe for concurrent use; encode batches sequentially
// in version order.
package dictenc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"

	simdjson "github.com/minio/simdjson-go"
)

// NullID marks a null (or absent) source value in a FactColumn.
// It is reserved: a dictionary can hold at most NullID-1 entries.
const NullID = ^uint32(0)

// DefaultColumns are encoded when NewEncoder is called without columns.
var DefaultColumns = []string{"channel", "region"}

// Dictionary is the cumulative, append-only business dictionary for one
// column. The unified ID of an entry is its index in Entries.
type Dictionary struct {
	Entries []string `json:"entries"`
	ids     map[string]uint32
}

func newDictionary() *Dictionary {
	return &Dictionary{ids: make(map[string]uint32)}
}

// ID returns the unified ID of value, and whether it is present.
func (d *Dictionary) ID(value string) (uint32, bool) {
	id, ok := d.ids[value]
	return id, ok
}

// Value returns the label stored under id, and whether it exists.
func (d *Dictionary) Value(id uint32) (string, bool) {
	if uint64(id) >= uint64(len(d.Entries)) {
		return "", false
	}
	return d.Entries[id], true
}

// Len returns the number of entries; the next allocated ID.
func (d *Dictionary) Len() int { return len(d.Entries) }

// add appends a label known to be new and returns its unified ID.
func (d *Dictionary) add(value string) uint32 {
	id := uint32(len(d.Entries))
	d.Entries = append(d.Entries, value)
	d.ids[value] = id
	return id
}

// reindex rebuilds the reverse index after loading and validates entries.
func (d *Dictionary) reindex() error {
	if uint64(len(d.Entries)) >= uint64(NullID) {
		return errors.New("dictenc: dictionary exhausted the ID space")
	}
	d.ids = make(map[string]uint32, len(d.Entries))
	for i, e := range d.Entries {
		if _, dup := d.ids[e]; dup {
			return fmt.Errorf("dictenc: duplicate dictionary entry %q", e)
		}
		d.ids[e] = uint32(i)
	}
	return nil
}

// Entry is one dictionary addition: a label and the unified ID it got.
type Entry struct {
	ID    uint32 `json:"id"`
	Value string `json:"value"`
}

// Increment holds the dictionary entries first seen in one batch.
// Applying it to the previous dictionary state reproduces the new state.
type Increment struct {
	Version string             `json:"version"`
	Columns map[string][]Entry `json:"columns"`
}

// Empty reports whether the increment adds no entries, e.g. when a batch
// version is re-run after its labels were already absorbed.
func (inc *Increment) Empty() bool {
	for _, es := range inc.Columns {
		if len(es) > 0 {
			return false
		}
	}
	return true
}

// WriteJSON writes the increment as deterministic JSON (keys sorted).
func (inc *Increment) WriteJSON(w io.Writer) error {
	b, err := json.Marshal(inc)
	if err != nil {
		return err
	}
	_, err = w.Write(append(b, '\n'))
	return err
}

// FactColumn is one encoded column of a batch: one unified ID per event,
// NullID where the source value was null or the key absent.
type FactColumn struct {
	Name string
	IDs  []uint32
}

// Batch is the result of encoding one batch version.
type Batch struct {
	Version   string
	Rows      int
	Facts     []FactColumn
	Increment *Increment
}

// Fact returns the encoded column by name, or nil if unknown.
func (b *Batch) Fact(column string) *FactColumn {
	for i := range b.Facts {
		if b.Facts[i].Name == column {
			return &b.Facts[i]
		}
	}
	return nil
}

// WriteFactsNDJSON writes the fact columns as NDJSON, one object per
// event with "<column>_id" keys holding the unified ID or null.
// Columns are emitted in configured order, so output is deterministic.
func (b *Batch) WriteFactsNDJSON(w io.Writer) error {
	var line []byte
	for r := 0; r < b.Rows; r++ {
		line = line[:0]
		line = append(line, '{')
		for i := range b.Facts {
			if i > 0 {
				line = append(line, ',')
			}
			line = append(line, strconv.Quote(b.Facts[i].Name+"_id")...)
			line = append(line, ':')
			id := b.Facts[i].IDs[r]
			if id == NullID {
				line = append(line, "null"...)
			} else {
				line = strconv.AppendUint(line, uint64(id), 10)
			}
		}
		line = append(line, '}', '\n')
		if _, err := w.Write(line); err != nil {
			return err
		}
	}
	return nil
}

// Encoder holds the cumulative dictionaries for all encoded columns.
// It is the state that must be persisted between batches (see Save/Load).
type Encoder struct {
	cols  []string
	dicts map[string]*Dictionary
}

// NewEncoder returns an Encoder with empty dictionaries for the given
// columns, or DefaultColumns when none are given. It panics on empty or
// duplicate column names.
func NewEncoder(columns ...string) *Encoder {
	if len(columns) == 0 {
		columns = DefaultColumns
	}
	e := &Encoder{dicts: make(map[string]*Dictionary, len(columns))}
	for _, c := range columns {
		if c == "" {
			panic("dictenc: empty column name")
		}
		if _, dup := e.dicts[c]; dup {
			panic("dictenc: duplicate column " + strconv.Quote(c))
		}
		e.cols = append(e.cols, c)
		e.dicts[c] = newDictionary()
	}
	return e
}

// Columns returns the encoded column names in configured order.
func (e *Encoder) Columns() []string { return append([]string(nil), e.cols...) }

// Dictionary returns the cumulative dictionary for a column.
func (e *Encoder) Dictionary(column string) (*Dictionary, bool) {
	d, ok := e.dicts[column]
	return d, ok
}

// EncodeBatch encodes one batch version from already parsed JSON.
//
// All pjs are treated as chunks of the same logical batch (same version):
// pass every chunk of a version in one call so its increment is complete.
// The encoder's dictionaries are updated in place; the returned Batch
// holds the fact columns and the dictionary increment for this version.
func (e *Encoder) EncodeBatch(version string, pjs ...*simdjson.ParsedJson) (*Batch, error) {
	colIndex := make(map[string]int, len(e.cols))
	onlyKeys := make(map[string]struct{}, len(e.cols))
	for i, c := range e.cols {
		colIndex[c] = i
		onlyKeys[c] = struct{}{}
	}

	// Pass 1: extract per-row values and collect labels not yet in the
	// cumulative dictionaries. nil means null (or absent key).
	rows := make([][]*string, 0, 1024)
	newLabels := make([]map[string]struct{}, len(e.cols))
	for i := range newLabels {
		newLabels[i] = make(map[string]struct{})
	}
	for _, pj := range pjs {
		if pj == nil {
			continue
		}
		var obj *simdjson.Object
		err := pj.ForEach(func(i simdjson.Iter) error {
			var err error
			obj, err = i.Object(obj)
			if err != nil {
				return fmt.Errorf("dictenc: event %d: %w", len(rows), err)
			}
			rec := make([]*string, len(e.cols))
			var valErr error
			err = obj.ForEach(func(key []byte, v simdjson.Iter) {
				ci, ok := colIndex[string(key)]
				if !ok {
					return
				}
				switch v.Type() {
				case simdjson.TypeString:
					b, err := v.StringBytes()
					if err != nil {
						valErr = err
						return
					}
					s := string(b)
					rec[ci] = &s
					if _, known := e.dicts[e.cols[ci]].ID(s); !known {
						newLabels[ci][s] = struct{}{}
					}
				case simdjson.TypeNull:
					// Explicit null: keep nil, dictionary untouched.
				default:
					valErr = fmt.Errorf("column %q: expected string or null, got %s", e.cols[ci], v.Type())
				}
			}, onlyKeys)
			if err != nil {
				return fmt.Errorf("dictenc: event %d: %w", len(rows), err)
			}
			if valErr != nil {
				return fmt.Errorf("dictenc: event %d: %w", len(rows), valErr)
			}
			rows = append(rows, rec)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	// Allocate IDs for new labels in sorted order: given the same
	// dictionary state, any re-run of this batch version maps labels
	// identically, independent of event order.
	inc := &Increment{Version: version, Columns: make(map[string][]Entry, len(e.cols))}
	for ci, col := range e.cols {
		inc.Columns[col] = []Entry{}
		if len(newLabels[ci]) == 0 {
			continue
		}
		labels := make([]string, 0, len(newLabels[ci]))
		for l := range newLabels[ci] {
			labels = append(labels, l)
		}
		sort.Strings(labels)
		d := e.dicts[col]
		for _, l := range labels {
			inc.Columns[col] = append(inc.Columns[col], Entry{ID: d.add(l), Value: l})
		}
	}

	// Pass 2: resolve unified IDs for every row.
	b := &Batch{Version: version, Rows: len(rows), Facts: make([]FactColumn, len(e.cols)), Increment: inc}
	for ci, col := range e.cols {
		fc := FactColumn{Name: col, IDs: make([]uint32, len(rows))}
		d := e.dicts[col]
		for r, rec := range rows {
			if rec[ci] == nil {
				fc.IDs[r] = NullID
				continue
			}
			id, ok := d.ID(*rec[ci])
			if !ok {
				return nil, fmt.Errorf("dictenc: internal error: label %q of column %q not in dictionary", *rec[ci], col)
			}
			fc.IDs[r] = id
		}
		b.Facts[ci] = fc
	}
	return b, nil
}

// stateFormat is the persisted dictionary format version.
const stateFormat = 1

type stateFile struct {
	Format  int                 `json:"format"`
	Columns map[string][]string `json:"columns"`
}

// Save writes the cumulative dictionaries to w as deterministic JSON.
// Unified IDs are the implicit entry indexes, so the mapping is stable
// across save/load cycles.
func (e *Encoder) Save(w io.Writer) error {
	sf := stateFile{Format: stateFormat, Columns: make(map[string][]string, len(e.cols))}
	for _, c := range e.cols {
		sf.Columns[c] = e.dicts[c].Entries
	}
	b, err := json.Marshal(sf)
	if err != nil {
		return err
	}
	_, err = w.Write(append(b, '\n'))
	return err
}

// Load restores an Encoder previously written with Save.
// The column set is taken from the state and must be non-empty.
func Load(r io.Reader) (*Encoder, error) {
	var sf stateFile
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&sf); err != nil {
		return nil, fmt.Errorf("dictenc: loading state: %w", err)
	}
	if sf.Format != stateFormat {
		return nil, fmt.Errorf("dictenc: unsupported state format %d", sf.Format)
	}
	if len(sf.Columns) == 0 {
		return nil, errors.New("dictenc: state has no columns")
	}
	cols := make([]string, 0, len(sf.Columns))
	for c := range sf.Columns {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	e := &Encoder{cols: cols, dicts: make(map[string]*Dictionary, len(cols))}
	for _, c := range cols {
		d := &Dictionary{Entries: sf.Columns[c]}
		if err := d.reindex(); err != nil {
			return nil, fmt.Errorf("dictenc: column %q: %w", c, err)
		}
		e.dicts[c] = d
	}
	return e, nil
}
