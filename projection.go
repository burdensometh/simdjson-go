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
	"math"
)

// ColumnType is the fixed output type of a projected column.
// The declared type decides which JSON values are accepted;
// values of any other JSON type are recorded as FieldTypeMismatch.
type ColumnType uint8

const (
	// ColumnString accepts JSON strings only.
	ColumnString ColumnType = iota
	// ColumnInt64 accepts JSON integers that fit in an int64.
	// Both plain integers and unsigned integers are accepted.
	// JSON floats (including values written as 1.0) are a type mismatch.
	ColumnInt64
	// ColumnUint64 accepts JSON integers that fit in a uint64 and are not negative.
	ColumnUint64
	// ColumnFloat64 accepts any JSON number (integer or float).
	ColumnFloat64
	// ColumnBool accepts JSON booleans only.
	ColumnBool
)

// String returns the name of the column type.
func (t ColumnType) String() string {
	switch t {
	case ColumnString:
		return "string"
	case ColumnInt64:
		return "int64"
	case ColumnUint64:
		return "uint64"
	case ColumnFloat64:
		return "float64"
	case ColumnBool:
		return "bool"
	}
	return "(invalid)"
}

func (t ColumnType) valid() bool {
	return t <= ColumnBool
}

// Column declares one output column of a Projection.
type Column struct {
	// Name is the output column name. It must be non-empty and unique.
	Name string
	// Path is the object path to the value, starting at the row object.
	// Only object nesting is traversed; arrays are not descended into.
	Path []string
	// Type is the fixed output type of the column.
	Type ColumnType
}

// FieldState records what was found for a single cell.
type FieldState uint8

const (
	// FieldMissing means the declared path was not present in the row,
	// or an intermediate path element was not an object.
	FieldMissing FieldState = iota
	// FieldNull means the path resolved to an explicit JSON null.
	FieldNull
	// FieldTypeMismatch means the path resolved to a value, but its JSON
	// type cannot be represented by the declared column type.
	FieldTypeMismatch
	// FieldOK means the path resolved to a value of the declared type.
	FieldOK
)

// String returns the name of the field state.
func (s FieldState) String() string {
	switch s {
	case FieldMissing:
		return "missing"
	case FieldNull:
		return "null"
	case FieldTypeMismatch:
		return "type-mismatch"
	case FieldOK:
		return "ok"
	}
	return "(invalid)"
}

// RowState records the shape of a row root.
type RowState uint8

const (
	// RowObject means the row was a JSON object and paths were resolved against it.
	RowObject RowState = iota
	// RowNotObject means the row was a scalar or an array. All cells of the
	// row are FieldMissing since no object path can resolve.
	RowNotObject
)

// String returns the name of the row state.
func (s RowState) String() string {
	switch s {
	case RowObject:
		return "object"
	case RowNotObject:
		return "not-object"
	}
	return "(invalid)"
}

// ColumnData holds the values and per-cell states of one output column.
//
// Values are stored column-major and are aligned with the rows of the
// Projection: index i always refers to row i. Exactly one of the typed
// slices is populated, matching Column.Type. That slice has the same length
// as States, and both are exactly Projection.Rows() long. Cells that are
// missing, null or mismatched keep their zero value, so positions stay
// aligned for batch writes.
type ColumnData struct {
	Column Column
	// States has one entry per row.
	States []FieldState
	// Strings holds values for ColumnString columns.
	Strings []string
	// Ints holds values for ColumnInt64 columns.
	Ints []int64
	// Uints holds values for ColumnUint64 columns.
	Uints []uint64
	// Floats holds values for ColumnFloat64 columns.
	Floats []float64
	// Bools holds values for ColumnBool columns.
	Bools []bool
}

// pathNode is a trie node of declared paths.
// Only branches that lead to a declared column are traversed.
type pathNode struct {
	children map[string]*pathNode
	// cols are columns whose path ends exactly at this node.
	cols []int
	// lastRow is the sequence number of the last row that resolved this
	// node. Used to make the first occurrence of a duplicated key win.
	lastRow uint64
}

func (n *pathNode) child(name string) *pathNode {
	if n.children == nil {
		return nil
	}
	return n.children[name]
}

// ProjectionOption configures a Projection.
type ProjectionOption func(*Projection)

// WithProjectionCopyStrings controls whether projected strings are copied
// out of the parsed JSON buffer.
// Copying (the default) makes the Projection independent of the ParsedJson
// and safe to keep when the ParsedJson or its buffers are reused for later
// batches. Disabling copying avoids one allocation per string, but the
// returned strings then point into the ParsedJson buffers and are only valid
// as long as those buffers are neither modified nor reused.
func WithProjectionCopyStrings(copy bool) ProjectionOption {
	return func(p *Projection) {
		p.copyStrings = copy
	}
}

// Projection reads selected, deeply nested fields of JSON objects into
// column-major typed output columns.
//
// Columns are declared up front with a name, a field path and a fixed type.
// Reading a parsed document only traverses the object branches needed to
// resolve the declared paths; all other keys and subtrees are skipped
// without being decoded.
//
// For every row and every column one FieldState is recorded, distinguishing
// a missing path, an explicit null and a value whose JSON type does not match
// the declared column type from a successfully extracted value.
//
// Row order is the order in which roots (or array elements, with ReadArray)
// appear in the parsed input. Column order is the declaration order. Both can
// be inspected by the caller with Rows, RowStates and Columns, which makes
// the result suitable for subsequent batch writes.
//
// A Projection is not safe for concurrent use.
type Projection struct {
	cols        []Column
	data        []ColumnData
	index       map[string]int
	root        *pathNode
	rows        int
	rowStates   []RowState
	copyStrings bool
	// seq is a monotonically increasing row sequence used to detect the
	// first occurrence of duplicated keys. It survives Reset.
	seq uint64
}

// NewProjection returns a Projection for the given columns.
// Column names must be non-empty and unique, paths must have at least one
// element and types must be valid.
// An optional destination cannot be reused here; use Reset for that.
func NewProjection(columns []Column, opts ...ProjectionOption) (*Projection, error) {
	if len(columns) == 0 {
		return nil, errors.New("simdjson: projection requires at least one column")
	}
	p := &Projection{
		cols:        make([]Column, 0, len(columns)),
		data:        make([]ColumnData, len(columns)),
		index:       make(map[string]int, len(columns)),
		root:        &pathNode{},
		copyStrings: true,
	}
	for _, opt := range opts {
		opt(p)
	}
	for i, c := range columns {
		if c.Name == "" {
			return nil, fmt.Errorf("simdjson: column %d: name must not be empty", i)
		}
		if _, ok := p.index[c.Name]; ok {
			return nil, fmt.Errorf("simdjson: duplicate column name %q", c.Name)
		}
		if len(c.Path) == 0 {
			return nil, fmt.Errorf("simdjson: column %q: path must have at least one element", c.Name)
		}
		if !c.Type.valid() {
			return nil, fmt.Errorf("simdjson: column %q: invalid type %d", c.Name, c.Type)
		}
		// Copy the path so later caller changes cannot desync it from the trie.
		c.Path = append([]string(nil), c.Path...)
		p.index[c.Name] = i
		p.cols = append(p.cols, c)
		p.data[i].Column = c

		n := p.root
		for _, seg := range c.Path {
			child := n.child(seg)
			if child == nil {
				if n.children == nil {
					n.children = make(map[string]*pathNode)
				}
				child = &pathNode{}
				n.children[seg] = child
			}
			n = child
		}
		n.cols = append(n.cols, i)
	}
	return p, nil
}

// Rows returns the number of rows read so far.
func (p *Projection) Rows() int {
	return p.rows
}

// RowStates returns the shape of each row, in row order.
// The returned slice is owned by the Projection and must not be modified.
func (p *Projection) RowStates() []RowState {
	return p.rowStates
}

// Columns returns the declared columns in output column order.
// The returned slice is owned by the Projection and must not be modified.
func (p *Projection) Columns() []Column {
	return p.cols
}

// Column returns the data for the column at output position i,
// or nil if i is out of range.
func (p *Projection) Column(i int) *ColumnData {
	if i < 0 || i >= len(p.data) {
		return nil
	}
	return &p.data[i]
}

// ColumnByName returns the data for the named column,
// or nil if no such column was declared.
func (p *Projection) ColumnByName(name string) *ColumnData {
	i, ok := p.index[name]
	if !ok {
		return nil
	}
	return &p.data[i]
}

// Reset clears all rows while keeping the declared columns and allocated
// buffers for reuse.
func (p *Projection) Reset() {
	p.rows = 0
	p.rowStates = p.rowStates[:0]
	for i := range p.data {
		p.data[i].States = p.data[i].States[:0]
		p.data[i].Strings = p.data[i].Strings[:0]
		p.data[i].Ints = p.data[i].Ints[:0]
		p.data[i].Uints = p.data[i].Uints[:0]
		p.data[i].Floats = p.data[i].Floats[:0]
		p.data[i].Bools = p.data[i].Bools[:0]
	}
}

// Read appends one row per top-level element of pj, in input order.
// This matches the layout produced by ParseND and also accepts the single
// root produced by Parse.
func (p *Projection) Read(pj *ParsedJson) error {
	if pj == nil {
		return errors.New("simdjson: Read called with nil ParsedJson")
	}
	return pj.ForEach(func(i Iter) error {
		return p.addRow(i)
	})
}

// ReadArray appends one row per element of the single top-level JSON array
// in pj. It returns an error if the input does not contain exactly one
// top-level element or if that element is not an array.
func (p *Projection) ReadArray(pj *ParsedJson) error {
	if pj == nil {
		return errors.New("simdjson: ReadArray called with nil ParsedJson")
	}
	i := pj.Iter()
	var elem Iter
	t, err := i.AdvanceIter(&elem)
	if err != nil {
		return err
	}
	if t != TypeRoot {
		return fmt.Errorf("simdjson: ReadArray: expected a top-level element, got %v", t)
	}
	if t = i.Advance(); t != TypeNone {
		return errors.New("simdjson: ReadArray: input contains more than one top-level element")
	}
	elem.AdvanceInto()
	if elem.Type() != TypeArray {
		return fmt.Errorf("simdjson: ReadArray: top-level element is %v, not an array", elem.Type())
	}
	arr, err := elem.Array(nil)
	if err != nil {
		return err
	}
	it := arr.Iter()
	var row Iter
	for {
		t, err := it.AdvanceIter(&row)
		if err != nil {
			return err
		}
		if t == TypeNone {
			return nil
		}
		if err := p.addRow(row); err != nil {
			return err
		}
	}
}

// addRow projects a single row iterator. i must be the content of a root
// element (as passed by ParsedJson.ForEach or an array iteration).
func (p *Projection) addRow(i Iter) error {
	if i.Type() != TypeObject {
		p.beginRow(RowNotObject)
		return nil
	}
	row := p.beginRow(RowObject)
	var obj Object
	if _, err := i.Object(&obj); err != nil {
		return err
	}
	return p.walk(&obj, p.root, row)
}

// beginRow appends one aligned slot to every column and returns the row index.
func (p *Projection) beginRow(state RowState) int {
	row := p.rows
	p.rows++
	p.seq++
	p.rowStates = append(p.rowStates, state)
	for i := range p.data {
		d := &p.data[i]
		d.States = append(d.States, FieldMissing)
		switch p.cols[i].Type {
		case ColumnString:
			d.Strings = append(d.Strings, "")
		case ColumnInt64:
			d.Ints = append(d.Ints, 0)
		case ColumnUint64:
			d.Uints = append(d.Uints, 0)
		case ColumnFloat64:
			d.Floats = append(d.Floats, 0)
		case ColumnBool:
			d.Bools = append(d.Bools, false)
		}
	}
	return row
}

// walk resolves node against obj. Only keys present in the trie are
// descended into; all other values are skipped by the object iterator.
func (p *Projection) walk(obj *Object, node *pathNode, row int) error {
	var it Iter
	for {
		name, t, err := obj.NextElementBytes(&it)
		if err != nil {
			return err
		}
		if t == TypeNone {
			return nil
		}
		child := node.child(string(name))
		if child == nil {
			// Not part of any declared path; already skipped.
			continue
		}
		// A duplicated key resolves a declared path only on its first
		// occurrence, matching Object.FindPath.
		if child.lastRow == p.seq {
			continue
		}
		child.lastRow = p.seq
		if len(child.cols) > 0 {
			for _, ci := range child.cols {
				if err := p.extract(ci, it, row); err != nil {
					return err
				}
			}
		}
		if len(child.children) > 0 && t == TypeObject {
			var sub Object
			if _, err := it.Object(&sub); err != nil {
				return err
			}
			if err := p.walk(&sub, child, row); err != nil {
				return err
			}
		}
	}
}

// extract records the state and, when possible, the value of a terminal path
// for column ci in the given row.
func (p *Projection) extract(ci int, it Iter, row int) error {
	d := &p.data[ci]
	if it.t == TagNull {
		d.States[row] = FieldNull
		return nil
	}
	switch p.cols[ci].Type {
	case ColumnString:
		if it.t != TagString {
			d.States[row] = FieldTypeMismatch
			return nil
		}
		b, err := it.StringBytes()
		if err != nil {
			return err
		}
		if p.copyStrings {
			d.Strings[row] = string(b)
		} else {
			d.Strings[row] = unsafeBytesToString(b)
		}
	case ColumnInt64:
		switch it.t {
		case TagInteger:
			v, err := it.Int()
			if err != nil {
				return err
			}
			d.Ints[row] = v
		case TagUint:
			v, err := it.Uint()
			if err != nil {
				return err
			}
			if v > math.MaxInt64 {
				d.States[row] = FieldTypeMismatch
				return nil
			}
			d.Ints[row] = int64(v)
		default:
			d.States[row] = FieldTypeMismatch
			return nil
		}
	case ColumnUint64:
		switch it.t {
		case TagUint:
			v, err := it.Uint()
			if err != nil {
				return err
			}
			d.Uints[row] = v
		case TagInteger:
			v, err := it.Int()
			if err != nil {
				return err
			}
			if v < 0 {
				d.States[row] = FieldTypeMismatch
				return nil
			}
			d.Uints[row] = uint64(v)
		default:
			d.States[row] = FieldTypeMismatch
			return nil
		}
	case ColumnFloat64:
		switch it.t {
		case TagFloat, TagInteger, TagUint:
			v, err := it.Float()
			if err != nil {
				return err
			}
			d.Floats[row] = v
		default:
			d.States[row] = FieldTypeMismatch
			return nil
		}
	case ColumnBool:
		switch it.t {
		case TagBoolTrue:
			d.Bools[row] = true
		case TagBoolFalse:
			d.Bools[row] = false
		default:
			d.States[row] = FieldTypeMismatch
			return nil
		}
	default:
		return fmt.Errorf("simdjson: column %q: invalid type %d", p.cols[ci].Name, p.cols[ci].Type)
	}
	d.States[row] = FieldOK
	return nil
}
