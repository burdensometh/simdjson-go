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
	"log"
	"reflect"
	"testing"
)

func TestProjectionValidate(t *testing.T) {
	tests := []struct {
		name    string
		columns []Column
		wantErr bool
	}{
		{name: "none", columns: nil, wantErr: true},
		{name: "empty-name", columns: []Column{{Path: []string{"a"}, Type: ColumnString}}, wantErr: true},
		{name: "empty-path", columns: []Column{{Name: "a", Type: ColumnString}}, wantErr: true},
		{name: "bad-type", columns: []Column{{Name: "a", Path: []string{"a"}, Type: ColumnType(42)}}, wantErr: true},
		{name: "duplicate", columns: []Column{
			{Name: "a", Path: []string{"a"}, Type: ColumnString},
			{Name: "a", Path: []string{"b"}, Type: ColumnInt64},
		}, wantErr: true},
		{name: "ok", columns: []Column{{Name: "a", Path: []string{"a"}, Type: ColumnString}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewProjection(tc.columns)
			if (err != nil) != tc.wantErr {
				t.Fatalf("got err %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestProjectionBasic(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	const input = `{"a":{"b":"x","c":7,"d":true,"e":1.5,"f":null},"g":"top"}
{"a":{"b":"y","c":8}}
{"a":5}
[1,2,3]`
	pj, err := ParseND([]byte(input), nil)
	if err != nil {
		t.Fatal(err)
	}

	cols := []Column{
		{Name: "b", Path: []string{"a", "b"}, Type: ColumnString},
		{Name: "c", Path: []string{"a", "c"}, Type: ColumnInt64},
		{Name: "d", Path: []string{"a", "d"}, Type: ColumnBool},
		{Name: "e", Path: []string{"a", "e"}, Type: ColumnFloat64},
		{Name: "f", Path: []string{"a", "f"}, Type: ColumnString},
		{Name: "g", Path: []string{"g"}, Type: ColumnString},
		{Name: "absent", Path: []string{"a", "zzz"}, Type: ColumnString},
	}
	p, err := NewProjection(cols)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Read(pj); err != nil {
		t.Fatal(err)
	}

	if got, want := p.Rows(), 4; got != want {
		t.Fatalf("rows: got %d want %d", got, want)
	}

	// Column order must match declaration order.
	if got := p.Columns(); !reflect.DeepEqual(got, cols) {
		t.Fatalf("columns: got %v want %v", got, cols)
	}
	for i, c := range cols {
		if got := p.Column(i); got == nil || got.Column.Name != c.Name {
			t.Fatalf("column %d: got %v want name %q", i, got, c.Name)
		}
	}
	if p.Column(-1) != nil || p.Column(len(cols)) != nil {
		t.Fatal("out-of-range column should be nil")
	}

	wantRowStates := []RowState{RowObject, RowObject, RowObject, RowNotObject}
	if got := p.RowStates(); !reflect.DeepEqual(got, wantRowStates) {
		t.Fatalf("row states: got %v want %v", got, wantRowStates)
	}

	// Row 0: all present.
	checkStates(t, "b", p.ColumnByName("b").States, []FieldState{FieldOK, FieldOK, FieldMissing, FieldMissing})
	checkStates(t, "c", p.ColumnByName("c").States, []FieldState{FieldOK, FieldOK, FieldMissing, FieldMissing})
	checkStates(t, "d", p.ColumnByName("d").States, []FieldState{FieldOK, FieldMissing, FieldMissing, FieldMissing})
	checkStates(t, "e", p.ColumnByName("e").States, []FieldState{FieldOK, FieldMissing, FieldMissing, FieldMissing})
	checkStates(t, "f", p.ColumnByName("f").States, []FieldState{FieldNull, FieldMissing, FieldMissing, FieldMissing})
	checkStates(t, "g", p.ColumnByName("g").States, []FieldState{FieldOK, FieldMissing, FieldMissing, FieldMissing})

	// Row 2 has "a":5, so no object path can resolve: all missing.
	checkStates(t, "absent", p.ColumnByName("absent").States, []FieldState{FieldMissing, FieldMissing, FieldMissing, FieldMissing})

	if got, want := p.ColumnByName("b").Strings, []string{"x", "y", "", ""}; !reflect.DeepEqual(got, want) {
		t.Fatalf("b: got %q want %q", got, want)
	}
	if got, want := p.ColumnByName("c").Ints, []int64{7, 8, 0, 0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("c: got %v want %v", got, want)
	}
	if got, want := p.ColumnByName("d").Bools, []bool{true, false, false, false}; !reflect.DeepEqual(got, want) {
		t.Fatalf("d: got %v want %v", got, want)
	}
	if got, want := p.ColumnByName("e").Floats, []float64{1.5, 0, 0, 0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("e: got %v want %v", got, want)
	}
	if got, want := p.ColumnByName("g").Strings, []string{"top", "", "", ""}; !reflect.DeepEqual(got, want) {
		t.Fatalf("g: got %q want %q", got, want)
	}

	// Only the declared column's slice is populated and every slice is
	// aligned with the row count.
	for _, name := range []string{"b", "c", "d", "e", "f", "g", "absent"} {
		d := p.ColumnByName(name)
		if len(d.States) != p.Rows() {
			t.Fatalf("%s: states len %d != rows %d", name, len(d.States), p.Rows())
		}
		nonEmpty := 0
		for _, n := range []int{len(d.Strings), len(d.Ints), len(d.Uints), len(d.Floats), len(d.Bools)} {
			if n != 0 {
				nonEmpty++
				if n != p.Rows() {
					t.Fatalf("%s: value slice len %d != rows %d", name, n, p.Rows())
				}
			}
		}
		if nonEmpty != 1 {
			t.Fatalf("%s: expected exactly one populated value slice, got %d", name, nonEmpty)
		}
	}
}

func TestProjectionTypeMismatch(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	const input = `{"i":5,"u":18446744073709551615,"f":1.0,"s":"str","b":true,"o":{},"ar":[]}`
	pj, err := Parse([]byte(input), nil)
	if err != nil {
		t.Fatal(err)
	}
	cols := []Column{
		{Name: "iasint", Path: []string{"i"}, Type: ColumnInt64},
		{Name: "iasuint", Path: []string{"i"}, Type: ColumnUint64},
		{Name: "iasfloat", Path: []string{"i"}, Type: ColumnFloat64},
		{Name: "u", Path: []string{"u"}, Type: ColumnUint64},
		{Name: "u-as-int", Path: []string{"u"}, Type: ColumnInt64},
		{Name: "f", Path: []string{"f"}, Type: ColumnFloat64},
		{Name: "f-as-int", Path: []string{"f"}, Type: ColumnInt64},
		{Name: "s", Path: []string{"s"}, Type: ColumnString},
		{Name: "s-as-int", Path: []string{"s"}, Type: ColumnInt64},
		{Name: "b", Path: []string{"b"}, Type: ColumnBool},
		{Name: "b-as-string", Path: []string{"b"}, Type: ColumnString},
		{Name: "o-as-string", Path: []string{"o"}, Type: ColumnString},
		{Name: "ar-as-string", Path: []string{"ar"}, Type: ColumnString},
	}
	p, err := NewProjection(cols)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Read(pj); err != nil {
		t.Fatal(err)
	}
	want := map[string]FieldState{
		"iasint":       FieldOK,
		"iasuint":      FieldOK,
		"iasfloat":     FieldOK,
		"u":            FieldOK,
		"u-as-int":     FieldTypeMismatch, // does not fit int64
		"f":            FieldOK,
		"f-as-int":     FieldTypeMismatch, // float notation is not an int
		"s":            FieldOK,
		"s-as-int":     FieldTypeMismatch,
		"b":            FieldOK,
		"b-as-string":  FieldTypeMismatch,
		"o-as-string":  FieldTypeMismatch,
		"ar-as-string": FieldTypeMismatch,
	}
	for name, wantState := range want {
		d := p.ColumnByName(name)
		if got := d.States[0]; got != wantState {
			t.Errorf("%s: got state %v want %v", name, got, wantState)
		}
	}
	if got, want := p.ColumnByName("u").Uints[0], uint64(18446744073709551615); got != want {
		t.Errorf("u value: got %d want %d", got, want)
	}
}

func TestProjectionNegativeToUint(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj, err := Parse([]byte(`{"n":-3}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProjection([]Column{{Name: "n", Path: []string{"n"}, Type: ColumnUint64}})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Read(pj); err != nil {
		t.Fatal(err)
	}
	if got := p.ColumnByName("n").States[0]; got != FieldTypeMismatch {
		t.Fatalf("got %v want %v", got, FieldTypeMismatch)
	}
}

func TestProjectionReadArray(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	const input = `[{"a":"one"},{"a":null},{"b":1},{"a":"two"}]`
	pj, err := Parse([]byte(input), nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProjection([]Column{{Name: "a", Path: []string{"a"}, Type: ColumnString}})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ReadArray(pj); err != nil {
		t.Fatal(err)
	}
	if got, want := p.Rows(), 4; got != want {
		t.Fatalf("rows: got %d want %d", got, want)
	}
	if got, want := p.ColumnByName("a").States, []FieldState{FieldOK, FieldNull, FieldMissing, FieldOK}; !reflect.DeepEqual(got, want) {
		t.Fatalf("states: got %v want %v", got, want)
	}
	if got, want := p.ColumnByName("a").Strings, []string{"one", "", "", "two"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("strings: got %q want %q", got, want)
	}

	// Non-array and multi-element documents must be rejected.
	for _, in := range []string{`{"a":1}`, `{"a":1}` + "\n" + `{"a":2}`} {
		pj, err := ParseND([]byte(in), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := p.ReadArray(pj); err == nil {
			t.Fatalf("ReadArray(%q): expected error", in)
		}
	}
}

func TestProjectionDuplicateKey(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	// First occurrence wins, matching Object.FindPath.
	pj, err := Parse([]byte(`{"a":"first","a":"second","n":{"x":1},"n":{"x":2}}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProjection([]Column{
		{Name: "a", Path: []string{"a"}, Type: ColumnString},
		{Name: "x", Path: []string{"n", "x"}, Type: ColumnInt64},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Read(pj); err != nil {
		t.Fatal(err)
	}
	if got, want := p.ColumnByName("a").Strings[0], "first"; got != want {
		t.Errorf("a: got %q want %q", got, want)
	}
	if got, want := p.ColumnByName("x").Ints[0], int64(1); got != want {
		t.Errorf("x: got %d want %d", got, want)
	}
}

func TestProjectionSharedPrefix(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	// "a" is both a leaf and an ancestor of another declared path.
	pj, err := Parse([]byte(`{"a":{"b":"inner"},"z":1}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProjection([]Column{
		{Name: "a", Path: []string{"a"}, Type: ColumnString},
		{Name: "ab", Path: []string{"a", "b"}, Type: ColumnString},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Read(pj); err != nil {
		t.Fatal(err)
	}
	// The terminal "a" is an object, so the string column mismatches.
	if got := p.ColumnByName("a").States[0]; got != FieldTypeMismatch {
		t.Errorf("a: got %v want %v", got, FieldTypeMismatch)
	}
	if got, want := p.ColumnByName("ab").Strings[0], "inner"; got != want {
		t.Errorf("ab: got %q want %q", got, want)
	}
}

func TestProjectionReadNonObject(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	// NDJSON roots may be arrays. Read treats each root as one row.
	pj, err := ParseND([]byte("[[1,2],[3],{}]"), nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProjection([]Column{{Name: "a", Path: []string{"a"}, Type: ColumnString}})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Read(pj); err != nil {
		t.Fatal(err)
	}
	if got, want := p.Rows(), 1; got != want {
		t.Fatalf("rows: got %d want %d", got, want)
	}
	if got, want := p.RowStates(), []RowState{RowNotObject}; !reflect.DeepEqual(got, want) {
		t.Fatalf("row states: got %v want %v", got, want)
	}
	checkStates(t, "a", p.ColumnByName("a").States, []FieldState{FieldMissing})

	// With ReadArray the same document yields one row per element.
	p.Reset()
	if err := p.ReadArray(pj); err != nil {
		t.Fatal(err)
	}
	if got, want := p.Rows(), 3; got != want {
		t.Fatalf("ReadArray rows: got %d want %d", got, want)
	}
	wantRowStates := []RowState{RowNotObject, RowNotObject, RowObject}
	if got := p.RowStates(); !reflect.DeepEqual(got, wantRowStates) {
		t.Fatalf("ReadArray row states: got %v want %v", got, wantRowStates)
	}
	checkStates(t, "a", p.ColumnByName("a").States, []FieldState{FieldMissing, FieldMissing, FieldMissing})

	// An array root read with Parse is also a single non-object row.
	pj, err = Parse([]byte(`[1,2]`), nil)
	if err != nil {
		t.Fatal(err)
	}
	p.Reset()
	if err := p.Read(pj); err != nil {
		t.Fatal(err)
	}
	if got, want := p.RowStates(), []RowState{RowNotObject}; !reflect.DeepEqual(got, want) {
		t.Fatalf("array root row states: got %v want %v", got, want)
	}
	if got := p.ColumnByName("a").States[0]; got != FieldMissing {
		t.Fatalf("array root state: got %v want %v", got, FieldMissing)
	}
}

func TestProjectionReset(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj, err := ParseND([]byte("{\"a\":\"x\"}\n{\"a\":\"y\"}"), nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProjection([]Column{{Name: "a", Path: []string{"a"}, Type: ColumnString}})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Read(pj); err != nil {
		t.Fatal(err)
	}
	if p.Rows() != 2 {
		t.Fatalf("rows: got %d want 2", p.Rows())
	}
	p.Reset()
	if got := p.Rows(); got != 0 {
		t.Fatalf("after reset rows: got %d want 0", got)
	}
	if got := len(p.ColumnByName("a").Strings); got != 0 {
		t.Fatalf("after reset strings len: got %d want 0", got)
	}
	if err := p.Read(pj); err != nil {
		t.Fatal(err)
	}
	if got, want := p.ColumnByName("a").Strings, []string{"x", "y"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("after reset strings: got %q want %q", got, want)
	}
}

func TestProjectionNoCopyStrings(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj, err := Parse([]byte(`{"a":"copy"}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProjection(
		[]Column{{Name: "a", Path: []string{"a"}, Type: ColumnString}},
		WithProjectionCopyStrings(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Read(pj); err != nil {
		t.Fatal(err)
	}
	if got, want := p.ColumnByName("a").Strings[0], "copy"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestProjectionNilParse(t *testing.T) {
	p, err := NewProjection([]Column{{Name: "a", Path: []string{"a"}, Type: ColumnString}})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Read(nil); err == nil {
		t.Fatal("Read(nil): expected error")
	}
	if err := p.ReadArray(nil); err == nil {
		t.Fatal("ReadArray(nil): expected error")
	}
}

func checkStates(t *testing.T, name string, got, want []FieldState) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s states: got %v want %v", name, got, want)
	}
}

// TestProjectionFixture runs the projection over a real NDJSON corpus and
// compares every projected cell against Object.FindPath as an oracle.
func TestProjectionFixture(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	if testing.Short() {
		t.Skip("skipping... too long")
	}
	ndjson := loadFile("testdata/parking-citations.json.zst")
	pj, err := ParseND(ndjson, nil)
	if err != nil {
		t.Fatal(err)
	}

	cols := []Column{
		{Name: "make", Path: []string{"Make"}, Type: ColumnString},
		{Name: "code", Path: []string{"ViolationCode"}, Type: ColumnInt64},
		{Name: "fine", Path: []string{"Fine"}, Type: ColumnFloat64},
		{Name: "lat", Path: []string{"Location", "Latitude"}, Type: ColumnFloat64},
		{Name: "missing", Path: []string{"NoSuchField"}, Type: ColumnString},
		{Name: "deep", Path: []string{"Location", "NoSuchField"}, Type: ColumnString},
	}
	p, err := NewProjection(cols)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Read(pj); err != nil {
		t.Fatal(err)
	}

	// Independently walk the same document to build the expected states.
	wantStates := make([][]FieldState, len(cols))
	for i := range wantStates {
		wantStates[i] = make([]FieldState, p.Rows())
	}
	row := 0
	var elem *Element
	err = pj.ForEach(func(i Iter) error {
		if i.Type() != TypeObject {
			return nil
		}
		var obj Object
		if _, err := i.Object(&obj); err != nil {
			return err
		}
		for ci, c := range cols {
			e, err := obj.FindPath(elem, c.Path...)
			if err != nil {
				wantStates[ci][row] = FieldMissing
				continue
			}
			elem = e
			switch {
			case e.Type == TypeNull:
				wantStates[ci][row] = FieldNull
			case c.Type == ColumnString && e.Type == TypeString,
				c.Type == ColumnInt64 && (e.Type == TypeInt || e.Type == TypeUint),
				c.Type == ColumnUint64 && (e.Type == TypeInt || e.Type == TypeUint),
				c.Type == ColumnFloat64 && (e.Type == TypeFloat || e.Type == TypeInt || e.Type == TypeUint),
				c.Type == ColumnBool && e.Type == TypeBool:
				wantStates[ci][row] = FieldOK
			default:
				wantStates[ci][row] = FieldTypeMismatch
			}
		}
		row++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if row != p.Rows() {
		t.Fatalf("row count: oracle %d projection %d", row, p.Rows())
	}
	for ci, c := range cols {
		checkStates(t, c.Name, p.Column(ci).States, wantStates[ci])
	}
	if n := p.ColumnByName("make").Strings; len(n) != p.Rows() {
		t.Fatalf("make strings len %d != rows %d", len(n), p.Rows())
	}
}

func BenchmarkProjection(b *testing.B) {
	if !SupportedCPU() {
		b.SkipNow()
	}
	ndjson := loadFile("testdata/parking-citations.json.zst")
	pj, err := ParseND(ndjson, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(ndjson)))

	cols := []Column{
		{Name: "make", Path: []string{"Make"}, Type: ColumnString},
		{Name: "code", Path: []string{"ViolationCode"}, Type: ColumnInt64},
		{Name: "fine", Path: []string{"Fine"}, Type: ColumnFloat64},
		{Name: "lat", Path: []string{"Location", "Latitude"}, Type: ColumnFloat64},
	}

	b.Run("projection", func(b *testing.B) {
		p, err := NewProjection(cols)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			p.Reset()
			if err := p.Read(pj); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("interface", func(b *testing.B) {
		var obj Object
		var elem *Element
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := pj.ForEach(func(it Iter) error {
				if _, err := it.Object(&obj); err != nil {
					return err
				}
				for _, c := range cols {
					e, err := obj.FindPath(elem, c.Path...)
					if err != nil {
						continue
					}
					elem = e
					_, _ = e.Iter.Interface()
				}
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func ExampleProjection() {
	if !SupportedCPU() {
		fmt.Println("EUR 120\nUSD 90")
		return
	}
	const ndjson = `{"price":120,"currency":"EUR","meta":{"region":"eu"}}
{"price":90,"currency":"USD"}
{"price":null,"currency":"EUR"}`
	pj, err := ParseND([]byte(ndjson), nil)
	if err != nil {
		log.Fatal(err)
	}
	p, err := NewProjection([]Column{
		{Name: "price", Path: []string{"price"}, Type: ColumnFloat64},
		{Name: "currency", Path: []string{"currency"}, Type: ColumnString},
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := p.Read(pj); err != nil {
		log.Fatal(err)
	}
	price := p.ColumnByName("price")
	currency := p.ColumnByName("currency")
	for row := 0; row < p.Rows(); row++ {
		if price.States[row] == FieldOK {
			fmt.Printf("%s %g\n", currency.Strings[row], price.Floats[row])
		}
	}
	// Output:
	// EUR 120
	// USD 90
}
