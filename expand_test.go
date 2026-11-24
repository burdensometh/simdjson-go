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
	"reflect"
	"strings"
	"testing"
)

func expandTestParse(t *testing.T, in string, nd bool) *ParsedJson {
	t.Helper()
	var (
		pj  *ParsedJson
		err error
	)
	if nd {
		pj, err = ParseND([]byte(in), nil)
	} else {
		pj, err = Parse([]byte(in), nil)
	}
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return pj
}

// rowSummary is a comparable version of a Row used in tests.
type rowSummary struct {
	ParentKey  string
	EventIndex int
	ItemIndex  int
	Bad        bool
}

func summarize(rows []Row) []rowSummary {
	out := make([]rowSummary, 0, len(rows))
	for _, r := range rows {
		out = append(out, rowSummary{
			ParentKey:  r.ParentKey,
			EventIndex: r.EventIndex,
			ItemIndex:  r.ItemIndex,
			Bad:        r.Bad(),
		})
	}
	return out
}

func TestParsedJsonExpand(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	const nd = `{"order":"A-1","items":[{"sku":"x"},{"sku":"y"}]}
{"order":"A-2","items":[]}
{"order":"A-3","items":[{"sku":"z"}]}`
	pj := expandTestParse(t, nd, true)

	rows, err := pj.Expand(nil, []string{"items"}, ExpandItemType(TypeObject))
	if err != nil {
		t.Fatal(err)
	}
	want := []rowSummary{
		{ParentKey: "items", EventIndex: 0, ItemIndex: 0},
		{ParentKey: "items", EventIndex: 0, ItemIndex: 1},
		{ParentKey: "items", EventIndex: 2, ItemIndex: 0},
	}
	if got := summarize(rows); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows: got %+v want %+v", got, want)
	}

	// The parent fields must be readable from each row.
	for _, r := range rows {
		parent, err := r.Parent.Object(nil)
		if err != nil {
			t.Fatal(err)
		}
		order := parent.FindKey("order", nil)
		if order == nil {
			t.Fatal("order not found on parent")
		}
		s, err := order.Iter.String()
		if err != nil {
			t.Fatal(err)
		}
		item, err := r.Item.Object(nil)
		if err != nil {
			t.Fatal(err)
		}
		sku := item.FindKey("sku", nil)
		if sku == nil {
			t.Fatal("sku not found on item")
		}
		if _, err := sku.Iter.String(); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(s, "A-") {
			t.Errorf("unexpected order %q", s)
		}
	}
}

func TestParsedJsonExpandKeepEmptyParent(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	const nd = `{"order":"A-1","items":[]}
{"order":"A-2","items":null}
{"order":"A-3"}`
	pj := expandTestParse(t, nd, true)

	rows, err := pj.Expand(nil, []string{"items"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("without parent rows: got %d rows, want 0", len(rows))
	}

	rows, err = pj.Expand(nil, []string{"items"}, ExpandKeepEmptyParent(true))
	if err != nil {
		t.Fatal(err)
	}
	want := []rowSummary{
		{ParentKey: "items", EventIndex: 0, ItemIndex: -1},
		{ParentKey: "items", EventIndex: 1, ItemIndex: -1},
		{ParentKey: "items", EventIndex: 2, ItemIndex: -1},
	}
	if got := summarize(rows); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows: got %+v want %+v", got, want)
	}
	// A missing key has no item iterator, but the parent is still available.
	if rows[2].Item.tape.Tape != nil {
		t.Errorf("missing key should have empty item iterator")
	}
	if got := rows[0].Location(); got != "event[0].items" {
		t.Errorf("location: got %q", got)
	}
}

func TestParsedJsonExpandBadItems(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	const nd = `{"order":"A-1","items":"oops"}
{"order":"A-2","items":[{"sku":"x"},7,null]}
{"order":"A-3","items":[{"sku":"z"}]}`
	pj := expandTestParse(t, nd, true)

	rows, err := pj.Expand(nil, []string{"items"}, ExpandItemType(TypeObject))
	if err != nil {
		t.Fatal(err)
	}
	want := []rowSummary{
		{ParentKey: "items", EventIndex: 0, ItemIndex: -1, Bad: true},
		{ParentKey: "items", EventIndex: 1, ItemIndex: 0},
		{ParentKey: "items", EventIndex: 1, ItemIndex: 1, Bad: true},
		{ParentKey: "items", EventIndex: 1, ItemIndex: 2, Bad: true},
		{ParentKey: "items", EventIndex: 2, ItemIndex: 0},
	}
	if got := summarize(rows); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows: got %+v want %+v", got, want)
	}

	bad := rows[0].BadItem()
	if bad == nil {
		t.Fatal("expected bad item")
	}
	if bad.Path() != "event[0].items" || bad.Want != TypeArray || bad.Got != TypeString {
		t.Errorf("bad item: %+v", bad)
	}
	if !strings.Contains(bad.Error(), "event[0].items") {
		t.Errorf("error lacks location: %v", bad)
	}

	bad = rows[2].BadItem()
	if bad == nil {
		t.Fatal("expected bad item")
	}
	if bad.Path() != "event[1].items[1]" || bad.Want != TypeObject || bad.Got != TypeInt {
		t.Errorf("bad item: %+v", bad)
	}

	bad = rows[3].BadItem()
	if bad == nil || bad.Got != TypeNull {
		t.Fatalf("expected null bad item, got %+v", bad)
	}

	// Bad rows report the BadItem as their value error.
	if _, err := rows[2].Value(); err == nil {
		t.Error("expected error from bad row value")
	}
	var target *BadItem
	if !errors.As(rows[2].Err, &target) {
		t.Error("BadItem should be usable with errors.As")
	}
}

func TestObjectExpand(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj := expandTestParse(t, `{"order":"A-1","items":[1,2],"tags":["a"]}`, false)
	var elem Element
	var obj *Object
	err := pj.ForEach(func(i Iter) error {
		var err error
		obj, err = i.Object(nil)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	var it Iter
	if _, _, err := obj.NextElement(&it); err != nil {
		t.Fatal(err)
	}

	rows, err := obj.Expand(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Without keys every array is expanded.
	want := []rowSummary{
		{ParentKey: "items", ItemIndex: 0},
		{ParentKey: "items", ItemIndex: 1},
		{ParentKey: "tags", ItemIndex: 0},
	}
	if got := summarize(rows); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows: got %+v want %+v", got, want)
	}

	// Expanding must not consume the object.
	rows, err = obj.Expand(nil, []string{"items"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("second expansion: got %d rows", len(rows))
	}

	// Explicit event index and parent key are recorded.
	rows, err = obj.Expand(nil, []string{"items"}, ExpandEventIndex(7))
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].EventIndex != 7 || rows[0].Location() != "event[7].items[0]" {
		t.Errorf("event index not applied: %+v", rows[0])
	}
	_ = elem
}

func TestArrayExpand(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj := expandTestParse(t, `[{"sku":"a"},{"sku":"b"}]`, false)
	var arr *Array
	err := pj.ForEach(func(i Iter) error {
		var err error
		arr, err = i.Array(nil)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	rows, err := arr.Expand(nil, ExpandParentKey("items"), ExpandItemType(TypeObject))
	if err != nil {
		t.Fatal(err)
	}
	want := []rowSummary{
		{ParentKey: "items", ItemIndex: 0},
		{ParentKey: "items", ItemIndex: 1},
	}
	if got := summarize(rows); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows: got %+v want %+v", got, want)
	}

	// A root array is expanded by ParsedJson.Expand as well.
	rows, err = pj.Expand(nil, []string{"items"}, ExpandParentKey("items"))
	if err != nil {
		t.Fatal(err)
	}
	if got := summarize(rows); !reflect.DeepEqual(got, want) {
		t.Fatalf("root rows: got %+v want %+v", got, want)
	}
}

func TestExpandEachStops(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj := expandTestParse(t, `{"items":[1,2,3]}`, false)
	stop := errors.New("stop")
	n := 0
	err := pj.ExpandEach([]string{"items"}, func(r Row) error {
		n++
		return stop
	})
	if !errors.Is(err, stop) {
		t.Fatalf("got %v, want stop", err)
	}
	if n != 1 {
		t.Fatalf("callback called %d times, want 1", n)
	}
}

func TestObjectExpandMissingKeys(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj := expandTestParse(t, `{"order":"A-1","items":[1]}`, false)
	var obj *Object
	err := pj.ForEach(func(i Iter) error {
		var err error
		obj, err = i.Object(nil)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Missing and null keys are reported as parent rows when requested,
	// in the order the keys were given.
	pjNull := expandTestParse(t, `{"order":"A-1","items":[1],"extra":null}`, false)
	err = pjNull.ForEach(func(i Iter) error {
		var err error
		obj, err = i.Object(nil)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := obj.Expand(nil, []string{"extra", "missing", "items"}, ExpandKeepEmptyParent(true))
	if err != nil {
		t.Fatal(err)
	}
	// Keys are visited in object order; missing keys are appended last.
	want := []rowSummary{
		{ParentKey: "items", ItemIndex: 0},
		{ParentKey: "extra", ItemIndex: -1},
		{ParentKey: "missing", ItemIndex: -1},
	}
	if got := summarize(rows); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

func TestArrayExpandEmpty(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj := expandTestParse(t, `[]`, false)
	var arr *Array
	err := pj.ForEach(func(i Iter) error {
		var err error
		arr, err = i.Array(nil)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := arr.Expand(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("got %d rows, want 0", len(rows))
	}
	rows, err = arr.Expand(nil, ExpandKeepEmptyParent(true), ExpandParentKey("items"))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ItemIndex != -1 || rows[0].Location() != "event[0].items" {
		t.Fatalf("got %+v", rows)
	}
	// The kept parent row exposes the empty array as its value.
	v, err := rows[0].Value()
	if err != nil {
		t.Fatal(err)
	}
	got, ok := v.([]interface{})
	if !ok || len(got) != 0 {
		t.Fatalf("value: got %#v", v)
	}
}

func TestExpandRootArrayBadItems(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	// A root array is expanded by ParsedJson.Expand. Scalars inside it are
	// bad items when objects are required.
	pj := expandTestParse(t, `[{"sku":"a"},42]`, false)
	rows, err := pj.Expand(nil, nil, ExpandParentKey("items"), ExpandItemType(TypeObject))
	if err != nil {
		t.Fatal(err)
	}
	want := []rowSummary{
		{ParentKey: "items", ItemIndex: 0},
		{ParentKey: "items", ItemIndex: 1, Bad: true},
	}
	if got := summarize(rows); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v want %+v", got, want)
	}
	if got := rows[1].Location(); got != "event[0].items[1]" {
		t.Errorf("location: got %q", got)
	}
}

func BenchmarkParsedJsonExpand(b *testing.B) {
	if !SupportedCPU() {
		b.SkipNow()
	}
	var sb strings.Builder
	sb.WriteString("[")
	for i := 0; i < 50; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`{"sku":"value","qty":2}`)
	}
	sb.WriteString("]")
	nd := `{"order":"A-1","items":` + sb.String() + "}"
	pj, err := ParseND([]byte(nd), nil)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := pj.Expand(nil, nil, ExpandItemType(TypeObject))
		if err != nil {
			b.Fatal(err)
		}
		if len(rows) == 0 {
			b.Fatal("no rows")
		}
	}
}
