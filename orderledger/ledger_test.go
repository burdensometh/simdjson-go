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
	"encoding/json"
	"errors"
	"testing"

	"github.com/minio/simdjson-go"
)

func ingest(t *testing.T, l *Ledger, batch string) *Result {
	t.Helper()
	if !simdjson.SupportedCPU() {
		t.SkipNow()
	}
	res, err := l.IngestJSON([]byte(batch))
	if err != nil {
		t.Fatalf("IngestJSON: %v", err)
	}
	return res
}

func mustEntry(t *testing.T, res *Result, key string) *Entry {
	t.Helper()
	for _, e := range res.Entries {
		if e.Key() == key {
			return e
		}
	}
	t.Fatalf("entry %q not found", key)
	return nil
}

func rawOf(t *testing.T, ev *Event) map[string]interface{} {
	t.Helper()
	if ev == nil {
		t.Fatal("nil event")
	}
	var m map[string]interface{}
	if err := json.Unmarshal(ev.Raw, &m); err != nil {
		t.Fatalf("unmarshal raw %s: %v", ev.Raw, err)
	}
	return m
}

func TestMergeByVersionAcrossBatches(t *testing.T) {
	l := New(Config{})

	// Batches arrive out of version order.
	res := ingest(t, l, `{"order_id":"A","version":2,"amount":20,"state":"paid"}
{"order_id":"B","version":1,"amount":5}
`)
	if len(res.Entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(res.Entries))
	}
	if len(res.Final) != 2 {
		t.Fatalf("got %d finals, want 2", len(res.Final))
	}

	// A later batch with an older version must not roll the final back.
	res = ingest(t, l, `{"order_id":"A","version":1,"amount":10,"state":"new"}
{"order_id":"B","version":3,"amount":7}
{"order_id":"A","version":3,"amount":30,"state":"shipped"}
`)
	if len(res.Entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(res.Entries))
	}

	a := mustEntry(t, res, "A")
	v, ok := a.FinalVersion()
	if !ok || v.String() != "3" {
		t.Fatalf("final version = %v, %v; want 3", v, ok)
	}
	final := a.FinalEvent()
	if final == nil {
		t.Fatal("nil final event")
	}
	if got := rawOf(t, final)["state"]; got != "shipped" {
		t.Fatalf("final state = %v, want shipped", got)
	}

	sup := a.Superseded()
	if len(sup) != 2 {
		t.Fatalf("got %d superseded versions, want 2", len(sup))
	}
	if sup[0].Version.String() != "1" || sup[1].Version.String() != "2" {
		t.Fatalf("superseded versions = %v, want [1 2]", sup)
	}
	// Superseded versions keep the values they carried.
	if got := rawOf(t, &sup[0].Events[0])["state"]; got != "new" {
		t.Fatalf("superseded v1 state = %v, want new", got)
	}
	if got := rawOf(t, &sup[1].Events[0])["state"]; got != "paid" {
		t.Fatalf("superseded v2 state = %v, want paid", got)
	}

	b := mustEntry(t, res, "B")
	if v, _ := b.FinalVersion(); v.String() != "3" {
		t.Fatalf("B final version = %v, want 3", v)
	}
	if got := len(b.Superseded()); got != 1 {
		t.Fatalf("B superseded count = %d, want 1", got)
	}
}

func TestDuplicateDeliveryCreatesNoVersion(t *testing.T) {
	l := New(Config{})
	first := ingest(t, l, `{"order_id":"A","version":2,"amount":20}
{"order_id":"A","version":2,"amount":20}
`)
	if len(first.Entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(first.Entries))
	}
	e := first.Entries[0]
	if len(e.Versions()) != 1 {
		t.Fatalf("duplicate created a version: %v", e.Versions())
	}
	if first.Duplicates != 1 {
		t.Fatalf("Duplicates = %d, want 1", first.Duplicates)
	}
	if d := e.Versions()[0].Events[0].Deliveries; d != 2 {
		t.Fatalf("event deliveries = %d, want 2", d)
	}

	// Redelivery in a later batch, with different key order and whitespace.
	second := ingest(t, l, `{ "amount" : 20.0 , "version" : "2" , "order_id" : "A" }`)
	if second.Duplicates != 1 {
		t.Fatalf("Duplicates = %d, want 1", second.Duplicates)
	}
	e = second.Entries[0]
	if len(e.Versions()) != 1 {
		t.Fatalf("redelivery created a version: %v", e.Versions())
	}
	if d := e.Versions()[0].Events[0].Deliveries; d != 3 {
		t.Fatalf("event deliveries = %d, want 3", d)
	}
	if len(second.Final) != 1 || second.Final[0].Deliveries != 3 {
		t.Fatalf("final = %+v", second.Final)
	}
}

func TestSameVersionDifferentValuesConflict(t *testing.T) {
	l := New(Config{})
	res := ingest(t, l, `{"order_id":"A","version":4,"amount":40}
{"order_id":"A","version":4,"amount":41}
{"order_id":"A","version":3,"amount":30}
`)
	e := mustEntry(t, res, "A")
	if len(res.Conflicts) != 1 {
		t.Fatalf("got %d conflicts, want 1", len(res.Conflicts))
	}
	c := res.Conflicts[0]
	if c.Version.String() != "4" || !c.Final {
		t.Fatalf("conflict = %+v, want version 4 final", c)
	}
	if len(c.Events) != 2 {
		t.Fatalf("conflict has %d events, want 2", len(c.Events))
	}
	// A conflicting final version has no unambiguous final event.
	if e.FinalEvent() != nil {
		t.Fatalf("FinalEvent = %+v, want nil", e.FinalEvent())
	}
	if len(res.Final) != 0 {
		t.Fatalf("res.Final = %+v, want none", res.Final)
	}
	// The lower version is still retained as superseded, not discarded.
	sup := e.Superseded()
	if len(sup) != 1 || sup[0].Version.String() != "3" {
		t.Fatalf("superseded = %v", sup)
	}

	// A conflicting non-final version does not block the final event.
	res = ingest(t, l, `{"order_id":"A","version":5,"amount":50}`)
	if len(res.Conflicts) != 1 {
		t.Fatalf("conflicts after new final = %v, want the v4 conflict", res.Conflicts)
	}
	if res.Conflicts[0].Final {
		t.Fatal("v4 conflict marked final after v5 arrived")
	}
	e = mustEntry(t, res, "A")
	if f := e.FinalEvent(); f == nil || rawOf(t, f)["amount"] != float64(50) {
		t.Fatalf("final = %+v, want amount 50", f)
	}
}

func TestIgnoreFieldsForDeliveryMetadata(t *testing.T) {
	l := New(Config{IgnoreFields: []string{"ingested_at"}})
	ingest(t, l, `{"order_id":"A","version":1,"amount":10,"ingested_at":"2026-01-01T00:00:00Z"}`)
	res := ingest(t, l, `{"order_id":"A","version":1,"amount":10,"ingested_at":"2026-01-02T00:00:00Z"}`)
	if res.Duplicates != 1 || len(res.Conflicts) != 0 {
		t.Fatalf("duplicates=%d conflicts=%v, want 1 duplicate and no conflict", res.Duplicates, res.Conflicts)
	}
	// Without the ignore list the same pair conflicts.
	l2 := New(Config{})
	ingest(t, l2, `{"order_id":"A","version":1,"amount":10,"ingested_at":"2026-01-01T00:00:00Z"}`)
	res = ingest(t, l2, `{"order_id":"A","version":1,"amount":10,"ingested_at":"2026-01-02T00:00:00Z"}`)
	if len(res.Conflicts) != 1 || len(res.Conflicts[0].Events) != 2 {
		t.Fatalf("conflicts = %+v, want one with two events", res.Conflicts)
	}
}

func TestVersionNormalization(t *testing.T) {
	l := New(Config{})
	res := ingest(t, l, `{"order_id":"A","version":7,"amount":1}
{"order_id":"A","version":"007","amount":1}
{"order_id":"A","version":"7","amount":1}
`)
	if len(res.Entries) != 1 || len(res.Entries[0].Versions()) != 1 {
		t.Fatalf("versions = %v, want one version", res.Entries[0].Versions())
	}
	if res.Duplicates != 2 || len(res.Conflicts) != 0 {
		t.Fatalf("duplicates=%d conflicts=%v", res.Duplicates, res.Conflicts)
	}
	if v, _ := res.Entries[0].FinalVersion(); v.String() != "7" {
		t.Fatalf("final = %v, want 7", v)
	}
}

func TestVersionOrdering(t *testing.T) {
	cases := []struct{ a, b string }{
		{"0", "1"},
		{"-1", "0"},
		{"-2", "-1"},
		{"9", "10"},
		{"18446744073709551615", "18446744073709551615"},
	}
	for _, c := range cases {
		va, err := ParseVersion(c.a)
		if err != nil {
			t.Fatalf("ParseVersion(%q): %v", c.a, err)
		}
		vb, err := ParseVersion(c.b)
		if err != nil {
			t.Fatalf("ParseVersion(%q): %v", c.b, err)
		}
		if va.Compare(vb) > 0 {
			t.Errorf("%s > %s", c.a, c.b)
		}
		if vb.Compare(va) < 0 {
			t.Errorf("%s < %s", c.b, c.a)
		}
		if c.a == c.b && (!va.Equal(vb) || va.Compare(vb) != 0) {
			t.Errorf("%s != %s", c.a, c.b)
		}
		if va.String() != c.a {
			t.Errorf("String() = %q, want %q", va.String(), c.a)
		}
	}

	bad := []string{"", "-", "+1", " 1", "1.0", "v2", "1e3", "0x10", "--1", "1-"}
	for _, s := range bad {
		if _, err := ParseVersion(s); !errors.Is(err, ErrBadVersion) {
			t.Errorf("ParseVersion(%q) error = %v, want ErrBadVersion", s, err)
		}
	}

	l := New(Config{})
	res := ingest(t, l, `{"order_id":"A","version":-1,"x":1}
{"order_id":"A","version":0,"x":2}
{"order_id":"A","version":-1,"x":1}
{"order_id":"A","version":1.5,"x":3}
{"order_id":"A","version":"two","x":4}
`)
	if len(res.Invalid) != 2 {
		t.Fatalf("Invalid = %+v, want 2", res.Invalid)
	}
	if !errors.Is(res.Invalid[0].Err, ErrBadVersion) || !errors.Is(res.Invalid[1].Err, ErrBadVersion) {
		t.Fatalf("Invalid errors = %v", res.Invalid)
	}
	e := mustEntry(t, res, "A")
	if v, _ := e.FinalVersion(); v.String() != "0" {
		t.Fatalf("final = %v, want 0", v)
	}
	if got := e.Superseded(); len(got) != 1 || got[0].Version.String() != "-1" {
		t.Fatalf("superseded = %v, want [-1]", got)
	}
	if res.Duplicates != 1 {
		t.Fatalf("Duplicates = %d, want 1", res.Duplicates)
	}
}

func TestInvalidRecords(t *testing.T) {
	l := New(Config{})
	res := ingest(t, l, `{"version":1,"x":1}
{"order_id":"A","x":1}
{"order_id":true,"version":1}
{"order_id":"A","version":1,"x":1}
[1,{"order_id":"B","version":1},"x"]
`)
	if res.Records != 7 {
		t.Fatalf("Records = %d, want 7", res.Records)
	}
	if len(res.Invalid) != 5 {
		t.Fatalf("Invalid = %+v, want 5", res.Invalid)
	}
	want := []error{ErrMissingKey, ErrMissingVersion, ErrBadKey, ErrNotObject, ErrNotObject}
	for i, w := range want {
		if !errors.Is(res.Invalid[i].Err, w) {
			t.Errorf("invalid[%d] = %v, want %v", i, res.Invalid[i].Err, w)
		}
	}
	if res.Invalid[3].Index != 4 || res.Invalid[4].Index != 6 {
		t.Fatalf("array scalar indices = %d, %d; want 4, 6", res.Invalid[3].Index, res.Invalid[4].Index)
	}
	if len(res.Entries) != 2 {
		t.Fatalf("entries = %v, want A and B", res.Entries)
	}
}

func TestArrayBatchAndKeyTypes(t *testing.T) {
	l := New(Config{})
	res := ingest(t, l, `[
		{"order_id":"1","version":1,"x":"string key"},
		{"order_id":1,"version":2,"x":"int key v2"}
	]`)
	if len(res.Entries) != 1 {
		t.Fatalf("entries = %v, want a single order key", res.Entries)
	}
	e := res.Entries[0]
	if e.Key() != "1" {
		t.Fatalf("key = %q, want 1", e.Key())
	}
	if v, _ := e.FinalVersion(); v.String() != "2" {
		t.Fatalf("final version = %v, want 2", v)
	}
	if got := rawOf(t, e.FinalEvent())["x"]; got != "int key v2" {
		t.Fatalf("final = %v, want int key v2", got)
	}
	if got := len(e.Superseded()); got != 1 {
		t.Fatalf("superseded = %d, want 1", got)
	}

	// The same key text, same version, different values is a conflict even
	// when the key is spelled with different JSON types.
	l2 := New(Config{})
	res = ingest(t, l2, `{"order_id":"1","version":1,"x":"string key"}
{"order_id":1,"version":1,"x":"int key"}`)
	if len(res.Conflicts) != 1 || len(res.Conflicts[0].Events) != 2 {
		t.Fatalf("conflicts = %+v, want one with two events", res.Conflicts)
	}
}

func TestLookupAndReset(t *testing.T) {
	l := New(Config{})
	ingest(t, l, `{"order_id":"A","version":1,"x":1}`)
	if l.Len() != 1 {
		t.Fatalf("Len = %d, want 1", l.Len())
	}
	e := l.Lookup("A")
	if e == nil || e.FinalEvent() == nil {
		t.Fatalf("Lookup(A) = %v", e)
	}
	if l.Lookup("missing") != nil {
		t.Fatal("Lookup(missing) != nil")
	}
	l.Reset()
	if l.Len() != 0 || l.Lookup("A") != nil {
		t.Fatal("Reset did not clear state")
	}
}

func TestSnapshotsAreIndependent(t *testing.T) {
	l := New(Config{})
	res := ingest(t, l, `{"order_id":"A","version":1,"x":1}`)
	e := res.Entries[0]
	// Mutating the returned slice must not affect the ledger.
	vs := e.Versions()
	vs[0].Version = Version{mag: 42}
	vs[0].Events[0].Deliveries = 99
	lk := l.Lookup("A")
	if got, _ := lk.FinalVersion(); got.String() != "1" {
		t.Fatalf("version after external mutation = %s", got)
	}
	if got := lk.Versions()[0].Events[0].Deliveries; got != 1 {
		t.Fatalf("deliveries after external mutation = %d", got)
	}
}
