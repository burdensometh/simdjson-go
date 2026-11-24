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
	"strings"
	"testing"
)

const filterRecords = `{"id":1,"payment":{"status":"completed","amount":100}}
{"id":2,"payment":{"status":"completed","amount":250.5}}
{"id":3,"payment":{"status":"completed","amount":99.99}}
{"id":4,"payment":{"status":"pending","amount":500}}
{"id":5,"payment":{"amount":1000}}
{"id":6}
{"id":7,"payment":{"status":"completed","amount":-5}}
{"id":8,"payment":{"status":"completed","amount":100}}
`

// gate is the user-facing shape of the requirement: only records whose
// payment is completed and whose amount is at least the threshold pass.
// Amount is matched with Number so that fractional amounts are accepted.
func gate(threshold float64) Filter {
	return And(
		Field("payment", "status").String(OpEqual, "completed"),
		Field("payment", "amount").Number(OpGreaterEqual, threshold),
	)
}

func decodeRecords(t *testing.T, in string) *ParsedJson {
	t.Helper()
	pj, err := ParseND([]byte(in), nil)
	if err != nil {
		t.Fatal(err)
	}
	return pj
}

func TestFilterMatchResults(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj := decodeRecords(t, filterRecords)
	want := map[int]MatchResult{
		1: Match,
		2: Match,
		3: NoMatch,      // amount below the threshold
		4: NoMatch,      // payment not completed
		5: MissingField, // status absent
		6: MissingField, // payment absent
		7: NoMatch,      // amount below the threshold
		8: Match,
	}
	got := map[int]MatchResult{}
	err := pj.ForEachFiltered(gate(100), func(i Iter, res MatchResult) error {
		var id Element
		if _, err := i.FindElement(&id, "id"); err != nil {
			return err
		}
		n, err := id.Iter.Int()
		if err != nil {
			return err
		}
		got[int(n)] = res
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d", len(got), len(want))
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("record %d: got %v, want %v", id, got[id], w)
		}
	}
}

func TestFilterForEachMatch(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj := decodeRecords(t, filterRecords)
	var ids []int64
	err := pj.ForEachMatch(gate(150), func(i Iter) error {
		var id Element
		if _, err := i.FindElement(&id, "id"); err != nil {
			return err
		}
		n, err := id.Iter.Int()
		if err != nil {
			return err
		}
		ids = append(ids, n)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{2}
	if len(ids) != len(want) || ids[0] != want[0] {
		t.Fatalf("got matches %v, want %v", ids, want)
	}
}

func TestFilterWriteMatchesPreservesOrder(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj := decodeRecords(t, filterRecords)
	var buf bytes.Buffer
	stats, err := pj.WriteMatches(&buf, gate(100))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Matches != 3 || stats.NoMatches != 3 || stats.MissingFields != 2 {
		t.Fatalf("got stats %+v", stats)
	}
	if stats.Total() != 8 {
		t.Fatalf("total %d, want 8", stats.Total())
	}
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3:\n%s", len(lines), buf.String())
	}
	// Matches must be written in input order.
	for i, wantID := range []string{`"id":1`, `"id":2`, `"id":8`} {
		if !strings.Contains(lines[i], wantID) {
			t.Errorf("line %d = %s, want it to contain %s", i, lines[i], wantID)
		}
	}
	// The output must be parseable NDJSON again.
	if _, err := ParseND(buf.Bytes(), nil); err != nil {
		t.Fatalf("output is not valid NDJSON: %v", err)
	}
}

func TestFilterCombinators(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj := decodeRecords(t, filterRecords)
	cases := []struct {
		name string
		f    Filter
		want MatchStats
	}{
		{
			name: "and-definite-miss-wins",
			f: And(
				Field("payment", "status").String(OpEqual, "pending"),
				Field("payment", "amount").Int(OpGreaterEqual, 100),
			),
			// Only record 4 is pending. Records 5 and 6 are missing a field;
			// the remaining records are definite misses.
			want: MatchStats{Matches: 1, NoMatches: 5, MissingFields: 2},
		},
		{
			name: "or-match-wins-over-missing",
			f: Or(
				Field("id").Int(OpEqual, 6),
				Field("payment", "amount").Int(OpGreaterEqual, 1000),
			),
			// id 6 matches; id 5 (amount 1000) matches; id 6 also lacks payment
			// but the id clause is decisive.
			want: MatchStats{Matches: 2, NoMatches: 6, MissingFields: 0}},
		{
			name: "not-inverts",
			f:    Not(Field("payment", "status").String(OpEqual, "completed")),
			// 1,2,3,7,8 are completed -> no match. 4 is pending -> match.
			// 5 has no status, 6 has no payment -> missing field.
			want: MatchStats{Matches: 1, NoMatches: 5, MissingFields: 2},
		},
		{
			name: "exists",
			f:    Field("payment", "status").Exists(),
			// Present for 1,2,3,4,7,8.
			want: MatchStats{Matches: 6, NoMatches: 0, MissingFields: 2},
		},
		{
			name: "empty-and-matches-all",
			f:    And(),
			want: MatchStats{Matches: 8},
		},
		{
			name: "empty-or-matches-none",
			f:    Or(),
			want: MatchStats{Matches: 0, NoMatches: 8},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stats MatchStats
			err := pj.ForEachFiltered(tc.f, func(i Iter, res MatchResult) error {
				stats.add(res)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if stats != tc.want {
				t.Errorf("got %+v, want %+v", stats, tc.want)
			}
		})
	}
}

func TestFilterTypedComparisons(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	const doc = `{"s":"abc","i":42,"u":18446744073709551615,"f":1.5,"b":true,"n":null,"a":[1,2,3]}`
	pj, err := Parse([]byte(doc), nil)
	if err != nil {
		t.Fatal(err)
	}
	rec := rootIter(t, pj)
	cases := []struct {
		name string
		f    Filter
		want MatchResult
	}{
		{"string-eq", Field("s").String(OpEqual, "abc"), Match},
		{"string-lt", Field("s").String(OpLess, "abd"), Match},
		{"string-wrong-type", Field("i").String(OpEqual, "42"), NoMatch},
		{"int-eq", Field("i").Int(OpEqual, 42), Match},
		{"int-ne", Field("i").Int(OpNotEqual, 42), NoMatch},
		{"int-on-float-is-not-match", Field("f").Int(OpEqual, 1), NoMatch},
		{"uint-max", Field("u").Uint(OpEqual, 18446744073709551615), Match},
		{"uint-vs-negative", Field("i").Uint(OpEqual, 0), NoMatch},
		{"int-against-uint-max", Field("u").Int(OpEqual, 42), NoMatch},
		{"int-against-uint-max-greater", Field("u").Int(OpGreater, 42), Match},
		{"number-mixes-int-and-float", Field("i").Number(OpGreaterEqual, 41.5), Match},
		{"number-on-float", Field("f").Number(OpEqual, 1.5), Match},
		{"number-on-string-is-no-match", Field("s").Number(OpEqual, 0), NoMatch},
		{"bool-true", Field("b").Bool(OpEqual, true), Match},
		{"bool-false", Field("b").Bool(OpEqual, false), NoMatch},
		{"bool-wrong-type", Field("i").Bool(OpEqual, true), NoMatch},
		{"null-counts-as-present", Field("n").Exists(), Match},
		{"missing", Field("absent").Exists(), MissingField},
		{"into-array-is-missing", Field("a", "0").Exists(), MissingField},
		{"wrong-type-mid-path", Field("i", "x").Exists(), MissingField},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := rec.Match(tc.f); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFilterDoesNotAdvanceIterator(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj := decodeRecords(t, filterRecords)
	seen := 0
	err := pj.ForEachFiltered(gate(100), func(i Iter, res MatchResult) error {
		seen++
		// The record iterator must still be usable from the start.
		if i.Type() != TypeObject {
			t.Fatalf("record %d: iterator not positioned at the record, got %v", seen, i.Type())
		}
		var id Element
		if _, err := i.FindElement(&id, "id"); err != nil {
			t.Fatalf("record %d: %v", seen, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen != 8 {
		t.Fatalf("saw %d records, want 8", seen)
	}
}

func TestFilterNilFilterMatchesAll(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj := decodeRecords(t, filterRecords)
	var stats MatchStats
	err := pj.ForEachFiltered(nil, func(i Iter, res MatchResult) error {
		stats.add(res)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Matches != 8 {
		t.Fatalf("got %+v, want all 8 matching", stats)
	}
}

func TestFilterNonNDJSON(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	// A non-NDJSON document is a single root element; WriteMatches emits a
	// single line.
	pj, err := Parse([]byte(`{"payment":{"status":"completed","amount":10}}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	stats, err := pj.WriteMatches(&buf, gate(5))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Matches != 1 {
		t.Fatalf("got %+v", stats)
	}
	if got := strings.TrimSpace(buf.String()); got != `{"payment":{"status":"completed","amount":10}}` {
		t.Fatalf("got %s", got)
	}
}

func TestFilterCustomFunc(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj := decodeRecords(t, filterRecords)
	// A custom filter that only reads the iterator directly.
	f := FilterFunc(func(i *Iter) MatchResult {
		var id Element
		if _, err := i.FindElement(&id, "id"); err != nil {
			return MissingField
		}
		n, err := id.Iter.Int()
		if err != nil {
			return NoMatch
		}
		if n%2 == 0 {
			return Match
		}
		return NoMatch
	})
	var stats MatchStats
	err := pj.ForEachFiltered(f, func(i Iter, res MatchResult) error {
		stats.add(res)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Matches != 4 || stats.NoMatches != 4 || stats.MissingFields != 0 {
		t.Fatalf("got %+v", stats)
	}
}

func TestMatchResultString(t *testing.T) {
	for _, tc := range []struct {
		in   MatchResult
		want string
	}{
		{Match, "match"},
		{NoMatch, "no match"},
		{MissingField, "missing field"},
		{MatchResult(99), "(invalid)"},
	} {
		if got := tc.in.String(); got != tc.want {
			t.Errorf("got %q, want %q", got, tc.want)
		}
	}
}

func TestOpString(t *testing.T) {
	for _, tc := range []struct {
		in   Op
		want string
	}{
		{OpEqual, "=="},
		{OpNotEqual, "!="},
		{OpLess, "<"},
		{OpLessEqual, "<="},
		{OpGreater, ">"},
		{OpGreaterEqual, ">="},
		{Op(99), "(invalid)"},
	} {
		if got := tc.in.String(); got != tc.want {
			t.Errorf("got %q, want %q", got, tc.want)
		}
	}
}

func TestWriteMatchesNilWriter(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj := decodeRecords(t, filterRecords)
	if _, err := pj.WriteMatches(nil, gate(1)); err == nil {
		t.Fatal("expected error for nil writer")
	}
}

// rootIter returns an iterator positioned at the single root value of pj.
func rootIter(t *testing.T, pj *ParsedJson) Iter {
	t.Helper()
	it := pj.Iter()
	var rec Iter
	typ, err := it.AdvanceIter(&rec)
	if err != nil {
		t.Fatal(err)
	}
	if typ != TypeRoot {
		t.Fatalf("expected root, got %v", typ)
	}
	rec.AdvanceInto()
	return rec
}

func TestFilterCountMatches(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj := decodeRecords(t, filterRecords)
	stats, err := pj.CountMatches(gate(100))
	if err != nil {
		t.Fatal(err)
	}
	want := MatchStats{Matches: 3, NoMatches: 3, MissingFields: 2}
	if stats != want {
		t.Fatalf("got %+v, want %+v", stats, want)
	}
}

func TestFilterWriteMatchesOrderOnLongInput(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	var in bytes.Buffer
	const n = 500
	for i := 0; i < n; i++ {
		status := `"completed"`
		if i%3 == 0 {
			status = `"pending"`
		}
		// Every 7th record omits the amount entirely.
		if i%7 == 0 {
			in.WriteString(`{"id":` + itoa(i) + `,"payment":{"status":` + status + `}}` + "\n")
			continue
		}
		in.WriteString(`{"id":` + itoa(i) + `,"payment":{"status":` + status + `,"amount":` + itoa(i) + `}}` + "\n")
	}
	pj := decodeRecords(t, in.String())

	var buf bytes.Buffer
	stats, err := pj.WriteMatches(&buf, gate(250))
	if err != nil {
		t.Fatal(err)
	}

	// Compute the expected set independently.
	var wantIDs []int
	for i := 0; i < n; i++ {
		if i%7 == 0 || i%3 == 0 || i < 250 {
			continue
		}
		wantIDs = append(wantIDs, i)
	}
	if stats.Matches != len(wantIDs) {
		t.Fatalf("got %d matches, want %d", stats.Matches, len(wantIDs))
	}
	if stats.Total() != n {
		t.Fatalf("total %d, want %d", stats.Total(), n)
	}

	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != len(wantIDs) {
		t.Fatalf("got %d lines, want %d", len(lines), len(wantIDs))
	}
	for n, wantID := range wantIDs {
		want := `"id":` + itoa(wantID) + `,`
		if !strings.HasPrefix(lines[n], "{"+want) {
			t.Fatalf("line %d = %s, want it to start with {%s", n, lines[n], want)
		}
	}
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}

func BenchmarkForEachFiltered(b *testing.B) {
	if !SupportedCPU() {
		b.SkipNow()
	}
	var in bytes.Buffer
	for i := 0; i < 1000; i++ {
		in.WriteString(`{"id":` + itoa(i) + `,"payment":{"status":"completed","amount":` + itoa(i) + `}}` + "\n")
	}
	pj, err := ParseND(in.Bytes(), nil)
	if err != nil {
		b.Fatal(err)
	}
	f := gate(500)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats, err := pj.CountMatches(f)
		if err != nil {
			b.Fatal(err)
		}
		if stats.Total() != 1000 {
			b.Fatalf("got %d records", stats.Total())
		}
	}
}

func BenchmarkWriteMatches(b *testing.B) {
	if !SupportedCPU() {
		b.SkipNow()
	}
	var in bytes.Buffer
	for i := 0; i < 1000; i++ {
		in.WriteString(`{"id":` + itoa(i) + `,"payment":{"status":"completed","amount":` + itoa(i) + `}}` + "\n")
	}
	pj, err := ParseND(in.Bytes(), nil)
	if err != nil {
		b.Fatal(err)
	}
	f := gate(500)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var w discardWriter
		if _, err := pj.WriteMatches(&w, f); err != nil {
			b.Fatal(err)
		}
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestFilterTopLevelNonObject(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	// A record that is not an object cannot supply an object path.
	pj, err := ParseND([]byte("[1,2]\n[3]\n"), nil)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := pj.CountMatches(Field("payment", "status").Exists())
	if err != nil {
		t.Fatal(err)
	}
	want := MatchStats{MissingFields: 2}
	if stats != want {
		t.Fatalf("got %+v, want %+v", stats, want)
	}
	// A filter over the record itself does not need an object path.
	stats, err = pj.CountMatches(FilterFunc(func(i *Iter) MatchResult {
		if i.Type() == TypeArray {
			return Match
		}
		return NoMatch
	}))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Matches != 2 {
		t.Fatalf("got %+v, want two array matches", stats)
	}
}

func TestFilterWriteMatchesStopsOnWriteError(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj := decodeRecords(t, filterRecords)
	stats, err := pj.WriteMatches(failingWriter{}, gate(0))
	if err == nil {
		t.Fatal("expected write error")
	}
	// The first matching record should have been counted before the failure.
	if stats.Matches != 1 {
		t.Fatalf("got %+v, want 1 match counted before the error", stats)
	}
}

type failingWriter struct{}

func (failingWriter) Write(p []byte) (int, error) { return 0, errWrite }

var errWrite = &writeError{}

type writeError struct{}

func (*writeError) Error() string { return "write failed" }

func TestFilterOnFreshIter(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj := decodeRecords(t, filterRecords)
	// An Iter returned by ParsedJson.Iter has not been advanced yet; Match
	// should still read the first record.
	it := pj.Iter()
	if got := it.Match(Field("payment", "status").String(OpEqual, "completed")); got != Match {
		t.Fatalf("got %v, want %v", got, Match)
	}
	// Match must not advance the caller's iterator.
	typ, err := it.AdvanceIter(new(Iter))
	if err != nil {
		t.Fatal(err)
	}
	if typ != TypeRoot {
		t.Fatalf("iterator was advanced by Match, got %v", typ)
	}
}
