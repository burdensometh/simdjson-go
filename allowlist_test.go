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
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

const allowlistFixture = `{
  "txn_id": "TX-1001",
  "counterparty": {
    "name": "Acme Corp",
    "account": "DE89-3704-0044-0532",
    "internal_note": "do not publish"
  },
  "amount": {
    "value": 1250.75,
    "currency": "EUR",
    "fx_rate": 1.08
  },
  "line_items": [
    {"sku": "A-1", "qty": 2, "cost": 10.5, "secret": "x"},
    {"sku": "B-2", "qty": 1, "cost": 1229.75, "secret": "y"},
    {"sku": "C-3", "qty": 9, "cost": 0.25, "secret": "z"}
  ],
  "status": "settled",
  "fraud_score": 0.03
}`

func parseForAllowlist(t *testing.T, msg string) *ParsedJson {
	t.Helper()
	if !SupportedCPU() {
		t.Skip("unsupported platform")
	}
	pj, err := Parse([]byte(msg), nil)
	if err != nil {
		t.Fatalf("failed to parse fixture: %v", err)
	}
	return pj
}

func mustParseAllowlist(t *testing.T, paths ...string) *PathAllowlist {
	t.Helper()
	p, issues := ParseAllowlistPaths(paths)
	for _, is := range issues {
		if is.Kind == IssueInvalid {
			t.Fatalf("unexpected build issue: %v", is)
		}
	}
	return p
}

func snapshot(pj *ParsedJson) (tape []uint64, msg, strings []byte) {
	return append([]uint64(nil), pj.Tape...),
		append([]byte(nil), pj.Message...),
		append([]byte(nil), pj.Strings.B...)
}

func TestPathAllowlist_ProjectBasic(t *testing.T) {
	pj := parseForAllowlist(t, allowlistFixture)
	p := mustParseAllowlist(t,
		"txn_id",
		"counterparty/name",
		"counterparty/account",
		"amount/value",
		"amount/currency",
		"line_items/0/sku",
		"line_items/0/qty",
		"line_items/2/sku",
		"status",
	)

	out, issues, err := p.Project(pj)
	if err != nil {
		t.Fatalf("project failed: %v", err)
	}
	if len(issues) != 0 {
		t.Fatalf("expected no issues, got %v", issues)
	}

	var got map[string]interface{}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out)
	}

	// Sibling fields must be gone.
	for _, banned := range []string{"fraud_score", "fx_rate", "internal_note"} {
		if _, ok := lookupAny(got, banned); ok {
			t.Errorf("banned key %q present in output", banned)
		}
	}
	if got["txn_id"] != "TX-1001" || got["status"] != "settled" {
		t.Errorf("scalar fields wrong: %v", got)
	}
	cp, _ := got["counterparty"].(map[string]interface{})
	if cp["name"] != "Acme Corp" || cp["account"] != "DE89-3704-0044-0532" {
		t.Errorf("counterparty projection wrong: %v", cp)
	}
	if _, ok := cp["internal_note"]; ok {
		t.Errorf("nested banned key present: %v", cp)
	}
	amt, _ := got["amount"].(map[string]interface{})
	if amt["currency"] != "EUR" || amt["value"] != 1250.75 {
		t.Errorf("amount projection wrong: %v", amt)
	}
	items, _ := got["line_items"].([]interface{})
	if len(items) != 2 {
		t.Fatalf("expected 2 kept array elements in order, got %d: %v", len(items), items)
	}
	first, _ := items[0].(map[string]interface{})
	second, _ := items[1].(map[string]interface{})
	if first["sku"] != "A-1" || first["qty"] != float64(2) {
		t.Errorf("first element wrong: %v", first)
	}
	if _, ok := first["cost"]; ok {
		t.Errorf("unapproved sub-field kept: %v", first)
	}
	if second["sku"] != "C-3" {
		t.Errorf("array order not preserved, expected C-3 at position 1, got %v", second)
	}
}

func TestPathAllowlist_PreservesMemberAndArrayOrder(t *testing.T) {
	pj := parseForAllowlist(t, allowlistFixture)
	// Approved paths deliberately given out of order.
	p := mustParseAllowlist(t,
		"status",
		"amount/currency",
		"line_items/2/sku",
		"line_items/0/sku",
		"txn_id",
	)
	out, _, err := p.Project(pj)
	if err != nil {
		t.Fatal(err)
	}

	var got map[string]interface{}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(got))
	// Re-decode preserving order.
	dec := json.NewDecoder(bytes.NewReader(out))
	tok, err := dec.Token()
	if err != nil || fmt.Sprint(tok) != "{" {
		t.Fatalf("expected object start, got %v, %v", tok, err)
	}
	for dec.More() {
		tok, _ := dec.Token()
		keys = append(keys, tok.(string))
		var v interface{}
		if err := dec.Decode(&v); err != nil {
			t.Fatal(err)
		}
	}
	wantKeys := []string{"txn_id", "amount", "line_items", "status"}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Errorf("member order not preserved: got %v want %v", keys, wantKeys)
	}
	items := got["line_items"].([]interface{})
	skus := []string{items[0].(map[string]interface{})["sku"].(string),
		items[1].(map[string]interface{})["sku"].(string)}
	if !reflect.DeepEqual(skus, []string{"A-1", "C-3"}) {
		t.Errorf("array order not preserved: %v", skus)
	}
}

func TestPathAllowlist_DoesNotModifyOriginal(t *testing.T) {
	pj := parseForAllowlist(t, allowlistFixture)
	tapeBefore, msgBefore, strBefore := snapshot(pj)

	p := mustParseAllowlist(t, "txn_id", "line_items/1/cost", "counterparty/name")
	if _, _, err := p.Project(pj); err != nil {
		t.Fatal(err)
	}

	tapeAfter, msgAfter, strAfter := snapshot(pj)
	if !reflect.DeepEqual(tapeBefore, tapeAfter) {
		t.Errorf("tape was modified")
	}
	if !reflect.DeepEqual(msgBefore, msgAfter) {
		t.Errorf("message was modified")
	}
	if !reflect.DeepEqual(strBefore, strAfter) {
		t.Errorf("string buffer was modified")
	}

	// The reference record must still contain every field.
	refIter := pj.Iter()
	full, err := refIter.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var ref map[string]interface{}
	if err := json.Unmarshal(full, &ref); err != nil {
		t.Fatal(err)
	}
	if _, ok := ref["fraud_score"]; !ok {
		t.Errorf("original record lost a field")
	}
}

func TestPathAllowlist_DuplicateAndInvalidSyntaxAreLocated(t *testing.T) {
	paths := []string{
		"txn_id",
		"",                 // 1: invalid, empty
		"amount//currency", // 2: invalid, empty segment
		"txn_id",           // 3: duplicate of 0
		"/status",          // 4: invalid, leading slash
		"line_items/0/sku",
		"line_items/0/sku", // 6: duplicate of 5
		"line_items/999/sku",
	}
	p, buildIssues := ParseAllowlistPaths(paths)

	wantBuild := map[int]PathIssueKind{1: IssueInvalid, 2: IssueInvalid, 3: IssueDuplicate, 4: IssueInvalid, 6: IssueDuplicate}
	if len(buildIssues) != len(wantBuild) {
		t.Fatalf("expected %d build issues, got %d: %v", len(wantBuild), len(buildIssues), buildIssues)
	}
	for _, is := range buildIssues {
		want, ok := wantBuild[is.Index]
		if !ok {
			t.Errorf("unexpected issue at index %d: %v", is.Index, is)
			continue
		}
		if is.Kind != want || is.Segment != -1 || is.Path != paths[is.Index] {
			t.Errorf("issue at %d wrong: %+v", is.Index, is)
		}
	}

	pj := parseForAllowlist(t, allowlistFixture)
	_, projIssues, err := p.Project(pj)
	if err != nil {
		t.Fatal(err)
	}
	// Build-time issues must surface again from Project, located the same way.
	seen := map[int]PathIssueKind{}
	for _, is := range projIssues {
		seen[is.Index] = is.Kind
	}
	for idx, kind := range wantBuild {
		if seen[idx] != kind {
			t.Errorf("project issues missing %d (%v), got %v", idx, kind, seen)
		}
	}
	var outOfRange *PathIssue
	for i := range projIssues {
		if projIssues[i].Index == 7 {
			outOfRange = &projIssues[i]
		}
	}
	if outOfRange == nil || outOfRange.Segment != 1 || outOfRange.Kind != IssueInvalid {
		t.Fatalf("out of range index not located correctly: %+v", projIssues)
	}
}

func TestPathAllowlist_MissingAndTypeMismatchAreLocated(t *testing.T) {
	pj := parseForAllowlist(t, allowlistFixture)
	p, buildIssues := ParseAllowlistPaths([]string{
		"txn_id",             // 0: fine
		"counterparty/name",  // 1: fine
		"counterparty/ghost", // 2: missing member, fails at segment 1
		"amount/value/eur",   // 3: scalar overshoot, fails at segment 2
		"status/0",           // 4: object index into a string, fails at segment 1
		"txn_id/name",        // 5: covered wholesale by path 0 -> no issue
		"line_items/name",    // 6: member segment into array, fails at segment 1
	})
	if len(buildIssues) != 0 {
		t.Fatalf("unexpected build issues: %v", buildIssues)
	}

	out, issues, err := p.Project(pj)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(out) {
		t.Fatalf("output not valid JSON: %s", out)
	}

	wantSeg := map[int]int{2: 1, 3: 2, 4: 1, 6: 1}
	gotSeg := map[int]PathIssue{}
	for _, is := range issues {
		if is.Kind != IssueInvalid {
			t.Errorf("expected invalid issue, got %v", is)
		}
		if _, dup := gotSeg[is.Index]; dup {
			t.Errorf("path index %d reported more than once: %v", is.Index, is)
		}
		gotSeg[is.Index] = is
	}
	for idx, seg := range wantSeg {
		is, ok := gotSeg[idx]
		if !ok {
			t.Errorf("expected issue for path index %d, got %v", idx, issues)
			continue
		}
		if is.Segment != seg {
			t.Errorf("path %d: expected failing segment %d, got %d (%s)", idx, seg, is.Segment, is.Message)
		}
	}

	// Sorted by configuration index.
	for i := 1; i < len(issues); i++ {
		if issues[i-1].Index > issues[i].Index {
			t.Errorf("issues not sorted by config index: %v", issues)
		}
	}
}

func TestPathAllowlist_WholesaleSubtreeAndPrefixRedundancy(t *testing.T) {
	pj := parseForAllowlist(t, allowlistFixture)
	// Whole object plus a redundant descendant: descendant must not be invalid
	// nor duplicated, and the full subtree must survive including its arrays.
	p, buildIssues := ParseAllowlistPaths([]string{
		"counterparty",
		"counterparty/name",
		"amount",
	})
	if len(buildIssues) != 0 {
		t.Fatalf("unexpected build issues: %v", buildIssues)
	}
	out, issues, err := p.Project(pj)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 0 {
		t.Fatalf("expected no issues, got %v", issues)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	cp := got["counterparty"].(map[string]interface{})
	if cp["internal_note"] != "do not publish" {
		t.Errorf("wholesale subtree should keep all members: %v", cp)
	}
	amt := got["amount"].(map[string]interface{})
	if _, ok := amt["fx_rate"]; !ok {
		t.Errorf("wholesale amount subtree incomplete: %v", amt)
	}
}

func TestPathAllowlist_NumericKeyVsArrayIndex(t *testing.T) {
	msg := `{"items": {"0": "zero-key", "1": "one-key"}, "rows": ["a", "b"]}`
	pj := parseForAllowlist(t, msg)
	p, buildIssues := ParseAllowlistPaths([]string{
		"items/0", // 0: index segment into object -> mismatch
		"rows/0",  // 1: array index -> "a"
		"rows/zz", // 2: member segment into array -> mismatch
	})
	if len(buildIssues) != 0 {
		t.Fatalf("unexpected build issues: %v", buildIssues)
	}
	out, issues, err := p.Project(pj)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("invalid output %s: %v", out, err)
	}
	rows, ok := got["rows"].([]interface{})
	if !ok || !reflect.DeepEqual(rows, []interface{}{"a"}) {
		t.Errorf("array index projection wrong: %v", got)
	}
	if _, ok := got["items"]; ok {
		t.Errorf("mismatched object subtree should be omitted: %v", got)
	}
	byIndex := map[int]PathIssue{}
	for _, is := range issues {
		byIndex[is.Index] = is
	}
	if _, ok := byIndex[0]; !ok {
		t.Errorf("expected mismatch issue for items/0: %v", issues)
	}
	if _, ok := byIndex[2]; !ok {
		t.Errorf("expected mismatch issue for rows/zz: %v", issues)
	}
}

func TestPathAllowlist_NDJSON(t *testing.T) {
	msg := `{"id":"r1","secret":1,"ok":true}` + "\n" +
		`{"id":"r2","secret":2,"ok":false}` + "\n"
	if !SupportedCPU() {
		t.Skip("unsupported platform")
	}
	pj, err := ParseND([]byte(msg), nil)
	if err != nil {
		t.Fatal(err)
	}
	p := mustParseAllowlist(t, "id")
	out, issues, err := p.Project(pj)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 0 {
		t.Fatalf("unexpected issues: %v", issues)
	}
	lines := splitLines(out)
	if len(lines) != 2 {
		t.Fatalf("expected 2 output lines, got %d: %s", len(lines), out)
	}
	for i, line := range lines {
		var rec map[string]interface{}
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("line %d invalid: %v", i, err)
		}
		if _, ok := rec["secret"]; ok {
			t.Errorf("line %d leaked secret: %v", i, rec)
		}
		if rec["id"] != fmt.Sprintf("r%d", i+1) {
			t.Errorf("line %d wrong id: %v", i, rec)
		}
	}
}

func TestPathAllowlist_NDJSONMixedRecordsReportFailure(t *testing.T) {
	if !SupportedCPU() {
		t.Skip("unsupported platform")
	}
	// The approved path exists in record 1 but not in record 2.
	msg := `{"id":"r1","pay":{"ref":"ok"}}` + "\n" +
		`{"id":"r2","other":1}` + "\n"
	pj, err := ParseND([]byte(msg), nil)
	if err != nil {
		t.Fatal(err)
	}
	p, buildIssues := ParseAllowlistPaths([]string{"id", "pay/ref"})
	if len(buildIssues) != 0 {
		t.Fatalf("unexpected build issues: %v", buildIssues)
	}
	out, issues, err := p.Project(pj)
	if err != nil {
		t.Fatal(err)
	}
	lines := splitLines(out)
	if len(lines) != 2 {
		t.Fatalf("expected 2 output lines: %s", out)
	}
	var r1 map[string]interface{}
	if err := json.Unmarshal(lines[0], &r1); err != nil {
		t.Fatal(err)
	}
	if r1["pay"].(map[string]interface{})["ref"] != "ok" {
		t.Errorf("record 1 projection wrong: %v", r1)
	}
	var r2 map[string]interface{}
	if err := json.Unmarshal(lines[1], &r2); err != nil {
		t.Fatal(err)
	}
	if _, ok := r2["pay"]; ok {
		t.Errorf("record 2 should omit absent subtree: %v", r2)
	}
	// The failure on record 2 must be reported even though record 1 matched.
	var found *PathIssue
	for i := range issues {
		if issues[i].Path == "pay/ref" {
			found = &issues[i]
		}
	}
	if found == nil {
		t.Fatalf("expected an issue for pay/ref from record 2, got %v", issues)
	}
	// Only one report despite two records.
	count := 0
	for _, is := range issues {
		if is.Path == "pay/ref" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected single deduplicated issue, got %d", count)
	}
}

func TestPathAllowlist_EmptyMatchAndAllTypes(t *testing.T) {
	msg := `{"s":"x","n":-3,"u":42,"f":1.5,"b":true,"z":null,"arr":[1,2],"obj":{"k":"v"}}`
	pj := parseForAllowlist(t, msg)

	// No path matches anything: the root object is omitted and the root
	// fallback emits null so the output line stays valid JSON.
	p, _ := ParseAllowlistPaths([]string{"missing"})
	out, issues, err := p.Project(pj)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 1 || issues[0].Kind != IssueInvalid {
		t.Fatalf("expected one invalid issue, got %v", issues)
	}
	if string(out) != "null" {
		t.Errorf("expected null fallback, got %s", out)
	}

	// Every scalar/container type projects unchanged.
	p2 := mustParseAllowlist(t, "s", "n", "u", "f", "b", "z", "arr", "obj")
	out2, issues2, err := p2.Project(pj)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues2) != 0 {
		t.Fatalf("unexpected issues: %v", issues2)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(out2, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]interface{}{
		"s": "x", "n": float64(-3), "u": float64(42), "f": 1.5,
		"b": true, "z": nil,
		"arr": []interface{}{float64(1), float64(2)},
		"obj": map[string]interface{}{"k": "v"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("type projection wrong:\ngot  %v\nwant %v", got, want)
	}
}

func TestPathAllowlist_MissingMemberReportsAtKeySegment(t *testing.T) {
	msg := `{"a": {"y": 1}, "b": 2}`
	pj := parseForAllowlist(t, msg)
	p, _ := ParseAllowlistPaths([]string{
		"a/x",      // 0: member x missing inside a -> fails at segment 1
		"a/x/deep", // 1: same failing key, segment 1 (one report per path)
		"c",        // 2: member c missing at root -> fails at segment 0
		"b",        // 3: fine
	})
	out, issues, err := p.Project(pj)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(out) {
		t.Fatalf("invalid output: %s", out)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got["a"]; ok {
		t.Errorf("object with no approved member should be omitted: %v", got)
	}
	if got["b"] != float64(2) {
		t.Errorf("unrelated approved field wrong: %v", got)
	}
	byIndex := map[int]PathIssue{}
	for _, is := range issues {
		byIndex[is.Index] = is
	}
	wantSeg := map[int]int{0: 1, 1: 1, 2: 0}
	for idx, seg := range wantSeg {
		is, ok := byIndex[idx]
		if !ok {
			t.Fatalf("missing issue for path index %d: %v", idx, issues)
		}
		if is.Segment != seg || is.Kind != IssueInvalid {
			t.Errorf("path %d: got segment %d, want %d", idx, is.Segment, seg)
		}
	}
}

func TestPathAllowlist_ReusableAcrossProjections(t *testing.T) {
	pj1 := parseForAllowlist(t, `{"a":1,"b":2}`)
	pj2 := parseForAllowlist(t, `{"a":3,"c":4}`)
	p := mustParseAllowlist(t, "a")

	out1, issues1, err := p.Project(pj1)
	if err != nil || len(issues1) != 0 {
		t.Fatalf("first projection: %v %v", err, issues1)
	}
	out2, issues2, err := p.Project(pj2)
	if err != nil || len(issues2) != 0 {
		t.Fatalf("second projection: %v %v", err, issues2)
	}
	if string(out1) != `{"a":1}` || string(out2) != `{"a":3}` {
		t.Errorf("reused allowlist gave wrong results: %s %s", out1, out2)
	}
}

func TestParseAllowlistPath(t *testing.T) {
	segs, err := ParseAllowlistPath("a/0/b/12")
	if err != nil {
		t.Fatal(err)
	}
	want := []PathSegment{
		{Key: "a"},
		{Index: 0, IsIndex: true},
		{Key: "b"},
		{Index: 12, IsIndex: true},
	}
	if !reflect.DeepEqual(segs, want) {
		t.Errorf("got %+v want %+v", segs, want)
	}
	for _, bad := range []string{"", "a/", "/a", "a//b", "a/01b"} {
		// "a/01b" is a valid member name (not all digits); check the rest.
		if bad == "a/01b" {
			if _, err := ParseAllowlistPath(bad); err != nil {
				t.Errorf("%q should be a valid member path: %v", bad, err)
			}
			continue
		}
		if _, err := ParseAllowlistPath(bad); err == nil {
			t.Errorf("expected error parsing %q", bad)
		}
	}
}

func TestPathIssue_Error(t *testing.T) {
	is := PathIssue{Index: 2, Path: "a/b", Kind: IssueDuplicate, Segment: -1, Message: "dup"}
	if got := is.Error(); got == "" {
		t.Fatal("empty error string")
	}
	is2 := PathIssue{Index: 0, Path: "a", Kind: IssueInvalid, Segment: 1, Message: "boom"}
	if got := is2.Error(); !contains(got, "segment 1") {
		t.Errorf("error should mention segment, got %q", got)
	}
}

// --- small helpers ---

func splitLines(b []byte) [][]byte {
	var out [][]byte
	start := 0
	for i, c := range b {
		if c == '\n' {
			out = append(out, b[start:i])
			start = i + 1
		}
	}
	if start < len(b) {
		out = append(out, b[start:])
	}
	return out
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func lookupAny(m map[string]interface{}, key string) (interface{}, bool) {
	if v, ok := m[key]; ok {
		return v, true
	}
	for _, v := range m {
		switch tv := v.(type) {
		case map[string]interface{}:
			if r, ok := lookupAny(tv, key); ok {
				return r, true
			}
		case []interface{}:
			for _, e := range tv {
				if em, ok := e.(map[string]interface{}); ok {
					if r, ok := lookupAny(em, key); ok {
						return r, true
					}
				}
			}
		}
	}
	return nil, false
}
