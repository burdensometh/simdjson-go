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
	"testing"
)

func parseWithPolicy(t *testing.T, input string, policy DuplicateKeyPolicy, extra ...ParserOption) *ParsedJson {
	t.Helper()
	opts := append([]ParserOption{WithDuplicateKeyPolicy(policy)}, extra...)
	pj, err := Parse([]byte(input), nil, opts...)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	return pj
}

func rootObject(t *testing.T, pj *ParsedJson) *Object {
	t.Helper()
	iter := pj.Iter()
	iter.AdvanceInto()
	_, root, err := iter.Root(nil)
	if err != nil {
		t.Fatalf("root failed: %v", err)
	}
	obj, err := root.Object(nil)
	if err != nil {
		t.Fatalf("object failed: %v", err)
	}
	return obj
}

func marshalRoot(t *testing.T, pj *ParsedJson) string {
	t.Helper()
	iter := pj.Iter()
	iter.AdvanceInto()
	_, root, err := iter.Root(nil)
	if err != nil {
		t.Fatalf("root failed: %v", err)
	}
	out, err := root.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	return string(out)
}

// checkTopKey reads a top-level integer key with every lookup method
// and verifies that all of them return the same value.
func checkTopKey(t *testing.T, pj *ParsedJson, key string, want int64) {
	t.Helper()

	// FindKey
	e := rootObject(t, pj).FindKey(key, nil)
	if e == nil {
		t.Errorf("FindKey(%q): not found", key)
	} else if v, err := e.Iter.Int(); err != nil || v != want {
		t.Errorf("FindKey(%q): got %d, err %v, want %d", key, v, err, want)
	}

	// FindPath
	e2, err := rootObject(t, pj).FindPath(nil, key)
	if err != nil {
		t.Errorf("FindPath(%q): %v", key, err)
	} else if v, err := e2.Iter.Int(); err != nil || v != want {
		t.Errorf("FindPath(%q): got %d, err %v, want %d", key, v, err, want)
	}

	// Map
	m, err := rootObject(t, pj).Map(nil)
	if err != nil {
		t.Errorf("Map: %v", err)
	} else if got, ok := m[key]; !ok {
		t.Errorf("Map: key %q not found", key)
	} else if got != want {
		t.Errorf("Map[%q]: got %v, want %d", key, got, want)
	}

	// Elements.Lookup
	elems, err := rootObject(t, pj).Parse(nil)
	if err != nil {
		t.Errorf("Parse elements: %v", err)
	} else if le := elems.Lookup(key); le == nil {
		t.Errorf("Elements.Lookup(%q): not found", key)
	} else if v, err := le.Iter.Int(); err != nil || v != want {
		t.Errorf("Elements.Lookup(%q): got %d, err %v, want %d", key, v, err, want)
	}
}

func TestDuplicateKeyPolicyConsistency(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	tests := []struct {
		name     string
		policy   DuplicateKeyPolicy
		input    string
		wantKeys map[string]int64
		wantJSON string
	}{
		{
			name:     "keep-first simple",
			policy:   DuplicateKeyKeepFirst,
			input:    `{"a":1,"a":2,"b":3,"a":4}`,
			wantKeys: map[string]int64{"a": 1, "b": 3},
			wantJSON: `{"a":1,"b":3}`,
		},
		{
			name:     "keep-last simple",
			policy:   DuplicateKeyKeepLast,
			input:    `{"a":1,"a":2,"b":3,"a":4}`,
			wantKeys: map[string]int64{"a": 4, "b": 3},
			wantJSON: `{"b":3,"a":4}`,
		},
		{
			name:     "keep-first triple",
			policy:   DuplicateKeyKeepFirst,
			input:    `{"a":1,"a":2,"a":3}`,
			wantKeys: map[string]int64{"a": 1},
			wantJSON: `{"a":1}`,
		},
		{
			name:     "keep-last triple",
			policy:   DuplicateKeyKeepLast,
			input:    `{"a":1,"a":2,"a":3}`,
			wantKeys: map[string]int64{"a": 3},
			wantJSON: `{"a":3}`,
		},
		{
			name:     "keep-first escaped key",
			policy:   DuplicateKeyKeepFirst,
			input:    `{"a\"b":1,"a\"b":2}`,
			wantKeys: map[string]int64{`a"b`: 1},
			wantJSON: `{"a\"b":1}`,
		},
		{
			name:     "keep-last empty objects",
			policy:   DuplicateKeyKeepLast,
			input:    `{"a":{},"b":1}`,
			wantKeys: map[string]int64{"b": 1},
			wantJSON: `{"a":{},"b":1}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pj := parseWithPolicy(t, test.input, test.policy)
			for k, v := range test.wantKeys {
				checkTopKey(t, pj, k, v)
			}
			if got := marshalRoot(t, pj); got != test.wantJSON {
				t.Errorf("marshal: got %s, want %s", got, test.wantJSON)
			}
		})
	}
}

func TestDuplicateKeyPolicyNested(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	tests := []struct {
		name     string
		policy   DuplicateKeyPolicy
		input    string
		wantJSON string
		// path -> expected integer value
		wantPaths map[string]int64
	}{
		{
			name:      "keep-first nested scopes independent",
			policy:    DuplicateKeyKeepFirst,
			input:     `{"a":1,"b":{"a":10,"a":20},"a":2}`,
			wantJSON:  `{"a":1,"b":{"a":10}}`,
			wantPaths: map[string]int64{"a": 1, "b/a": 10},
		},
		{
			name:      "keep-last nested scopes independent",
			policy:    DuplicateKeyKeepLast,
			input:     `{"a":1,"b":{"a":10,"a":20},"a":2}`,
			wantJSON:  `{"b":{"a":20},"a":2}`,
			wantPaths: map[string]int64{"a": 2, "b/a": 20},
		},
		{
			name:      "keep-first same key different objects untouched",
			policy:    DuplicateKeyKeepFirst,
			input:     `{"o1":{"k":1},"o2":{"k":2},"k":3}`,
			wantJSON:  `{"o1":{"k":1},"o2":{"k":2},"k":3}`,
			wantPaths: map[string]int64{"o1/k": 1, "o2/k": 2, "k": 3},
		},
		{
			name:      "keep-first objects in arrays",
			policy:    DuplicateKeyKeepFirst,
			input:     `{"c":[{"y":5,"y":6},{"y":7}]}`,
			wantJSON:  `{"c":[{"y":5},{"y":7}]}`,
			wantPaths: nil,
		},
		{
			name:      "keep-last objects in arrays",
			policy:    DuplicateKeyKeepLast,
			input:     `{"c":[{"y":5,"y":6},{"y":7}]}`,
			wantJSON:  `{"c":[{"y":6},{"y":7}]}`,
			wantPaths: nil,
		},
		{
			name:      "keep-first object values",
			policy:    DuplicateKeyKeepFirst,
			input:     `{"a":{"v":1},"a":{"v":2}}`,
			wantJSON:  `{"a":{"v":1}}`,
			wantPaths: map[string]int64{"a/v": 1},
		},
		{
			name:      "keep-last object values",
			policy:    DuplicateKeyKeepLast,
			input:     `{"a":{"v":1},"a":{"v":2}}`,
			wantJSON:  `{"a":{"v":2}}`,
			wantPaths: map[string]int64{"a/v": 2},
		},
		{
			name:      "keep-first array values",
			policy:    DuplicateKeyKeepFirst,
			input:     `{"a":[1],"a":[2]}`,
			wantJSON:  `{"a":[1]}`,
			wantPaths: nil,
		},
		{
			name:      "keep-first root array",
			policy:    DuplicateKeyKeepFirst,
			input:     `[{"a":1,"a":2}]`,
			wantJSON:  `[{"a":1}]`,
			wantPaths: nil,
		},
		{
			name:      "keep-first dup inside kept subtree",
			policy:    DuplicateKeyKeepFirst,
			input:     `{"a":{"x":1,"x":2},"a":3}`,
			wantJSON:  `{"a":{"x":1}}`,
			wantPaths: map[string]int64{"a/x": 1},
		},
		{
			name:      "keep-last dup inside discarded subtree",
			policy:    DuplicateKeyKeepLast,
			input:     `{"a":{"x":1,"x":2},"a":3}`,
			wantJSON:  `{"a":3}`,
			wantPaths: map[string]int64{"a": 3},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pj := parseWithPolicy(t, test.input, test.policy)
			if got := marshalRoot(t, pj); got != test.wantJSON {
				t.Errorf("marshal: got %s, want %s", got, test.wantJSON)
			}
			for path, want := range test.wantPaths {
				e, err := rootObject(t, pj).FindPath(nil, strings.Split(path, "/")...)
				if err != nil {
					t.Errorf("FindPath(%q): %v", path, err)
					continue
				}
				if v, err := e.Iter.Int(); err != nil || v != want {
					t.Errorf("FindPath(%q): got %d, err %v, want %d", path, v, err, want)
				}
			}
		})
	}
}

func TestDuplicateKeyPolicyReject(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	tests := []struct {
		name     string
		input    string
		ndjson   bool
		wantKey  string
		wantPath []string
		wantLine int
	}{
		{
			name:    "top level",
			input:   `{"a":1,"a":2}`,
			wantKey: "a",
		},
		{
			name:     "nested path",
			input:    `{"b":{"x":1,"x":2}}`,
			wantKey:  "x",
			wantPath: []string{"b"},
		},
		{
			name:     "deep nested path",
			input:    `{"a":{"b":{"c":1,"c":2}}}`,
			wantKey:  "c",
			wantPath: []string{"a", "b"},
		},
		{
			name:    "object in root array",
			input:   `[{"q":1,"q":2}]`,
			wantKey: "q",
		},
		{
			name:     "ndjson second line",
			input:    "{\"a\":1}\n{\"b\":2,\"b\":3}\n{\"c\":4}",
			ndjson:   true,
			wantKey:  "b",
			wantLine: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parse := Parse
			if test.ndjson {
				parse = ParseND
			}
			_, err := parse([]byte(test.input), nil, WithDuplicateKeyPolicy(DuplicateKeyReject))
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !errors.Is(err, ErrDuplicateKey) {
				t.Errorf("expected ErrDuplicateKey, got %v", err)
			}
			var dkErr *DuplicateKeyError
			if !errors.As(err, &dkErr) {
				t.Fatalf("expected *DuplicateKeyError, got %T", err)
			}
			if dkErr.Key != test.wantKey {
				t.Errorf("key: got %q, want %q", dkErr.Key, test.wantKey)
			}
			if fmt.Sprint(dkErr.Path) != fmt.Sprint(test.wantPath) {
				t.Errorf("path: got %v, want %v", dkErr.Path, test.wantPath)
			}
			if dkErr.Line != test.wantLine {
				t.Errorf("line: got %d, want %d", dkErr.Line, test.wantLine)
			}
			if !strings.Contains(dkErr.Error(), fmt.Sprintf("%q", test.wantKey)) {
				t.Errorf("error message %q does not contain key %q", dkErr.Error(), test.wantKey)
			}
		})
	}
}

func TestDuplicateKeyPolicyRejectClean(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	// Same key in different objects is not a duplicate.
	clean := []string{
		`{"a":{"b":1},"b":2}`,
		`{"o1":{"k":1},"o2":{"k":2},"k":3}`,
		`[{"a":1},{"a":2}]`,
		`{"a":[{"a":1}]}`,
		`{}`,
	}
	for _, input := range clean {
		_, err := Parse([]byte(input), nil, WithDuplicateKeyPolicy(DuplicateKeyReject))
		if err != nil {
			t.Errorf("input %s: unexpected error: %v", input, err)
		}
	}
}

func TestDuplicateKeyPolicyDefault(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	// Document the legacy behavior of the default policy:
	// lookups and output do not agree.
	pj, err := Parse([]byte(`{"a":1,"a":2}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	e := rootObject(t, pj).FindKey("a", nil)
	if e == nil {
		t.Fatal("FindKey: not found")
	}
	if v, _ := e.Iter.Int(); v != 1 {
		t.Errorf("FindKey: got %d, want 1 (first occurrence) on default policy", v)
	}
	m, err := rootObject(t, pj).Map(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := m["a"]; got != int64(2) {
		t.Errorf("Map: got %v, want 2 (last occurrence) on default policy", got)
	}
	if got := marshalRoot(t, pj); got != `{"a":1,"a":2}` {
		t.Errorf("marshal: got %s, want all occurrences on default policy", got)
	}
}

func TestDuplicateKeyPolicyInvalid(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	_, err := Parse([]byte(`{"a":1}`), nil, WithDuplicateKeyPolicy(DuplicateKeyPolicy(42)))
	if err == nil {
		t.Fatal("expected error for invalid policy, got nil")
	}
	if !strings.Contains(err.Error(), "invalid duplicate key policy") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDuplicateKeyPolicyNDJSON(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	input := "{\"a\":1,\"a\":2}\n{\"b\":10,\"a\":5,\"b\":20}\n{\"c\":3}"
	pj, err := ParseND([]byte(input), nil, WithDuplicateKeyPolicy(DuplicateKeyKeepLast))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{`{"a":2}`, `{"a":5,"b":20}`, `{"c":3}`}
	var got []string
	iter := pj.Iter()
	var root Iter
	for {
		tp, err := iter.AdvanceIter(&root)
		if err != nil {
			t.Fatal(err)
		}
		if tp == TypeNone {
			break
		}
		_, r, err := root.Root(nil)
		if err != nil {
			t.Fatal(err)
		}
		out, err := r.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, string(out))
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestDuplicateKeyPolicySerializeRoundTrip(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	input := `{"a":1,"a":2,"b":{"x":1,"x":2}}`
	pj := parseWithPolicy(t, input, DuplicateKeyKeepFirst)

	ser := NewSerializer()
	b := ser.Serialize(nil, *pj)
	pj2, err := ser.Deserialize(b, nil)
	if err != nil {
		t.Fatal(err)
	}
	checkTopKey(t, pj2, "a", 1)
	if got := marshalRoot(t, pj2); got != `{"a":1,"b":{"x":1}}` {
		t.Errorf("marshal after round trip: got %s", got)
	}
}

func TestDuplicateKeyPolicyNoCopyStrings(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj := parseWithPolicy(t, `{"a":1,"a":2}`, DuplicateKeyKeepLast, WithCopyStrings(false))
	checkTopKey(t, pj, "a", 2)
	if got := marshalRoot(t, pj); got != `{"a":2}` {
		t.Errorf("marshal: got %s", got)
	}
}

func TestDuplicateKeyPolicyLargeInput(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	// Inputs above 8KB are parsed with the concurrent stage 1+2 path.
	var sb strings.Builder
	sb.WriteString(`{"a":1,"a":2`)
	for i := 0; i < 2000; i++ {
		fmt.Fprintf(&sb, `,"k%d":%d`, i, i)
	}
	sb.WriteString(`,"a":3}`)
	input := sb.String()
	if len(input) <= 8<<10 {
		t.Fatalf("input too small for async path: %d bytes", len(input))
	}

	t.Run("keep-first", func(t *testing.T) {
		pj := parseWithPolicy(t, input, DuplicateKeyKeepFirst)
		checkTopKey(t, pj, "a", 1)
		checkTopKey(t, pj, "k123", 123)
	})
	t.Run("keep-last", func(t *testing.T) {
		pj := parseWithPolicy(t, input, DuplicateKeyKeepLast)
		checkTopKey(t, pj, "a", 3)
		checkTopKey(t, pj, "k123", 123)
	})
	t.Run("reject", func(t *testing.T) {
		_, err := Parse([]byte(input), nil, WithDuplicateKeyPolicy(DuplicateKeyReject))
		if !errors.Is(err, ErrDuplicateKey) {
			t.Fatalf("expected ErrDuplicateKey, got %v", err)
		}
	})
}

// TestDeleteElemsReaderConsistency verifies that all readers agree
// after elements have been deleted from the tape.
func TestDeleteElemsReaderConsistency(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	input := `{"one":1,"two":2,"three":3}`
	for _, del := range []string{"one", "two", "three"} {
		t.Run(del, func(t *testing.T) {
			pj, err := Parse([]byte(input), nil)
			if err != nil {
				t.Fatal(err)
			}
			obj := rootObject(t, pj)
			err = obj.DeleteElems(nil, map[string]struct{}{del: {}})
			if err != nil {
				t.Fatal(err)
			}
			for k, want := range map[string]int64{"one": 1, "two": 2, "three": 3} {
				if k == del {
					continue
				}
				checkTopKey(t, pj, k, want)
			}
			var remaining []string
			err = rootObject(t, pj).ForEach(func(key []byte, i Iter) {
				remaining = append(remaining, string(key))
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(remaining) != 2 {
				t.Errorf("ForEach: got %v, want 2 keys", remaining)
			}
		})
	}
}
