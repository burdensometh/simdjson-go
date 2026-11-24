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
	"testing"
)

func canon(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := canonicalValue(nil, v)
	if err != nil {
		t.Fatalf("canonicalValue(%#v): %v", v, err)
	}
	return string(b)
}

func TestCanonicalValueStability(t *testing.T) {
	same := [][2]interface{}{
		{map[string]interface{}{"a": int64(1), "b": "x"}, map[string]interface{}{"b": "x", "a": int64(1)}},
		{map[string]interface{}{"a": float64(10)}, map[string]interface{}{"a": uint64(10)}},
		{map[string]interface{}{"a": float64(1.5)}, map[string]interface{}{"a": float64(1.5)}},
		{[]interface{}{int64(1), "x"}, []interface{}{int64(1), "x"}},
		{map[string]interface{}{"a": []interface{}{map[string]interface{}{"b": true}}}, map[string]interface{}{"a": []interface{}{map[string]interface{}{"b": true}}}},
	}
	for i, c := range same {
		if a, b := canon(t, c[0]), canon(t, c[1]); a != b {
			t.Errorf("case %d: %q != %q", i, a, b)
		}
	}

	diff := [][2]interface{}{
		{map[string]interface{}{"a": int64(1)}, map[string]interface{}{"a": int64(2)}},
		{map[string]interface{}{"a": "1"}, map[string]interface{}{"a": int64(1)}},
		{map[string]interface{}{"a": nil}, map[string]interface{}{}},
		{map[string]interface{}{"a": []interface{}{int64(1)}}, map[string]interface{}{"a": []interface{}{int64(1), int64(1)}}},
		// Escaping must not let a value forge the framing.
		{map[string]interface{}{"a": `x","b":"y`}, map[string]interface{}{"a": "x", "b": "y"}},
		{map[string]interface{}{"a": "\x00"}, map[string]interface{}{"a": "\x01"}},
	}
	for i, c := range diff {
		if a, b := canon(t, c[0]), canon(t, c[1]); a == b {
			t.Errorf("case %d: both encoded as %q", i, a)
		}
	}
}

func TestVersionSortOrder(t *testing.T) {
	in := []Version{}
	for _, s := range []string{"5", "-1", "0", "10", "-3", "2"} {
		v, err := ParseVersion(s)
		if err != nil {
			t.Fatal(err)
		}
		in = append(in, v)
	}
	want := []string{"-3", "-1", "0", "2", "5", "10"}
	// Insertion sort keeps this test independent of sort.Slice behavior.
	for i := 1; i < len(in); i++ {
		for j := i; j > 0 && in[j].Compare(in[j-1]) < 0; j-- {
			in[j], in[j-1] = in[j-1], in[j]
		}
	}
	for i, v := range in {
		if v.String() != want[i] {
			t.Fatalf("got %v, want %v", in, want)
		}
	}
}
