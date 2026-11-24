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
	"testing"
)

// findRaw walks a parsed root and returns the iter of the named field.
func findRaw(t *testing.T, pj *ParsedJson, field string) Iter {
	t.Helper()
	var elem Iter
	if err := pj.ForEach(func(o Iter) error {
		var dst Element
		e, err := o.FindElement(&dst, field)
		if err != nil {
			return err
		}
		elem = e.Iter
		return nil
	}); err != nil {
		t.Fatalf("finding field %q: %v", field, err)
	}
	return elem
}

func TestRawNumberInteger(t *testing.T) {
	// Two ids beyond uint64 that collapse to the same float64.
	const id1 = "92233720368547758081234"
	const id2 = "92233720368547758081235"
	input := []byte(`{"order_id":` + id1 + `,"other_id":` + id2 + `,"count":7}`)

	// Default behaviour: big ids become floats and lose their identity.
	pj, err := Parse(input, nil)
	if err != nil {
		t.Fatal(err)
	}
	plain := findRaw(t, pj, "order_id")
	if plain.IsRawNumber() {
		t.Fatal("number should not be a raw number without the option")
	}
	if _, err := plain.RawNumber(); err == nil {
		t.Fatal("RawNumber must error when the feature is disabled")
	}
	fv, err := plain.Float()
	if err != nil {
		t.Fatal(err)
	}
	// Both distinct ids round to the same float, demonstrating the collision.
	plain2 := findRaw(t, pj, "other_id")
	fv2, _ := plain2.Float()
	if fv != fv2 {
		t.Fatalf("expected float collision, got %v != %v", fv, fv2)
	}

	// Opt-in: the named fields keep their exact lexical text.
	pj2, err := Parse(input, nil, WithRawNumbers("order_id", "other_id"))
	if err != nil {
		t.Fatal(err)
	}
	raw := findRaw(t, pj2, "order_id")
	if !raw.IsRawNumber() {
		t.Fatal("order_id should be a raw number")
	}
	got, err := raw.RawNumber()
	if err != nil {
		t.Fatal(err)
	}
	if got != id1 {
		t.Fatalf("raw integer mismatch: want %s, got %s", id1, got)
	}
	if s, _ := raw.String(); s != id1 {
		t.Fatalf("String() mismatch: want %s, got %s", id1, s)
	}

	raw2 := findRaw(t, pj2, "other_id")
	got2, _ := raw2.RawNumber()
	if got2 != id2 {
		t.Fatalf("raw integer mismatch: want %s, got %s", id2, got2)
	}
	if got == got2 {
		t.Fatal("distinct ids must remain distinct as raw numbers")
	}

	// Numeric accessors reject the preserved value instead of silently rounding.
	if _, err := raw.Uint(); err == nil {
		t.Fatal("Uint() on a raw number must fail")
	}
	if _, err := raw.Int(); err == nil {
		t.Fatal("Int() on a raw number must fail")
	}
	if _, err := raw.Float(); err == nil {
		t.Fatal("Float() on a raw number must fail")
	}

	// An unlisted numeric field keeps the ordinary numeric representation.
	count := findRaw(t, pj2, "count")
	if count.IsRawNumber() {
		t.Fatal("unlisted field must not be a raw number")
	}
	if n, err := count.Int(); err != nil || n != 7 {
		t.Fatalf("unlisted int field changed: n=%d err=%v", n, err)
	}
}

func TestRawNumberDecimalAndExponent(t *testing.T) {
	cases := []struct {
		field, lexical string
	}{
		{"decimal", "12.340"},      // trailing zero a float would drop
		{"negdecimal", "-0.00750"}, // leading zero and trailing zero
		{"exp", "1.23e3"},          // lower-case exponent
		{"expUpper", "7E-2"},       // upper-case exponent with sign
		{"expPlus", "2.5E+10"},     // explicit plus
		{"bigExp", "123456789012345678901.0e2"},
		{"plainZero", "0"},
		{"negZero", "-0"},
	}
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, c := range cases {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('"')
		buf.WriteString(c.field)
		buf.WriteString(`":`)
		buf.WriteString(c.lexical)
	}
	buf.WriteByte('}')

	fields := make([]string, len(cases))
	for i, c := range cases {
		fields[i] = c.field
	}
	pj, err := Parse(buf.Bytes(), nil, WithRawNumbers(fields...))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		raw := findRaw(t, pj, c.field)
		if !raw.IsRawNumber() {
			t.Errorf("%s: expected raw number", c.field)
			continue
		}
		got, err := raw.RawNumber()
		if err != nil {
			t.Errorf("%s: %v", c.field, err)
			continue
		}
		if got != c.lexical {
			t.Errorf("%s: lexical mismatch: want %s, got %s", c.field, c.lexical, got)
		}
	}
}

func TestRawNumberNestedAndArrays(t *testing.T) {
	input := []byte(`{
		"meta": {"id": 100000000000000000001, "note": "x"},
		"ids": [200000000000000000001, 200000000000000000002, {"inner": 300000000000000000003}],
		"matrix": [[400000000000000000001]],
		"plain": [1, 2, 3],
		"named": 5
	}`)
	pj, err := Parse(input, nil, WithRawNumbers("id", "ids", "matrix", "named"))
	if err != nil {
		t.Fatal(err)
	}

	// Nested object field by name.
	meta := findRaw(t, pj, "meta")
	var me Object
	if _, err := meta.Object(&me); err != nil {
		t.Fatal(err)
	}
	var it Iter
	name, typ, err := me.NextElement(&it)
	if err != nil {
		t.Fatal(err)
	}
	if name != "id" || !it.IsRawNumber() {
		t.Fatalf("nested id: name=%s type=%v raw=%v", name, typ, it.IsRawNumber())
	}
	if v, _ := it.RawNumber(); v != "100000000000000000001" {
		t.Fatalf("nested raw: %s", v)
	}

	// Object whose own field is not configured stays a normal string.
	var me2 Object
	meta.Object(&me2)
	var it2 Iter
	for {
		n, _, err := me2.NextElement(&it2)
		if err != nil {
			t.Fatal(err)
		}
		if n == "" {
			break
		}
		if n == "note" && it2.IsRawNumber() {
			t.Fatal("note must remain a regular string")
		}
	}

	// Raw array: direct and nested numbers preserved; the object inside uses
	// its own field rules (inner not listed => regular number).
	ids := findRaw(t, pj, "ids")
	var arr Array
	if _, err := ids.Array(&arr); err != nil {
		t.Fatal(err)
	}
	ai := arr.Iter()
	var want = []string{"200000000000000000001", "200000000000000000002"}
	for _, w := range want {
		if ai.Advance() != TypeString {
			t.Fatalf("expected raw number element, got %v", ai.Type())
		}
		if v, _ := ai.RawNumber(); v != w {
			t.Fatalf("array element: want %s got %s", w, v)
		}
	}
	// The object element inside the raw array is governed by its own keys.
	if ai.Advance() != TypeObject {
		t.Fatalf("expected nested object, got %v", ai.Type())
	}
	var innerObj Object
	if _, err := ai.Object(&innerObj); err != nil {
		t.Fatal(err)
	}
	var innerIter Iter
	if _, _, err := innerObj.NextElement(&innerIter); err != nil {
		t.Fatal(err)
	}
	if innerIter.IsRawNumber() {
		t.Fatal("unlisted nested object field must stay a regular number")
	}

	// Nested array inherits the raw scope.
	matrix := findRaw(t, pj, "matrix")
	var marr Array
	matrix.Array(&marr)
	outer := marr.Iter()
	if outer.Advance() != TypeArray {
		t.Fatalf("expected inner array, got %v", outer.Type())
	}
	var innerArr Array
	outer.Array(&innerArr)
	ii := innerArr.Iter()
	if ii.Advance() != TypeString {
		t.Fatalf("expected nested raw number, got %v", ii.Type())
	}
	if v, _ := ii.RawNumber(); v != "400000000000000000001" {
		t.Fatalf("nested array raw: %s", v)
	}

	// An unlisted array keeps ordinary numeric elements.
	plain := findRaw(t, pj, "plain")
	var parr Array
	plain.Array(&parr)
	pi := parr.Iter()
	if typ := pi.Advance(); typ != TypeInt && typ != TypeUint {
		t.Fatalf("unlisted array element changed to %v", typ)
	}
}

func TestRawNumberMarshalRoundTrip(t *testing.T) {
	input := []byte(`{"order_id":92233720368547758081234,"price":12.340,"e":1.2e3,"count":5}`)
	pj, err := Parse(input, nil, WithRawNumbers("order_id", "price", "e"))
	if err != nil {
		t.Fatal(err)
	}
	root := pj.Iter()
	out, err := root.MarshalJSONBuffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	// Raw numbers must be emitted verbatim, as unquoted JSON numbers.
	want := `{"order_id":92233720368547758081234,"price":12.340,"e":1.2e3,"count":5}`
	if string(out) != want {
		t.Fatalf("marshal mismatch:\nwant %s\ngot  %s", want, out)
	}

	// Re-parsing the output with the same option preserves identity again.
	pj2, err := Parse(out, nil, WithRawNumbers("order_id", "price", "e"))
	if err != nil {
		t.Fatal(err)
	}
	raw := findRaw(t, pj2, "order_id")
	if v, _ := raw.RawNumber(); v != "92233720368547758081234" {
		t.Fatalf("reparse raw mismatch: %s", v)
	}
}

func TestRawNumberSerializeRoundTrip(t *testing.T) {
	for _, mode := range []CompressMode{CompressNone, CompressFast, CompressDefault, CompressBest} {
		input := []byte(`{"order_id":92233720368547758081234,"ids":[1,200000000000000000001],"other":"str"}`)
		pj, err := Parse(input, nil, WithRawNumbers("order_id", "ids"))
		if err != nil {
			t.Fatal(err)
		}
		s := NewSerializer()
		s.CompressMode(mode)
		blob := s.Serialize(nil, *pj)

		var dst ParsedJson
		got, err := s.Deserialize(blob, &dst)
		if err != nil {
			t.Fatalf("mode %d: deserialize: %v", mode, err)
		}
		raw := findRaw(t, got, "order_id")
		if !raw.IsRawNumber() {
			t.Fatalf("mode %d: raw number marker lost across serialization", mode)
		}
		if v, _ := raw.RawNumber(); v != "92233720368547758081234" {
			t.Fatalf("mode %d: raw value %s", mode, v)
		}
		// The plain string survives unchanged.
		other := findRaw(t, got, "other")
		if other.IsRawNumber() {
			t.Fatalf("mode %d: regular string wrongly marked", mode)
		}
		if v, _ := other.String(); v != "str" {
			t.Fatalf("mode %d: regular string %q", mode, v)
		}
		// Marshaling the deserialized batch still yields the exact identity.
		groot := got.Iter()
		out, err := groot.MarshalJSONBuffer(nil)
		if err != nil {
			t.Fatalf("mode %d: marshal: %v", mode, err)
		}
		if !bytes.Contains(out, []byte("92233720368547758081234")) {
			t.Fatalf("mode %d: marshal lost exact number: %s", mode, out)
		}
	}
}

func TestRawNumberNDJSON(t *testing.T) {
	input := []byte("{\"order_id\":92233720368547758081234}\n{\"order_id\":92233720368547758081235}\n")
	pj, err := ParseND(input, nil, WithRawNumbers("order_id"))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	err = pj.ForEach(func(root Iter) error {
		var dst Element
		e, err := root.FindElement(&dst, "order_id")
		if err != nil {
			return err
		}
		v, err := e.Iter.RawNumber()
		if err != nil {
			return err
		}
		got = append(got, v)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"92233720368547758081234", "92233720368547758081235"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("ndjson raw numbers: %v", got)
	}
}

func TestRawNumberInvalidInputStillRejected(t *testing.T) {
	// A malformed number must not be accepted just because the field is raw.
	input := []byte(`{"order_id":12abc}`)
	if _, err := Parse(input, nil, WithRawNumbers("order_id")); err == nil {
		t.Fatal("invalid number should fail parsing")
	}
}

func TestRawNumberReuseAndSetString(t *testing.T) {
	input := []byte(`{"order_id":92233720368547758081234}`)
	pj, err := Parse(input, nil, WithRawNumbers("order_id"))
	if err != nil {
		t.Fatal(err)
	}
	// Reuse the parser; the option and exact value must hold across messages.
	pj2, err := Parse([]byte(`{"order_id":55}`), pj, WithRawNumbers("order_id"))
	if err != nil {
		t.Fatal(err)
	}
	raw := findRaw(t, pj2, "order_id")
	if v, _ := raw.RawNumber(); v != "55" {
		t.Fatalf("reuse raw mismatch: %s", v)
	}
	// Replacing with a regular string drops the raw marker and quotes output.
	if err := raw.SetString("abc"); err != nil {
		t.Fatal(err)
	}
	if raw.IsRawNumber() {
		t.Fatal("SetString must clear the raw number marker")
	}
	r2 := pj2.Iter()
	out, err := r2.MarshalJSONBuffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != `{"order_id":"abc"}` {
		t.Fatalf("post-set marshal mismatch: %s", out)
	}
}

func TestRawNumberBufferReuseSafety(t *testing.T) {
	// Even with WithCopyStrings(false), preserved numbers are copied into the
	// strings buffer so reusing the input buffer cannot corrupt them.
	input := []byte(`{"order_id":92233720368547758081234}`)
	pj, err := Parse(input, nil, WithCopyStrings(false), WithRawNumbers("order_id"))
	if err != nil {
		t.Fatal(err)
	}
	// Blank only the numeric token; the key remains readable from the message.
	for i := len(input) - 1; input[i] != ':'; i-- {
		input[i] = ' '
	}
	raw := findRaw(t, pj, "order_id")
	if v, _ := raw.RawNumber(); v != "92233720368547758081234" {
		t.Fatalf("raw number not copied safely: %s", v)
	}
}
