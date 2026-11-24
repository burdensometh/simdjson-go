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
	"io"
	"reflect"
	"strings"
	"testing"
)

func tapeEqual(a, b []uint64) bool { return reflect.DeepEqual(a, b) }

// portableFixtures covers every value type, nesting, escapes, unicode,
// exact integers (including overflow), null handling and NDJSON.
var portableFixtures = []string{
	`{}`,
	`[]`,
	`{"a":null,"b":true,"c":false}`,
	`[null,true,false,1,-1,1.5,"s",{"k":"v"},[1,2,3]]`,
	`{"Image":{"Width":800,"Height":600,"Title":"View from 15th Floor","Thumbnail":{"Url":"http://www.example.com/image/481989943","Height":125,"Width":100},"Animated":false,"IDs":[116,943,234,38793]}}`,
	`{"escaped":"a\"b\\c\/d\b\f\n\r\t","unicode":"\u00e9\u4567\ud83d\ude00","low":"\udc00","bmp":"\u1234"}`,
	`{"big":9223372036854775807,"bigger":9223372036854775808,"huge":18446744073709551615,"overflow":99999999999999999999999999999999,"neg":-9223372036854775808,"negbig":-99999999999999999999}`,
	`{"f":1e10,"g":1.5e-3,"h":-0.0,"i":2.5E+4,"zero":0.000}`,
	`{"nested":[[[[{"deep":true}]]]],"emptyobj":{},"emptyarr":[]}`,
	`[ 1 , 2 , { "k" : "v" } , null ]`,
	"{\n\t\"pretty\": true,\r\n\t\"spaced\": \"x\"\n}",
	`{"utf8":"héllo→世界😊"}`,
	`{"keptraw":"plain ascii text"}`,
}

const portableNDJSON = `{"Image":{"Width":800,"Height":600,"Title":"View from 15th Floor","Thumbnail":{"Url":"http://www.example.com/image/481989943","Height":125,"Width":100},"Animated":false,"IDs":[116,943,234,38793]}}
{"Image":{"Width":801,"Height":601,"Title":"View from 15th Floor","Thumbnail":{"Url":"http://www.example.com/image/481989943","Height":125,"Width":100},"Animated":false,"IDs":[116,943,234,38793]}}
{"Image":{"Width":802,"Height":602,"Title":"View from 15th Floor","Thumbnail":{"Url":"http://www.example.com/image/481989943","Height":125,"Width":100},"Animated":false,"IDs":[116,943,234,38793]}}`

// invalidFixtures must be rejected by the portable parser.
var invalidFixtures = []string{
	``,
	`   `,
	`{`,
	`}`,
	`[`,
	`{"a"}`,
	`{"a":}`,
	`{"a":1,}`,
	`[1,]`,
	`tru`,
	`nul`,
	`falsx`,
	`{"a":tru}`,
	`01`,
	`1.`,
	`-`,
	`"unterminated`,
	`{"k":"bad \u12"}`,
	`{"k":"\ud800"}`,
	`{"k":"\ud800ሴ"}`,
	`{"k":"raw	ctl"}`,
	`[1 2]`,
	`{"a":1} extra`,
}

func TestPortableParserBasic(t *testing.T) {
	for _, tc := range portableFixtures {
		t.Run("parse", func(t *testing.T) {
			pj, err := Parse([]byte(tc), nil, WithParser(ParserGo))
			if err != nil {
				t.Fatalf("Parse(%q) portable: %v", tc, err)
			}
			if got := pj.Parser(); got != ParserGo {
				t.Errorf("Parser() = %v, want ParserGo", got)
			}
			if len(pj.Tape) == 0 {
				t.Fatalf("empty tape for %q", tc)
			}
		})
	}
}

func TestPortableParserRejectsInvalid(t *testing.T) {
	for _, tc := range invalidFixtures {
		if _, err := Parse([]byte(tc), nil, WithParser(ParserGo)); err == nil {
			t.Errorf("Parse(%q) expected error, got nil", tc)
		}
		if _, err := ParseND([]byte(tc), nil, WithParser(ParserGo)); err == nil {
			t.Errorf("ParseND(%q) expected error, got nil", tc)
		}
	}
}

// TestPortableMatchesSIMD compares the two parsers byte-for-byte on the tape
// and string buffer, proving both produce the same records, nulls and exact
// integers. Only runs where the SIMD implementation is available.
func TestPortableMatchesSIMD(t *testing.T) {
	if !SupportedCPU() {
		t.Skip("SIMD parser not available on this build")
	}
	for _, copyStrings := range []bool{true, false} {
		for _, tc := range portableFixtures {
			simdPJ, err := Parse([]byte(tc), nil, WithParser(ParserSIMD), WithCopyStrings(copyStrings))
			if err != nil {
				t.Fatalf("SIMD Parse(%q): %v", tc, err)
			}
			goPJ, err := Parse([]byte(tc), nil, WithParser(ParserGo), WithCopyStrings(copyStrings))
			if err != nil {
				t.Fatalf("Go Parse(%q): %v", tc, err)
			}
			if !tapeEqual(simdPJ.Tape, goPJ.Tape) {
				t.Errorf("tape mismatch for %q (copyStrings=%v)\nSIMD: %x\nGo:   %x", tc, copyStrings, simdPJ.Tape, goPJ.Tape)
			}
			if !bytes.Equal(simdPJ.Strings.B, goPJ.Strings.B) {
				t.Errorf("string buffer mismatch for %q (copyStrings=%v)\nSIMD: %q\nGo:   %q", tc, copyStrings, simdPJ.Strings.B, goPJ.Strings.B)
			}
		}
	}
}

// TestPortableNDJSONMatchesSIMD covers the multi-root tape layout.
func TestPortableNDJSONMatchesSIMD(t *testing.T) {
	if !SupportedCPU() {
		t.Skip("SIMD parser not available on this build")
	}
	simdPJ, err := ParseND([]byte(portableNDJSON), nil, WithParser(ParserSIMD))
	if err != nil {
		t.Fatal(err)
	}
	goPJ, err := ParseND([]byte(portableNDJSON), nil, WithParser(ParserGo))
	if err != nil {
		t.Fatal(err)
	}
	if !tapeEqual(simdPJ.Tape, goPJ.Tape) {
		t.Errorf("ndjson tape mismatch\nSIMD: %x\nGo:   %x", simdPJ.Tape, goPJ.Tape)
	}
	if !bytes.Equal(simdPJ.Strings.B, goPJ.Strings.B) {
		t.Errorf("ndjson string buffer mismatch")
	}

	// Traversal via ForEach must deliver the same number of roots/records.
	var simdRoots, goRoots int
	if err = simdPJ.ForEach(func(Iter) error { simdRoots++; return nil }); err != nil {
		t.Fatal(err)
	}
	if err = goPJ.ForEach(func(Iter) error { goRoots++; return nil }); err != nil {
		t.Fatal(err)
	}
	if simdRoots != 3 || goRoots != 3 {
		t.Errorf("root count simd=%d go=%d, want 3", simdRoots, goRoots)
	}
}

// TestPortableProjectionAndTraversal exercises the existing traversal and
// projection (FindElement/Object/Array/Interface) API on the portable tape.
func TestPortableProjectionAndTraversal(t *testing.T) {
	pj, err := Parse([]byte(portableFixtures[4]), nil, WithParser(ParserGo))
	if err != nil {
		t.Fatal(err)
	}
	var seen string
	err = pj.ForEach(func(i Iter) error {
		elem, err := i.FindElement(nil, "Image", "Thumbnail", "Url")
		if err != nil {
			return err
		}
		v, err := elem.Iter.StringCvt()
		if err != nil {
			return err
		}
		seen = v
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen != "http://www.example.com/image/481989943" {
		t.Errorf("projected url = %q", seen)
	}

	// Exact integer types must be preserved.
	numPJ, err := Parse([]byte(portableFixtures[6]), nil, WithParser(ParserGo))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]interface{}
	if err = numPJ.ForEach(func(i Iter) error {
		obj, err := i.Object(nil)
		if err != nil {
			return err
		}
		m, err = obj.Map(nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := m["big"].(int64); !ok {
		t.Errorf("big expected int64, got %T", m["big"])
	}
	if _, ok := m["bigger"].(uint64); !ok {
		t.Errorf("bigger expected uint64, got %T", m["bigger"])
	}
	if _, ok := m["huge"].(uint64); !ok {
		t.Errorf("huge expected uint64, got %T", m["huge"])
	}
	if _, ok := m["overflow"].(float64); !ok {
		t.Errorf("overflow expected float64, got %T", m["overflow"])
	}
	if m["neg"].(int64) != -9223372036854775808 {
		t.Errorf("neg exact int mismatch: %v", m["neg"])
	}

	// Null stays null.
	nullPJ, err := Parse([]byte(`{"a":null}`), nil, WithParser(ParserGo))
	if err != nil {
		t.Fatal(err)
	}
	var nm map[string]interface{}
	_ = nullPJ.ForEach(func(i Iter) error {
		o, _ := i.Object(nil)
		nm, _ = o.Map(nil)
		return nil
	})
	if v, ok := nm["a"]; !ok || v != nil {
		t.Errorf("null projection got %v ok=%v", v, ok)
	}

	// Re-serialization round trip matches stdlib semantics. The lone low
	// surrogate fixture is excluded because the SIMD parser preserves it as
	// raw WTF-8 bytes while encoding/json normalizes it to U+FFFD; byte-level
	// equivalence with SIMD is covered by TestPortableMatchesSIMD.
	for _, tc := range portableFixtures {
		if strings.Contains(tc, `\udc00`) {
			continue
		}
		p, err := Parse([]byte(tc), nil, WithParser(ParserGo))
		if err != nil {
			t.Fatalf("parse %q: %v", tc, err)
		}
		it := p.Iter()
		out, err := it.MarshalJSON()
		if err != nil {
			t.Fatalf("marshal %q: %v", tc, err)
		}
		var want, got interface{}
		if err := json.Unmarshal([]byte(tc), &want); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(out, &got); err != nil {
			t.Fatalf("re-unmarshal %q: %v", tc, err)
		}
		if !jsonEqual(want, got) {
			t.Errorf("roundtrip mismatch for %q:\n want %v\n got  %v", tc, want, got)
		}
	}
}

func jsonEqual(a, b interface{}) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return bytes.Equal(ab, bb)
}

// TestPortableParserSelection verifies the reported parsing path for every
// selection mode and that forcing SIMD fails on unsupported CPUs.
func TestPortableParserSelection(t *testing.T) {
	// Default (auto) must work and report a concrete path.
	pj, err := Parse([]byte(`{"a":1}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if pj.Parser() != ParserSIMD && pj.Parser() != ParserGo {
		t.Errorf("auto resolved to unexpected %v", pj.Parser())
	}
	pjGo, err := Parse([]byte(`{"a":1}`), nil, WithParser(ParserGo))
	if err != nil {
		t.Fatal(err)
	}
	if pjGo.Parser() != ParserGo {
		t.Errorf("want ParserGo, got %v", pjGo.Parser())
	}
	if !SupportedCPU() {
		if _, err := Parse([]byte(`{"a":1}`), nil, WithParser(ParserSIMD)); err == nil {
			t.Error("ParserSIMD should fail on unsupported CPU")
		}
	} else {
		pjSimd, err := Parse([]byte(`{"a":1}`), nil, WithParser(ParserSIMD))
		if err != nil {
			t.Fatal(err)
		}
		if pjSimd.Parser() != ParserSIMD {
			t.Errorf("want ParserSIMD, got %v", pjSimd.Parser())
		}
	}
	if ParserGo.String() != "go" || ParserSIMD.String() != "simd" || ParserAuto.String() != "auto" {
		t.Error("unexpected parser name")
	}
}

// TestPortableNDStream exercises the streaming entry on the portable path.
func TestPortableNDStream(t *testing.T) {
	res := make(chan Stream, 4)
	reuse := make(chan *ParsedJson, 4)
	ParseNDStream(strings.NewReader(portableNDJSON+"\n"), res, reuse, WithParser(ParserGo))

	var roots int
	var finished bool
	for s := range res {
		if s.Error != nil {
			if s.Error == io.EOF {
				finished = true
				continue
			}
			t.Fatalf("stream error: %v", s.Error)
		}
		if s.Value.Parser() != ParserGo {
			t.Errorf("stream value parser = %v, want ParserGo", s.Value.Parser())
		}
		if err := s.Value.ForEach(func(Iter) error { roots++; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if !finished {
		t.Error("stream did not finish with io.EOF")
	}
	if roots != 3 {
		t.Errorf("stream roots = %d, want 3", roots)
	}
}

// TestPortableFullAPI exercises the whole public surface on the portable
// tape so it runs (and provides coverage) on machines without SIMD.
func TestPortableFullAPI(t *testing.T) {
	input := `{"nums":[1,2,3],"words":["a","b"],"obj":{"k":42},"flag":true,"nothing":null}`
	pj, err := Parse([]byte(input), nil, WithParser(ParserGo))
	if err != nil {
		t.Fatal(err)
	}

	var obj *Object
	if err = pj.ForEach(func(i Iter) error {
		obj, err = i.Object(obj)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// Object.Parse + Elements lookup.
	var elems *Elements
	elems, err = obj.Parse(elems)
	if err != nil {
		t.Fatal(err)
	}
	if e := elems.Lookup("flag"); e == nil || e.Type != TypeBool {
		t.Errorf("flag lookup got %v", e)
	}

	// Array typed projections preserve exact integers and strings.
	numsElem := elems.Lookup("nums")
	if numsElem == nil {
		t.Fatal("nums missing")
	}
	var arr *Array
	arr, err = numsElem.Iter.Array(arr)
	if err != nil {
		t.Fatal(err)
	}
	ints, err := arr.AsInteger()
	if err != nil {
		t.Fatal(err)
	}
	if len(ints) != 3 || ints[0] != 1 || ints[2] != 3 {
		t.Errorf("AsInteger = %v", ints)
	}
	wordsElem := elems.Lookup("words")
	var warr *Array
	warr, err = wordsElem.Iter.Array(warr)
	if err != nil {
		t.Fatal(err)
	}
	words, err := warr.AsString()
	if err != nil {
		t.Fatal(err)
	}
	if len(words) != 2 || words[0] != "a" || words[1] != "b" {
		t.Errorf("AsString = %v", words)
	}

	// FindPath projection into a nested object. Locate the nested object
	// element first, then look the key up inside it.
	objElem := elems.Lookup("obj")
	if objElem == nil {
		t.Fatal("obj missing")
	}
	var innerObj *Object
	innerObj, err = objElem.Iter.Object(innerObj)
	if err != nil {
		t.Fatal(err)
	}
	kelem, err := innerObj.FindPath(nil, "k")
	if err != nil {
		t.Fatal(err)
	}
	if v, err := kelem.Iter.Int(); err != nil || v != 42 {
		t.Errorf("nested k = %v, %v", v, err)
	}

	// In-place value replacement (SetInt/SetBool/SetNull).
	if err = kelem.Iter.SetInt(7); err != nil {
		t.Fatal(err)
	}
	flagIter := elems.Lookup("flag").Iter
	if err = flagIter.SetBool(false); err != nil {
		t.Fatal(err)
	}
	nothingIter := elems.Lookup("nothing").Iter
	if err = nothingIter.SetNull(); err != nil {
		t.Fatal(err)
	}

	// Element deletion. Use a fresh object iterator since obj was advanced
	// by Parse/FindPath above.
	var delObj *Object
	if err = pj.ForEach(func(i Iter) error {
		delObj, err = i.Object(delObj)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err = delObj.DeleteElems(func(key []byte, i Iter) bool {
		return string(key) == "words"
	}, nil); err != nil {
		t.Fatal(err)
	}

	rootIt := pj.Iter()
	out, err := rootIt.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]interface{}
	if err = json.Unmarshal(out, &decoded); err != nil {
		t.Fatalf("output not valid json: %v (%s)", err, out)
	}
	if _, ok := decoded["words"]; ok {
		t.Error("words should have been deleted")
	}
	if decoded["nothing"] != nil {
		t.Error("nothing should remain null")
	}
	if m, _ := decoded["obj"].(map[string]interface{}); m["k"].(float64) != 7 {
		t.Errorf("k replacement failed: %v", decoded["obj"])
	}

	// Serialization round trip must preserve the output across all modes.
	s := NewSerializer()
	var target *ParsedJson
	for _, mode := range []CompressMode{CompressNone, CompressFast, CompressDefault, CompressBest} {
		s.CompressMode(mode)
		target, err = s.Deserialize(s.Serialize(nil, *pj), target)
		if err != nil {
			t.Fatalf("mode %d: %v", mode, err)
		}
		it := target.Iter()
		got, err := it.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, out) {
			t.Errorf("serialize mode %d mismatch\nwant %s\ngot  %s", mode, out, got)
		}
	}
}

// TestPortableReuse ensures a previously used ParsedJson can be recycled,
// including switching implementations between calls without option leakage.
func TestPortableReuse(t *testing.T) {
	pj, err := Parse([]byte(`{"a":1}`), nil, WithParser(ParserGo))
	if err != nil {
		t.Fatal(err)
	}
	pj2, err := Parse([]byte(`{"b":[1,2,3]}`), pj, WithParser(ParserGo))
	if err != nil {
		t.Fatal(err)
	}
	if pj2.Parser() != ParserGo {
		t.Errorf("reused parser = %v", pj2.Parser())
	}
	it := pj2.Iter()
	out, err := it.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != `{"b":[1,2,3]}` {
		t.Errorf("reuse output = %s", out)
	}

	if SupportedCPU() {
		// A portable ParsedJson reused without an explicit option must fall
		// back to the auto-selected (SIMD) path rather than retain ParserGo.
		pj3, err := Parse([]byte(`{"c":true}`), pj2)
		if err != nil {
			t.Fatal(err)
		}
		if pj3.Parser() != ParserSIMD {
			t.Errorf("auto reuse parser = %v, want ParserSIMD", pj3.Parser())
		}
	}
}
