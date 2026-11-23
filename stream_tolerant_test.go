//go:build amd64 && !noasm && !appengine && gc
// +build amd64,!noasm,!appengine,gc

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
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// drainTolerant collects all values emitted to res (until io.EOF) and
// all quarantine events, returning them in delivery order per channel.
func drainTolerant(t *testing.T, input io.Reader, reuseCap int) (vals []map[string]interface{}, bads []BadLine, termErr error) {
	t.Helper()
	res := make(chan Stream, 4)
	bad := make(chan BadLine, 4)
	var reuse chan *ParsedJson
	if reuseCap > 0 {
		reuse = make(chan *ParsedJson, reuseCap)
	}

	ParseNDStreamTolerant(input, res, bad, reuse)

	done := false
	for !done {
		select {
		case got, ok := <-res:
			if !ok {
				done = true
				break
			}
			if got.Error != nil {
				if !errors.Is(got.Error, io.EOF) {
					termErr = got.Error
				}
				done = true
			} else {
				it := got.Value.Iter()
				roots, err := it.Interface()
				if err != nil {
					t.Fatalf("decoding result: %v", err)
				}
				for _, r := range roots.([]interface{}) {
					vals = append(vals, r.(map[string]interface{}))
				}
				if reuse != nil {
					select {
					case reuse <- got.Value:
					default:
					}
				}
			}
		case b, ok := <-bad:
			if !ok {
				continue
			}
			bads = append(bads, b)
		}
	}
	// res is closed; the forwarder closes bad afterwards, so drain
	// any quarantine events still in flight.
	for b := range bad {
		bads = append(bads, b)
	}
	return vals, bads, termErr
}

func TestParseNDStreamTolerantBadLine(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	input := strings.NewReader("{\"id\":1}\n{\"id\":2,\"broken\":\n{\"id\":3}\n")

	vals, bads, err := drainTolerant(t, input, 0)
	if err != nil {
		t.Fatalf("unexpected terminal error: %v", err)
	}
	if len(vals) != 2 {
		t.Fatalf("got %d valid records, want 2: %v", len(vals), vals)
	}
	if vals[0]["id"] != int64(1) || vals[1]["id"] != int64(3) {
		t.Errorf("valid records out of order: %v", vals)
	}
	if len(bads) != 1 {
		t.Fatalf("got %d bad lines, want 1: %v", len(bads), bads)
	}
	b := bads[0]
	if b.Line != 2 {
		t.Errorf("bad line number = %d, want 2", b.Line)
	}
	if want := `{"id":2,"broken":` + "\n"; string(b.Raw) != want {
		t.Errorf("raw line = %q, want %q", b.Raw, want)
	}
	if b.Err == nil || !strings.Contains(b.Err.Error(), "line 2") {
		t.Errorf("bad line error = %v, want an error mentioning line 2", b.Err)
	}
}

func TestParseNDStreamTolerantMultipleBadAndOrder(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	var sb strings.Builder
	// 200 lines, every 25th one malformed; long enough to span chunks.
	for i := 1; i <= 200; i++ {
		if i%25 == 0 {
			fmt.Fprintf(&sb, `{"id":%d,"oops":`+"\n", i) // truncated value
		} else {
			fmt.Fprintf(&sb, `{"id":%d}`+"\n", i)
		}
	}

	vals, bads, err := drainTolerant(t, strings.NewReader(sb.String()), 4)
	if err != nil {
		t.Fatalf("unexpected terminal error: %v", err)
	}
	if len(bads) != 8 {
		t.Fatalf("got %d bad lines, want 8", len(bads))
	}
	for j, b := range bads {
		if want := int64((j + 1) * 25); b.Line != want {
			t.Errorf("bad[%d].Line = %d, want %d", j, b.Line, want)
		}
		if !strings.HasPrefix(string(b.Raw), fmt.Sprintf(`{"id":%d,`, b.Line)) {
			t.Errorf("bad[%d] raw mismatch: %q", j, b.Raw)
		}
	}
	if len(vals) != 192 {
		t.Fatalf("got %d valid records, want 192", len(vals))
	}
	// Valid records must retain their original relative order.
	wantID := int64(1)
	for _, v := range vals {
		for wantID%25 == 0 {
			wantID++
		}
		if v["id"] != wantID {
			t.Fatalf("order broken: got id %v, want %d", v["id"], wantID)
		}
		wantID++
	}
}

func TestParseNDStreamTolerantLineNumbersAcrossChunks(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	// Build an input larger than one stream chunk so the bad line sits
	// in the second chunk and line numbering must carry over. Each
	// physical line is one valid (or, for line 3, malformed) object.
	line := `{"pad":"` + strings.Repeat("x", 700) + `"}` + "\n" // ~715 bytes/line
	var sb strings.Builder
	totalLines := streamChunkSize/len(line) + 10
	for i := 1; i <= totalLines; i++ {
		if i == 3 {
			sb.WriteString("{\"bad\":\n")
			continue
		}
		sb.WriteString(line)
	}

	vals, bads, err := drainTolerant(t, strings.NewReader(sb.String()), 0)
	if err != nil {
		t.Fatalf("unexpected terminal error: %v", err)
	}
	if len(bads) != 1 || bads[0].Line != 3 {
		t.Fatalf("bad lines = %v, want single bad line at line 3", summarizeBads(bads))
	}
	if len(vals) != totalLines-1 {
		t.Fatalf("got %d valid records, want %d", len(vals), totalLines-1)
	}
}

func summarizeBads(bads []BadLine) string {
	lines := make([]int64, len(bads))
	for i, b := range bads {
		lines[i] = b.Line
	}
	return fmt.Sprintf("lines %v", lines)
}

// scriptedReader returns one scripted result per Read call. Each entry
// supplies the byte count (copied from data at the running offset) and
// the error to return alongside it.
type scriptedReader struct {
	data []byte
	off  int
	step []struct {
		n   int
		err error
	}
	i int
}

func (r *scriptedReader) Read(p []byte) (int, error) {
	if r.i >= len(r.step) {
		return 0, errors.New("simulated reader failure")
	}
	s := r.step[r.i]
	r.i++
	if s.n > len(p) {
		panic("scripted step larger than caller buffer")
	}
	copy(p, r.data[r.off:r.off+s.n])
	r.off += s.n
	return s.n, s.err
}

func TestParseNDStreamTolerantReaderErrorFatal(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	// Script the underlying reads:
	//  1. fill the first 10MB internal buffer with valid lines,
	//  2. return one more valid line so the parser can complete a
	//     second chunk (this read also surfaces no error),
	//  3. fail.
	// The first chunk must already have been parsed and forwarded
	// before the terminal reader error arrives.
	line := `{"pad":"` + strings.Repeat("x", 500) + `"}` + "\n"
	first := strings.Repeat(line, streamChunkSize/len(line)) // exactly 10MB
	second := strings.Repeat(line, 10)
	data := first + second
	r := &scriptedReader{
		data: []byte(data),
		step: []struct {
			n   int
			err error
		}{
			{len(first), nil},
			{len(second), nil},
			{0, errors.New("simulated reader failure")},
		},
	}
	wantFirst := streamChunkSize / len(line)

	res := make(chan Stream, 4)
	bad := make(chan BadLine, 4)
	ParseNDStreamTolerant(r, res, bad, nil)

	var nVals int
	var termErr error
loop:
	for {
		select {
		case got, ok := <-res:
			if !ok {
				break loop
			}
			if got.Error != nil {
				termErr = got.Error
				break loop
			}
			it := got.Value.Iter()
			roots, err := it.Interface()
			if err != nil {
				t.Fatal(err)
			}
			nVals += len(roots.([]interface{}))
		case b, ok := <-bad:
			if !ok {
				// bad closes together with res; keep draining res
				// for the terminal error.
				continue
			}
			t.Fatalf("unexpected quarantine event: line %d", b.Line)
		}
	}
	if termErr == nil || !strings.Contains(termErr.Error(), "simulated reader failure") {
		t.Fatalf("terminal error = %v, want simulated reader failure", termErr)
	}
	// All completed chunks must survive the reader failure.
	if nVals < wantFirst {
		t.Fatalf("got %d records before reader error, want at least %d (completed position lost)", nVals, wantFirst)
	}
	// Both channels must be closed after the terminal error.
	if _, ok := <-res; ok {
		t.Fatal("res channel was not closed")
	}
	if _, ok := <-bad; ok {
		t.Fatal("bad channel was not closed")
	}
}

func TestParseNDStreamTolerantBlankLinesAndEOF(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	input := strings.NewReader("\n{\"id\":1}\n\n{\"id\":2}\n") // no trailing bad data

	vals, bads, err := drainTolerant(t, input, 0)
	if err != nil {
		t.Fatalf("unexpected terminal error: %v", err)
	}
	if len(bads) != 0 {
		t.Errorf("unexpected bad lines: %v", bads)
	}
	if len(vals) != 2 || vals[0]["id"] != int64(1) || vals[1]["id"] != int64(2) {
		t.Errorf("unexpected values: %v", vals)
	}
}

func TestParseNDStreamTolerantNoTrailingNewline(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	input := strings.NewReader("{\"id\":1}\n{\"broken\":\n{\"id\":3}")

	vals, bads, err := drainTolerant(t, input, 0)
	if err != nil {
		t.Fatalf("unexpected terminal error: %v", err)
	}
	if len(vals) != 2 || vals[0]["id"] != int64(1) || vals[1]["id"] != int64(3) {
		t.Errorf("unexpected values: %v", vals)
	}
	if len(bads) != 1 || bads[0].Line != 2 {
		t.Fatalf("bad lines = %v, want line 2", bads)
	}
	if want := `{"broken":` + "\n"; string(bads[0].Raw) != want {
		t.Errorf("raw = %q, want %q", bads[0].Raw, want)
	}
}

// Strict mode must keep its original behavior: any parse error ends the
// stream with that error, and no quarantine exists.
func TestParseNDStreamStrictStillFails(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	input := bytes.NewBufferString("{\"id\":1}\n{\"broken\":\n{\"id\":3}\n")
	res := make(chan Stream, 4)
	ParseNDStream(input, res, nil)

	var termErr error
	var good int
	for got := range res {
		if got.Error != nil {
			termErr = got.Error
			break
		}
		good++
	}
	if termErr == nil {
		t.Fatal("strict stream: expected a terminal error, got none")
	}
	if !errors.Is(termErr, io.EOF) && !strings.Contains(termErr.Error(), "parsing input") {
		t.Fatalf("unexpected terminal error: %v", termErr)
	}
	if good > 0 {
		t.Errorf("strict stream emitted %d blocks before the error; chunk should have failed atomically", good)
	}
}

func TestParseNDStreamTolerantNilQuarantinePanics(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on nil quarantine channel")
		}
	}()
	ParseNDStreamTolerant(strings.NewReader("{}\n"), make(chan Stream), nil, nil)
}

func TestParseNDStreamTolerantCleanInputMatchesStrict(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	var sb strings.Builder
	for i := 0; i < 500; i++ {
		fmt.Fprintf(&sb, `{"id":%d,"s":"value-%d"}`+"\n", i, i)
	}
	raw := sb.String()

	collect := func(tolerant bool) []interface{} {
		res := make(chan Stream, 8)
		var bad chan BadLine
		if tolerant {
			bad = make(chan BadLine, 8)
		}
		if tolerant {
			ParseNDStreamTolerant(strings.NewReader(raw), res, bad, nil)
		} else {
			ParseNDStream(strings.NewReader(raw), res, nil)
		}
		var out []interface{}
		for got := range res {
			if got.Error != nil {
				if errors.Is(got.Error, io.EOF) {
					break
				}
				t.Fatalf("unexpected error: %v", got.Error)
			}
			it := got.Value.Iter()
			roots, err := it.Interface()
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, roots.([]interface{})...)
		}
		if tolerant {
			for range bad {
			}
		}
		return out
	}

	strict := collect(false)
	tolerant := collect(true)
	if len(tolerant) != len(strict) {
		t.Fatalf("record count: tolerant %d, strict %d", len(tolerant), len(strict))
	}
	for i := range strict {
		a := fmt.Sprintf("%v", strict[i])
		b := fmt.Sprintf("%v", tolerant[i])
		if a != b {
			t.Fatalf("record %d differs: strict=%s tolerant=%s", i, a, b)
		}
	}
}
