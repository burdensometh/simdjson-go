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
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func writeInput(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// readAll reads every line, committing each one, and returns the lines.
func readAll(t *testing.T, r *NDJSONReader) []string {
	t.Helper()
	var got []string
	for {
		line, err := r.Next()
		if err == io.EOF {
			return got
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		got = append(got, string(line))
		if err := r.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
}

func TestNDJSONReaderBasic(t *testing.T) {
	const input = "{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n"
	dir := t.TempDir()
	path := writeInput(t, dir, "in.ndjson", input)

	r, err := OpenNDJSON(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if got := readAll(t, r); !reflect.DeepEqual(got, []string{`{"a":1}`, `{"b":2}`, `{"c":3}`}) {
		t.Fatalf("got %q", got)
	}
	cp := r.Checkpoint()
	if cp.Offset != int64(len(input)) {
		t.Errorf("offset = %d, want %d", cp.Offset, len(input))
	}
	if cp.Records != 3 {
		t.Errorf("records = %d, want 3", cp.Records)
	}
	if cp.Unterminated {
		t.Error("checkpoint unexpectedly marked unterminated")
	}
	if _, err := r.Next(); err != io.EOF {
		t.Errorf("Next after end = %v, want io.EOF", err)
	}
}

func TestNDJSONReaderSkipsBlankLinesAndCRLF(t *testing.T) {
	const input = "\n{\"a\":1}\r\n \n\r\n\t\n{\"b\":2}\r\n"
	dir := t.TempDir()
	path := writeInput(t, dir, "in.ndjson", input)

	r, err := OpenNDJSON(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if got := readAll(t, r); !reflect.DeepEqual(got, []string{`{"a":1}`, `{"b":2}`}) {
		t.Fatalf("got %q", got)
	}
	if cp := r.Checkpoint(); cp.Records != 2 || cp.Offset != int64(len(input)) {
		t.Errorf("checkpoint = %+v", cp)
	}
}

func TestNDJSONReaderResumeAcrossRestarts(t *testing.T) {
	const n = 40
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "{\"i\":%d}\n", i)
	}
	input := b.String()

	dir := t.TempDir()
	path := writeInput(t, dir, "in.ndjson", input)
	cpPath := filepath.Join(dir, "progress.json")

	var got []string
	var cp *NDJSONCheckpoint
	// Restart every 7 lines to exercise reopening at many offsets.
	for restarts := 0; ; restarts++ {
		r, err := OpenNDJSON(path, cp)
		if err != nil {
			t.Fatalf("restart %d: %v", restarts, err)
		}
		stop := len(got) + 7
		for len(got) < stop {
			line, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, string(line))
			if err := r.Commit(); err != nil {
				t.Fatal(err)
			}
		}
		if err := r.SaveCheckpointFile(cpPath); err != nil {
			t.Fatal(err)
		}
		saved := r.Checkpoint()
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if len(got) == n {
			break
		}
		loaded, err := LoadCheckpointFile(cpPath)
		if err != nil {
			t.Fatal(err)
		}
		if loaded != saved {
			t.Fatalf("saved %+v != loaded %+v", saved, loaded)
		}
		cp = &loaded
	}

	want := make([]string, n)
	for i := range want {
		want[i] = fmt.Sprintf(`{"i":%d}`, i)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("order mismatch:\n got: %q\nwant: %q", got, want)
	}
}

func TestNDJSONReaderUncommittedLineRepeats(t *testing.T) {
	dir := t.TempDir()
	path := writeInput(t, dir, "in.ndjson", "one\ntwo\nthree\n")

	r, err := OpenNDJSON(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	line, err := r.Next()
	if err != nil || string(line) != "one" {
		t.Fatalf("Next = %q, %v", line, err)
	}
	if err := r.Commit(); err != nil {
		t.Fatal(err)
	}
	// Read but deliberately do not commit; simulate a crash.
	line, err = r.Next()
	if err != nil || string(line) != "two" {
		t.Fatalf("Next = %q, %v", line, err)
	}
	cp := r.Checkpoint()
	if cp.Records != 1 || cp.Offset != int64(len("one\n")) {
		t.Fatalf("uncommitted line leaked into checkpoint: %+v", cp)
	}
	r.Close()

	r, err = OpenNDJSON(path, &cp)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if got := readAll(t, r); !reflect.DeepEqual(got, []string{"two", "three"}) {
		t.Fatalf("got %q", got)
	}
}

func TestNDJSONReaderCommitStateErrors(t *testing.T) {
	dir := t.TempDir()
	path := writeInput(t, dir, "in.ndjson", "one\ntwo\n")
	r, err := OpenNDJSON(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if err := r.Commit(); !errors.Is(err, ErrNoPendingLine) {
		t.Fatalf("Commit without Next = %v, want ErrNoPendingLine", err)
	}
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); !errors.Is(err, ErrUnackedLine) {
		t.Fatalf("Next without Commit = %v, want ErrUnackedLine", err)
	}
	if err := r.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); !errors.Is(err, ErrReaderClosed) {
		t.Fatalf("Next after Close = %v, want ErrReaderClosed", err)
	}
	if err := r.Commit(); !errors.Is(err, ErrReaderClosed) {
		t.Fatalf("Commit after Close = %v, want ErrReaderClosed", err)
	}
}

func TestNDJSONReaderEmptyFileThenGrows(t *testing.T) {
	dir := t.TempDir()
	path := writeInput(t, dir, "in.ndjson", "")

	r, err := OpenNDJSON(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("Next = %v, want io.EOF", err)
	}
	cp := r.Checkpoint()
	if cp.Offset != 0 || cp.Records != 0 || cp.Size != 0 {
		t.Fatalf("unexpected checkpoint: %+v", cp)
	}
	r.Close()

	if err := os.WriteFile(path, []byte("a\nb\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r, err = OpenNDJSON(path, &cp)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if got := readAll(t, r); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("got %q", got)
	}
}

func TestNDJSONReaderEmptyFileGrowsWhileOpen(t *testing.T) {
	dir := t.TempDir()
	path := writeInput(t, dir, "in.ndjson", "")

	r, err := OpenNDJSON(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("Next = %v, want io.EOF", err)
	}
	// The file grows while the reader is still open, e.g. a log being appended.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("a\nb\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	if got := readAll(t, r); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("got %q", got)
	}
	cp := r.Checkpoint()
	if cp.HeadLen == 0 {
		t.Fatalf("checkpoint has no input identity: %+v", cp)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	// The checkpoint must be usable to reopen.
	r, err = OpenNDJSON(path, &cp)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("Next = %v, want io.EOF", err)
	}
	r.Close()
}

func TestNDJSONReaderUnterminatedGrowsWhileOpen(t *testing.T) {
	dir := t.TempDir()
	path := writeInput(t, dir, "in.ndjson", "one\ntwo")

	r, err := OpenNDJSON(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, r); !reflect.DeepEqual(got, []string{"one", "two"}) {
		t.Fatalf("got %q", got)
	}
	if !r.Checkpoint().Unterminated {
		t.Fatal("checkpoint should be unterminated")
	}

	// Terminator and a new record are appended while the reader stays open.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("\nthree\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	line, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(line) != "three" {
		t.Fatalf("got %q, want %q (record must not repeat)", line, "three")
	}
	if err := r.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("Next = %v, want io.EOF", err)
	}
	r.Close()

	r, err = OpenNDJSON(path, &NDJSONCheckpoint{})
	if err == nil {
		r.Close()
		t.Fatal("empty checkpoint value must be rejected")
	}
}

func TestNDJSONReaderUnterminatedGrowsMidRecordWhileOpen(t *testing.T) {
	dir := t.TempDir()
	path := writeInput(t, dir, "in.ndjson", "one\ntwo")

	r, err := OpenNDJSON(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, r); !reflect.DeepEqual(got, []string{"one", "two"}) {
		t.Fatalf("got %q", got)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("three\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	if _, err := r.Next(); !errors.Is(err, ErrCheckpointMidLine) {
		t.Fatalf("Next = %v, want ErrCheckpointMidLine", err)
	}
	r.Close()
}

// TestNDJSONReaderResumeAtEveryBoundary reopens a saved checkpoint at every
// line boundary and checks that the remaining records are exactly the tail of
// the input, in order.
func TestNDJSONReaderResumeAtEveryBoundary(t *testing.T) {
	const n = 30
	want := make([]string, n)
	var b strings.Builder
	for i := 0; i < n; i++ {
		want[i] = fmt.Sprintf(`{"i":%d}`, i)
		fmt.Fprintf(&b, "%s\n", want[i])
	}
	dir := t.TempDir()
	path := writeInput(t, dir, "in.ndjson", b.String())

	// Collect the checkpoint after committing k records, for every k.
	cps := make([]NDJSONCheckpoint, n+1)
	r, err := OpenNDJSON(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	cps[0] = r.Checkpoint()
	for k := 1; k <= n; k++ {
		if _, err := r.Next(); err != nil {
			t.Fatal(err)
		}
		if err := r.Commit(); err != nil {
			t.Fatal(err)
		}
		cps[k] = r.Checkpoint()
	}
	r.Close()

	for k := 0; k <= n; k++ {
		cp := cps[k]
		r, err := OpenNDJSON(path, &cp)
		if err != nil {
			t.Fatalf("k=%d: %v", k, err)
		}
		got := readAll(t, r)
		r.Close()
		if !slices.Equal(got, want[k:]) {
			t.Fatalf("k=%d:\n got: %q\nwant: %q", k, got, want[k:])
		}
	}
}

func TestNDJSONReaderFinalLineWithoutNewline(t *testing.T) {
	dir := t.TempDir()
	path := writeInput(t, dir, "in.ndjson", "one\ntwo")

	r, err := OpenNDJSON(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, r); !reflect.DeepEqual(got, []string{"one", "two"}) {
		t.Fatalf("got %q", got)
	}
	cp := r.Checkpoint()
	if !cp.Unterminated {
		t.Fatalf("checkpoint should be unterminated: %+v", cp)
	}
	if cp.Offset != int64(len("one\ntwo")) {
		t.Fatalf("offset = %d", cp.Offset)
	}
	r.Close()

	// Same file, no appended data: the record must not repeat.
	r, err = OpenNDJSON(path, &cp)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("Next = %v, want io.EOF", err)
	}
	r.Close()

	// Append data with a terminator: the terminator is consumed and the new
	// record is returned exactly once.
	if err := os.WriteFile(path, []byte("one\ntwo\nthree\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r, err = OpenNDJSON(path, &cp)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if got := readAll(t, r); !reflect.DeepEqual(got, []string{"three"}) {
		t.Fatalf("got %q", got)
	}
}

func TestNDJSONReaderUnterminatedThenAppendedWithoutTerminator(t *testing.T) {
	dir := t.TempDir()
	path := writeInput(t, dir, "in.ndjson", "one\ntwo")

	r, err := OpenNDJSON(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, r); !reflect.DeepEqual(got, []string{"one", "two"}) {
		t.Fatalf("got %q", got)
	}
	cp := r.Checkpoint()
	r.Close()

	// Appending without a separator would make the committed offset point
	// into the middle of a record.
	if err := os.WriteFile(path, []byte("one\ntwothree\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenNDJSON(path, &cp); !errors.Is(err, ErrCheckpointMidLine) {
		t.Fatalf("OpenNDJSON = %v, want ErrCheckpointMidLine", err)
	}
}

func TestNDJSONReaderWrongFile(t *testing.T) {
	dir := t.TempDir()
	path := writeInput(t, dir, "in.ndjson", "one\ntwo\nthree\n")

	r, err := OpenNDJSON(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if err := r.Commit(); err != nil {
		t.Fatal(err)
	}
	cp := r.Checkpoint()
	r.Close()

	// Different input, same path.
	if err := os.WriteFile(path, []byte("ONE\ntwo\nthree\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenNDJSON(path, &cp); !errors.Is(err, ErrCheckpointMismatch) {
		t.Fatalf("changed head: OpenNDJSON = %v, want ErrCheckpointMismatch", err)
	}

	// Same input, different location.
	if err := os.WriteFile(path, []byte("one\ntwo\nthree\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(dir, "other.ndjson")
	if err := os.WriteFile(other, []byte("one\ntwo\nthree\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenNDJSON(other, &cp); !errors.Is(err, ErrCheckpointMismatch) {
		t.Fatalf("renamed input: OpenNDJSON = %v, want ErrCheckpointMismatch", err)
	}

	// Truncated input.
	if err := os.WriteFile(path, []byte("on"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenNDJSON(path, &cp); !errors.Is(err, ErrCheckpointMismatch) {
		t.Fatalf("truncated input: OpenNDJSON = %v, want ErrCheckpointMismatch", err)
	}
}

func TestNDJSONReaderBoundaryChangeDetected(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 200; i++ {
		fmt.Fprintf(&b, "{\"i\":%04d,\"pad\":\"%s\"}\n", i, strings.Repeat("x", 600))
	}
	input := b.String()
	dir := t.TempDir()
	path := writeInput(t, dir, "in.ndjson", input)

	r, err := OpenNDJSON(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 150; i++ {
		if _, err := r.Next(); err != nil {
			t.Fatal(err)
		}
		if err := r.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	cp := r.Checkpoint()
	r.Close()
	// The checkpoint offset is well beyond the hashed head.
	if cp.Offset <= ndjsonHeadSize {
		t.Fatalf("offset %d must exceed head hash size %d", cp.Offset, ndjsonHeadSize)
	}

	// Change one byte in the already consumed region, outside the head.
	mutated := []byte(input)
	mutated[cp.Offset-100] ^= 0x01
	if err := os.WriteFile(path, mutated, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenNDJSON(path, &cp); !errors.Is(err, ErrCheckpointMismatch) {
		t.Fatalf("OpenNDJSON = %v, want ErrCheckpointMismatch", err)
	}
}

func TestNDJSONReaderMidLineOffset(t *testing.T) {
	const input = "aaa\nbbb\nccc\n"
	dir := t.TempDir()
	path := writeInput(t, dir, "in.ndjson", input)

	// A checkpoint whose offset falls inside "bbb", with a boundary hash that
	// legitimately matches the bytes before that offset.
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h, err := hashFileAt(f, 0, len(input))
	if err != nil {
		t.Fatal(err)
	}
	cp := NDJSONCheckpoint{
		Version:     NDJSONCheckpointVersion,
		Name:        path,
		Offset:      6, // "aaa\nbb"
		Size:        int64(len(input)),
		HeadLen:     len(input),
		HeadHash:    h,
		BoundaryLen: 6,
	}
	cp.BoundaryHash, err = hashFileAt(f, 0, 6)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenNDJSON(path, &cp); !errors.Is(err, ErrCheckpointMidLine) {
		t.Fatalf("OpenNDJSON = %v, want ErrCheckpointMidLine", err)
	}

	// Offset 0 is never mid-line.
	cp.BoundaryLen, cp.BoundaryHash = 0, 0
	cp.Offset = 0
	r, err := OpenNDJSON(path, &cp)
	if err != nil {
		t.Fatalf("offset 0: %v", err)
	}
	r.Close()

	// The end of the input also follows a newline.
	cp.Offset = int64(len(input))
	cp.BoundaryLen = len(input)
	cp.BoundaryHash, err = hashFileAt(f, 0, len(input))
	if err != nil {
		t.Fatal(err)
	}
	r, err = OpenNDJSON(path, &cp)
	if err != nil {
		t.Fatalf("offset %d: %v", cp.Offset, err)
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("Next = %v, want io.EOF", err)
	}
	r.Close()
}

func TestNDJSONCheckpointValidation(t *testing.T) {
	valid := NDJSONCheckpoint{
		Version:      NDJSONCheckpointVersion,
		Name:         "in.ndjson",
		Offset:       10,
		Records:      2,
		Size:         20,
		HeadLen:      10,
		HeadHash:     1,
		BoundaryLen:  10,
		BoundaryHash: 2,
	}
	var buf bytes.Buffer
	if err := valid.Save(&buf); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := LoadCheckpoint(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("LoadCheckpoint: %v", err)
	}
	if got != valid {
		t.Fatalf("round trip mismatch: %+v != %+v", got, valid)
	}

	cases := []struct {
		name   string
		mutate func(*NDJSONCheckpoint)
	}{
		{"version", func(c *NDJSONCheckpoint) { c.Version = 99 }},
		{"missing name", func(c *NDJSONCheckpoint) { c.Name = "" }},
		{"negative offset", func(c *NDJSONCheckpoint) { c.Offset = -1 }},
		{"negative records", func(c *NDJSONCheckpoint) { c.Records = -1 }},
		{"size before offset", func(c *NDJSONCheckpoint) { c.Size = 5 }},
		{"boundary at zero", func(c *NDJSONCheckpoint) { c.Offset = 0 }},
		{"boundary length", func(c *NDJSONCheckpoint) { c.BoundaryLen = 9 }},
		{"head length", func(c *NDJSONCheckpoint) { c.HeadLen = 21 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := valid
			tc.mutate(&c)
			if err := c.Save(io.Discard); !errors.Is(err, ErrCheckpointCorrupt) {
				t.Fatalf("Save = %v, want ErrCheckpointCorrupt", err)
			}
		})
	}

	// Corrupt JSON must not be mistaken for a valid checkpoint.
	if _, err := LoadCheckpoint(strings.NewReader("{")); !errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("LoadCheckpoint = %v, want ErrCheckpointCorrupt", err)
	}
}

func TestNDJSONCheckpointSaveFileAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cp.json")

	cp := NDJSONCheckpoint{
		Version: NDJSONCheckpointVersion,
		Name:    "in.ndjson",
		Offset:  0,
		Size:    0,
	}
	if err := cp.SaveFile(path); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "cp.json" {
		t.Fatalf("unexpected directory entries: %v", entries)
	}
	if _, err := LoadCheckpointFile(path); err != nil {
		t.Fatal(err)
	}
}

func TestNDJSONReaderNextParsed(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	dir := t.TempDir()
	path := writeInput(t, dir, "in.ndjson", "{\"a\":1}\nnot json\n{\"c\":3}\n")

	r, err := OpenNDJSON(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	pj, err := r.NextParsed(nil)
	if err != nil {
		t.Fatal(err)
	}
	var a int64
	var elem Element
	if err := pj.ForEach(func(i Iter) error {
		obj, err := i.Object(nil)
		if err != nil {
			return err
		}
		if e := obj.FindKey("a", &elem); e != nil {
			a, _ = e.Iter.Int()
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if a != 1 {
		t.Fatalf("a = %d", a)
	}
	if err := r.Commit(); err != nil {
		t.Fatal(err)
	}

	// A parse failure leaves the line uncommitted so the caller can decide
	// what to do with it.
	if _, err := r.NextParsed(nil); err == nil {
		t.Fatal("expected parse error")
	}
	if cp := r.Checkpoint(); cp.Records != 1 {
		t.Fatalf("failed line advanced checkpoint: %+v", cp)
	}
	if err := r.Commit(); err != nil {
		t.Fatal(err)
	}
}

// TestNDJSONReaderExactlyOnce simulates a pipeline that appends each line to a
// durable output and only then commits, crashing after a few lines per run.
// The output must contain every record exactly once, in order.
func TestNDJSONReaderExactlyOnce(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 20; i++ {
		fmt.Fprintf(&b, "r%d\n", i)
	}
	dir := t.TempDir()
	path := writeInput(t, dir, "in.ndjson", b.String())

	var emitted []string
	var cp *NDJSONCheckpoint
	for runs := 0; ; runs++ {
		if runs > 100 {
			t.Fatal("did not reach the end of the input")
		}
		r, err := OpenNDJSON(path, cp)
		if err != nil {
			t.Fatal(err)
		}
		delivered := 0
		eof := false
		for delivered < 3 {
			line, err := r.Next()
			if err == io.EOF {
				eof = true
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			// Durable output write, then acknowledge.
			emitted = append(emitted, string(line))
			if err := r.Commit(); err != nil {
				t.Fatal(err)
			}
			delivered++
		}
		saved := r.Checkpoint()
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if eof {
			break
		}
		cp = &saved
	}

	want := make([]string, 20)
	for i := range want {
		want[i] = fmt.Sprintf("r%d", i)
	}
	if !reflect.DeepEqual(emitted, want) {
		t.Fatalf("got %q\nwant %q", emitted, want)
	}
}
