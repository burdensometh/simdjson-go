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
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
)

const (
	// NDJSONCheckpointVersion is the version written to NDJSONCheckpoint.
	// Checkpoints with a different version are rejected.
	NDJSONCheckpointVersion = 1

	// ndjsonHeadSize is how many leading bytes are hashed to identify an input.
	ndjsonHeadSize = 64 << 10
	// ndjsonBoundarySize is how many bytes before the checkpoint offset are hashed
	// to make sure the consumed prefix did not change.
	ndjsonBoundarySize = 256
	// ndjsonDefaultBufferSize is the default buffered reader size.
	ndjsonDefaultBufferSize = 1 << 20
)

// Sentinel errors returned by the resumable NDJSON reader.
var (
	// ErrCheckpointCorrupt is returned when a checkpoint is missing required
	// fields or has an unsupported version.
	ErrCheckpointCorrupt = errors.New("simdjson: corrupt checkpoint")
	// ErrCheckpointMismatch is returned when a checkpoint was written for a
	// different input than the one being opened.
	ErrCheckpointMismatch = errors.New("simdjson: checkpoint does not belong to this input")
	// ErrCheckpointMidLine is returned when a checkpoint offset does not point
	// to the start of a line.
	ErrCheckpointMidLine = errors.New("simdjson: checkpoint offset is not on a line boundary")
	// ErrUnackedLine is returned when Next is called before the previously
	// returned line was committed with Commit.
	ErrUnackedLine = errors.New("simdjson: previous line was not committed")
	// ErrNoPendingLine is returned when Commit is called without a preceding
	// successful call to Next.
	ErrNoPendingLine = errors.New("simdjson: no line to commit")
	// ErrReaderClosed is returned when the reader is used after Close.
	ErrReaderClosed = errors.New("simdjson: reader is closed")
)

// NDJSONCheckpoint is a durable record of how far a newline delimited JSON
// input has been consumed.
//
// A checkpoint associates the identity of the input (its name and a hash of
// its leading bytes) with a byte offset that always lies on a complete-line
// boundary. Only lines passed to NDJSONReader.Commit are reflected in the
// offset, so callers can persist the checkpoint immediately after (or before)
// handing a line to their output sink without risking skipped records.
//
// Checkpoints are values and are safe to copy. They can be stored with Save or
// SaveFile and loaded with LoadCheckpoint or LoadCheckpointFile.
type NDJSONCheckpoint struct {
	// Version is the checkpoint format version. It must equal
	// NDJSONCheckpointVersion.
	Version int `json:"version"`
	// Name identifies the input, normally the path it was opened with.
	Name string `json:"name"`
	// Offset is the byte offset of the first unconsumed line. It is always
	// either 0 or the position directly after a line terminator.
	Offset int64 `json:"offset"`
	// Records is the number of complete records committed so far. It excludes
	// empty lines, which are skipped by the reader.
	Records int64 `json:"records"`
	// Size is the size of the input at the time the checkpoint was written.
	// It is used to detect truncation; the input may grow after the
	// checkpoint was written.
	Size int64 `json:"size"`
	// HeadLen and HeadHash identify the beginning of the input.
	HeadLen  int    `json:"head_len"`
	HeadHash uint64 `json:"head_hash"`
	// BoundaryLen and BoundaryHash identify the bytes immediately before
	// Offset. They are used to verify that the consumed prefix did not change.
	BoundaryLen  int    `json:"boundary_len"`
	BoundaryHash uint64 `json:"boundary_hash"`
	// Unterminated is set when the last committed record was the final line
	// of the file and was not terminated by a newline. If the input later
	// grows, the reader consumes the missing terminator before continuing.
	Unterminated bool `json:"unterminated,omitempty"`
}

// Save writes the checkpoint as a single JSON object.
func (c NDJSONCheckpoint) Save(w io.Writer) error {
	if err := c.validate(); err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	return enc.Encode(c)
}

// SaveFile writes the checkpoint to path atomically: the data is written to a
// temporary file in the same directory, synced, and renamed over path. A
// previous checkpoint at path is left intact on error.
func (c NDJSONCheckpoint) SaveFile(path string) error {
	if err := c.validate(); err != nil {
		return err
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("simdjson: creating checkpoint file: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() {
		tmp.Close()
		os.Remove(tmpName)
	}
	if err := json.NewEncoder(tmp).Encode(c); err != nil {
		cleanup()
		return fmt.Errorf("simdjson: writing checkpoint file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("simdjson: syncing checkpoint file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("simdjson: closing checkpoint file: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("simdjson: replacing checkpoint file: %w", err)
	}
	return nil
}

// LoadCheckpoint decodes a checkpoint written by Save.
func LoadCheckpoint(r io.Reader) (NDJSONCheckpoint, error) {
	var c NDJSONCheckpoint
	if err := json.NewDecoder(r).Decode(&c); err != nil {
		return c, fmt.Errorf("%w: %v", ErrCheckpointCorrupt, err)
	}
	if err := c.validate(); err != nil {
		return c, err
	}
	return c, nil
}

// LoadCheckpointFile loads a checkpoint previously written with SaveFile.
func LoadCheckpointFile(path string) (NDJSONCheckpoint, error) {
	f, err := os.Open(path)
	if err != nil {
		return NDJSONCheckpoint{}, err
	}
	defer f.Close()
	return LoadCheckpoint(f)
}

func (c NDJSONCheckpoint) validate() error {
	switch {
	case c.Version != NDJSONCheckpointVersion:
		return fmt.Errorf("%w: unsupported version %d", ErrCheckpointCorrupt, c.Version)
	case c.Name == "":
		return fmt.Errorf("%w: missing input name", ErrCheckpointCorrupt)
	case c.Offset < 0:
		return fmt.Errorf("%w: negative offset %d", ErrCheckpointCorrupt, c.Offset)
	case c.Records < 0:
		return fmt.Errorf("%w: negative record count %d", ErrCheckpointCorrupt, c.Records)
	case c.Size < c.Offset:
		return fmt.Errorf("%w: size %d is before offset %d", ErrCheckpointCorrupt, c.Size, c.Offset)
	case c.Offset == 0 && (c.BoundaryLen != 0 || c.BoundaryHash != 0):
		return fmt.Errorf("%w: boundary set at offset 0", ErrCheckpointCorrupt)
	case c.Offset > 0 && c.BoundaryLen != int(min64(c.Offset, ndjsonBoundarySize)):
		return fmt.Errorf("%w: boundary length %d does not match offset %d", ErrCheckpointCorrupt, c.BoundaryLen, c.Offset)
	case c.Unterminated && c.Offset == 0:
		return fmt.Errorf("%w: unterminated record at offset 0", ErrCheckpointCorrupt)
	case c.Size == 0 && (c.HeadLen != 0 || c.HeadHash != 0):
		return fmt.Errorf("%w: head set for empty input", ErrCheckpointCorrupt)
	case c.Size > 0 && (c.HeadLen <= 0 || int64(c.HeadLen) > min64(c.Size, ndjsonHeadSize)):
		return fmt.Errorf("%w: invalid head length %d for size %d", ErrCheckpointCorrupt, c.HeadLen, c.Size)
	}
	return nil
}

// NDJSONOption configures OpenNDJSON.
type NDJSONOption func(*ndjsonOptions) error

type ndjsonOptions struct {
	bufferSize int
}

// WithNDJSONBufferSize sets the size of the read buffer used by OpenNDJSON.
// The value must be positive. The default is 1 MiB.
func WithNDJSONBufferSize(n int) NDJSONOption {
	return func(o *ndjsonOptions) error {
		if n <= 0 {
			return fmt.Errorf("simdjson: invalid buffer size %d", n)
		}
		o.bufferSize = n
		return nil
	}
}

// NDJSONReader reads newline delimited JSON from a file with resumable,
// exactly-once semantics.
//
// Lines are returned one at a time by Next. The reader does not advance its
// checkpoint until Commit is called, so callers must commit a line only after
// it has been durably handed to their output. If the process stops before
// Commit, the line is returned again after reopening; if it stops after
// Commit, it is not returned again.
//
// The reader is not safe for concurrent use.
type NDJSONReader struct {
	f                   *os.File
	path                string
	br                  *bufio.Reader
	cp                  NDJSONCheckpoint
	size                int64 // input size when the file was opened
	scanOff             int64 // physical offset of the next unread byte
	tail                tailRing
	closed              bool
	pending             bool
	pendingEnd          int64
	pendingUnterminated bool
}

// OpenNDJSON opens path for resumable NDJSON reading.
//
// If cp is nil, reading starts at the beginning of the file. Otherwise the
// checkpoint is validated against the file: the name must match, the file must
// not be shorter than the checkpoint requires, the hashed head and boundary
// must be unchanged, and the offset must lie on a line boundary. A mismatch
// returns an error wrapping ErrCheckpointMismatch; an offset in the middle of a
// line returns an error wrapping ErrCheckpointMidLine.
//
// A checkpoint for an unterminated final line is valid. If the file has since
// been terminated with a newline, that terminator is consumed during Open so
// the already-delivered record is not read again.
func OpenNDJSON(path string, cp *NDJSONCheckpoint, opts ...NDJSONOption) (*NDJSONReader, error) {
	cfg := ndjsonOptions{bufferSize: ndjsonDefaultBufferSize}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*NDJSONReader, error) {
		f.Close()
		return nil, err
	}

	st, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	if !st.Mode().IsRegular() {
		return fail(fmt.Errorf("simdjson: %q is not a regular file", path))
	}
	size := st.Size()

	var cur NDJSONCheckpoint
	if cp != nil {
		cur = *cp
		if err := cur.validate(); err != nil {
			return fail(err)
		}
		if cur.Name != path {
			return fail(fmt.Errorf("%w: checkpoint names %q, opened %q", ErrCheckpointMismatch, cur.Name, path))
		}
		if need := max64(cur.Size, cur.Offset); size < need {
			return fail(fmt.Errorf("%w: input is %d bytes, checkpoint requires at least %d", ErrCheckpointMismatch, size, need))
		}
		if err := verifyHead(f, cur); err != nil {
			return fail(err)
		}
		if cur.HeadLen == 0 && size > 0 {
			// The checkpoint was written for an empty input which has since
			// grown. Adopt the new prefix as the input identity and re-anchor
			// the checkpoint size.
			cur.HeadLen = int(min64(size, ndjsonHeadSize))
			h, err := hashFileAt(f, 0, cur.HeadLen)
			if err != nil {
				return fail(err)
			}
			cur.HeadHash = h
			cur.Size = size
		}
		if err := verifyBoundary(f, cur); err != nil {
			return fail(err)
		}
		if cur.Offset > 0 {
			if cur.Unterminated {
				if cur.Offset < size {
					skip, err := terminatorLenAt(f, cur.Offset, size)
					if err != nil {
						return fail(err)
					}
					if skip == 0 {
						return fail(fmt.Errorf("%w: offset %d is followed by more data but no line terminator", ErrCheckpointMidLine, cur.Offset))
					}
					cur.Offset += int64(skip)
					cur.Unterminated = false
					cur.Size = size
					if err := refreshBoundary(f, &cur); err != nil {
						return fail(err)
					}
				}
			} else {
				last, err := readByteAt(f, cur.Offset-1)
				if err != nil {
					return fail(err)
				}
				if last != '\n' {
					return fail(fmt.Errorf("%w: byte before offset %d is %q, not a newline", ErrCheckpointMidLine, cur.Offset, last))
				}
			}
		}
	} else {
		cur = NDJSONCheckpoint{
			Version: NDJSONCheckpointVersion,
			Name:    path,
			Size:    size,
		}
		if size > 0 {
			cur.HeadLen = int(min64(size, ndjsonHeadSize))
			h, err := hashFileAt(f, 0, cur.HeadLen)
			if err != nil {
				return fail(err)
			}
			cur.HeadHash = h
		}
	}

	r := &NDJSONReader{
		f:    f,
		path: path,
		cp:   cur,
		size: size,
	}
	if err := r.resetTail(); err != nil {
		return fail(err)
	}
	if _, err := f.Seek(cur.Offset, io.SeekStart); err != nil {
		return fail(err)
	}
	r.br = bufio.NewReaderSize(f, cfg.bufferSize)
	r.scanOff = cur.Offset
	return r, nil
}

// Checkpoint returns the current durable progress. It reflects only lines that
// have been committed; a line returned by Next but not yet committed is not
// included, so it will be returned again after reopening.
func (r *NDJSONReader) Checkpoint() NDJSONCheckpoint {
	return r.cp
}

// SaveCheckpoint writes the current checkpoint to w.
func (r *NDJSONReader) SaveCheckpoint(w io.Writer) error {
	return r.cp.Save(w)
}

// SaveCheckpointFile writes the current checkpoint to path atomically.
func (r *NDJSONReader) SaveCheckpointFile(path string) error {
	return r.cp.SaveFile(path)
}

// Next returns the next non-empty line with its line terminator removed.
// A trailing carriage return is removed as well, so CRLF inputs yield the same
// lines as LF inputs.
//
// The returned slice is valid until the next call to Next. Empty lines are
// skipped. Next returns io.EOF once the input is exhausted.
//
// A line must be committed with Commit before Next may be called again. If it
// is not, Next returns an error wrapping ErrUnackedLine.
func (r *NDJSONReader) Next() ([]byte, error) {
	if r.closed {
		return nil, ErrReaderClosed
	}
	if r.pending {
		return nil, fmt.Errorf("%w: call Commit before reading the next line", ErrUnackedLine)
	}
	if r.cp.Unterminated {
		// The last committed record was the final line and had no terminator.
		// If the input grew since the commit, the missing terminator must come
		// first; otherwise the record's end is no longer a line boundary.
		if err := r.consumeDeferredTerminator(); err != nil {
			return nil, err
		}
	}
	for {
		raw, err := r.br.ReadBytes('\n')
		if len(raw) > 0 {
			r.scanOff += int64(len(raw))
			r.tail.write(raw)
		}
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("simdjson: reading %q: %w", r.path, err)
		}
		if len(raw) == 0 {
			return nil, io.EOF
		}
		terminated := raw[len(raw)-1] == '\n'
		line := raw
		if terminated {
			line = line[:len(line)-1]
		}
		if n := len(line); n > 0 && line[n-1] == '\r' {
			line = line[:n-1]
		}
		if isBlankLine(line) {
			if err == io.EOF {
				return nil, io.EOF
			}
			continue
		}
		r.pending = true
		r.pendingEnd = r.scanOff
		r.pendingUnterminated = !terminated
		return line, nil
	}
}

// Commit acknowledges that the line most recently returned by Next has been
// durably handed to the output. Only then does the checkpoint advance.
//
// Committing a final line that has no newline terminator is allowed: the
// checkpoint records that the last record was unterminated. If the input later
// grows, OpenNDJSON consumes the missing terminator instead of returning the
// record again. If the appended data does not start with a terminator, the
// checkpoint can no longer describe a line boundary and reopening fails with
// ErrCheckpointMidLine.
func (r *NDJSONReader) Commit() error {
	if r.closed {
		return ErrReaderClosed
	}
	if !r.pending {
		return ErrNoPendingLine
	}
	r.cp.Offset = r.pendingEnd
	r.cp.Records++
	r.cp.Unterminated = r.pendingUnterminated
	r.cp.Size = max64(r.size, r.cp.Offset)
	if r.cp.HeadLen == 0 {
		// The reader was opened on an empty input that has since grown. Adopt
		// the bytes read so far as the input identity.
		size := r.cp.Offset
		if st, err := r.f.Stat(); err == nil {
			size = max64(size, st.Size())
		}
		n := int(min64(size, ndjsonHeadSize))
		if n > 0 {
			h, err := hashFileAt(r.f, 0, n)
			if err != nil {
				return err
			}
			r.cp.HeadLen = n
			r.cp.HeadHash = h
			r.cp.Size = max64(r.cp.Size, int64(n))
		}
	}
	n := int(min64(r.cp.Offset, ndjsonBoundarySize))
	r.cp.BoundaryLen = n
	if n == 0 {
		r.cp.BoundaryHash = 0
	} else {
		r.cp.BoundaryHash = r.tail.hashLast(n)
	}
	r.pending = false
	return nil
}

// consumeDeferredTerminator handles a checkpoint whose last committed record
// was unterminated. If the input has since grown, the already-delivered record
// is only a complete line when the new data starts with a terminator; that
// terminator is consumed so the record is not emitted twice. If the new data
// starts mid-record, the checkpoint no longer describes a line boundary and
// ErrCheckpointMidLine is returned.
func (r *NDJSONReader) consumeDeferredTerminator() error {
	if !r.cp.Unterminated {
		return nil
	}
	ok, err := r.br.Peek(1)
	if err != nil && err != io.EOF {
		return fmt.Errorf("simdjson: reading %q: %w", r.path, err)
	}
	if err == io.EOF || len(ok) == 0 {
		// Still at the end of the input.
		return nil
	}
	var skip int
	switch ok[0] {
	case '\n':
		skip = 1
	case '\r':
		b, err := r.br.Peek(2)
		if err != nil && err != io.EOF {
			return fmt.Errorf("simdjson: reading %q: %w", r.path, err)
		}
		if len(b) < 2 {
			return fmt.Errorf("%w: offset %d is followed by %q without a newline", ErrCheckpointMidLine, r.cp.Offset, b)
		}
		if b[1] != '\n' {
			return fmt.Errorf("%w: offset %d is followed by %q", ErrCheckpointMidLine, r.cp.Offset, b[:2])
		}
		skip = 2
	default:
		return fmt.Errorf("%w: offset %d is followed by %q", ErrCheckpointMidLine, r.cp.Offset, ok)
	}
	term := make([]byte, skip)
	if _, err := io.ReadFull(r.br, term); err != nil {
		return fmt.Errorf("simdjson: reading %q: %w", r.path, err)
	}
	r.scanOff += int64(skip)
	r.tail.write(term)
	r.cp.Offset += int64(skip)
	r.cp.Unterminated = false
	r.cp.Size = max64(r.cp.Size, r.scanOff)
	n := int(min64(r.cp.Offset, ndjsonBoundarySize))
	r.cp.BoundaryLen = n
	r.cp.BoundaryHash = r.tail.hashLast(n)
	return nil
}

// NextParsed returns the next line parsed as a single JSON value. It uses Parse
// internally and therefore requires a supported CPU. Strings are copied by
// default; see WithCopyStrings.
//
// If parsing fails, the line remains uncommitted. The caller can either Commit
// it after handling the bad record (for example by writing it to a dead letter
// queue) or retry parsing after correcting the input.
func (r *NDJSONReader) NextParsed(reuse *ParsedJson, opts ...ParserOption) (*ParsedJson, error) {
	line, err := r.Next()
	if err != nil {
		return nil, err
	}
	pj, err := Parse(bytes.TrimSpace(line), reuse, opts...)
	if err != nil {
		return nil, fmt.Errorf("simdjson: parsing line ending at offset %d: %w", r.pendingEnd, err)
	}
	return pj, nil
}

// Close closes the underlying file. Any line returned by Next but not
// committed is discarded; the checkpoint remains at the previous line.
func (r *NDJSONReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return r.f.Close()
}

func (r *NDJSONReader) resetTail() error {
	r.tail = tailRing{}
	if r.cp.Offset == 0 {
		return nil
	}
	n := int(min64(r.cp.Offset, ndjsonBoundarySize))
	buf := make([]byte, n)
	if _, err := r.f.ReadAt(buf, r.cp.Offset-int64(n)); err != nil {
		return fmt.Errorf("simdjson: reading boundary: %w", err)
	}
	r.tail.write(buf)
	return nil
}

func verifyHead(f *os.File, c NDJSONCheckpoint) error {
	if c.HeadLen == 0 {
		return nil
	}
	h, err := hashFileAt(f, 0, c.HeadLen)
	if err != nil {
		return err
	}
	if h != c.HeadHash {
		return fmt.Errorf("%w: first %d bytes differ", ErrCheckpointMismatch, c.HeadLen)
	}
	return nil
}

func verifyBoundary(f *os.File, c NDJSONCheckpoint) error {
	if c.Offset == 0 {
		return nil
	}
	n := int(min64(c.Offset, ndjsonBoundarySize))
	h, err := hashFileAt(f, c.Offset-int64(n), n)
	if err != nil {
		return err
	}
	if h != c.BoundaryHash {
		return fmt.Errorf("%w: %d bytes before offset %d differ", ErrCheckpointMismatch, n, c.Offset)
	}
	return nil
}

func refreshBoundary(f *os.File, c *NDJSONCheckpoint) error {
	n := int(min64(c.Offset, ndjsonBoundarySize))
	c.BoundaryLen = n
	if n == 0 {
		c.BoundaryHash = 0
		return nil
	}
	h, err := hashFileAt(f, c.Offset-int64(n), n)
	if err != nil {
		return err
	}
	c.BoundaryHash = h
	return nil
}

// terminatorLenAt returns the length of the line terminator at off, or 0 if
// the bytes there are not a complete terminator.
func terminatorLenAt(f *os.File, off, size int64) (int, error) {
	b, err := readByteAt(f, off)
	if err != nil {
		return 0, err
	}
	switch b {
	case '\n':
		return 1, nil
	case '\r':
		if off+1 < size {
			b2, err := readByteAt(f, off+1)
			if err != nil {
				return 0, err
			}
			if b2 == '\n' {
				return 2, nil
			}
		}
	}
	return 0, nil
}

func hashFileAt(f *os.File, off int64, n int) (uint64, error) {
	if n == 0 {
		return 0, nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(io.NewSectionReader(f, off, int64(n)), buf); err != nil {
		return 0, fmt.Errorf("simdjson: reading input at offset %d: %w", off, err)
	}
	return hashBytes(buf), nil
}

func readByteAt(f *os.File, off int64) (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(io.NewSectionReader(f, off, 1), b[:]); err != nil {
		return 0, fmt.Errorf("simdjson: reading input at offset %d: %w", off, err)
	}
	return b[0], nil
}

func hashBytes(b []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64()
}

func isBlankLine(line []byte) bool {
	for _, c := range line {
		switch c {
		case ' ', '\t', '\r':
		default:
			return false
		}
	}
	return true
}

// tailRing keeps the last len(buf) bytes written to it so the checkpoint can
// fingerprint the consumed prefix without rereading the file.
type tailRing struct {
	buf [ndjsonBoundarySize]byte
	n   int
	pos int
}

func (t *tailRing) write(p []byte) {
	if len(p) >= len(t.buf) {
		copy(t.buf[:], p[len(p)-len(t.buf):])
		t.n = len(t.buf)
		t.pos = 0
		return
	}
	for _, b := range p {
		t.buf[t.pos] = b
		t.pos++
		if t.pos == len(t.buf) {
			t.pos = 0
		}
		if t.n < len(t.buf) {
			t.n++
		}
	}
}

func (t *tailRing) hashLast(n int) uint64 {
	if n > t.n {
		n = t.n
	}
	if n == 0 {
		return 0
	}
	h := fnv.New64a()
	start := (t.pos - n + len(t.buf)) % len(t.buf)
	if start+n <= len(t.buf) {
		_, _ = h.Write(t.buf[start : start+n])
	} else {
		_, _ = h.Write(t.buf[start:])
		_, _ = h.Write(t.buf[:n-(len(t.buf)-start)])
	}
	return h.Sum64()
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
