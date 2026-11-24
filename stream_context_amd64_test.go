//go:build !noasm && !appengine && gc
// +build !noasm,!appengine,gc

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
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// chunkReader hands out its data in pieces of at most size bytes per Read, so a
// single underlying Read produces a small batch instead of the whole input.
type chunkReader struct {
	data []byte
	off  int
	size int

	mu    sync.Mutex
	reads int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	r.reads++
	r.mu.Unlock()
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	n := r.size
	if n > len(p) {
		n = len(p)
	}
	if r.off+n > len(r.data) {
		n = len(r.data) - r.off
	}
	copy(p, r.data[r.off:r.off+n])
	r.off += n
	return n, nil
}

func (r *chunkReader) ReadCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reads
}

// streamRecordCount returns the number of JSON root records held in Value.
func streamRecordCount(t *testing.T, pj *ParsedJson) int64 {
	t.Helper()
	it := pj.Iter()
	var n int64
	for it.Advance() == TypeRoot {
		n++
	}
	return n
}

// buildNDJSON builds n single-record NDJSON lines with an "id" field.
func buildNDJSON(n int) []byte {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, `{"id":%d}`+"\n", i)
	}
	return []byte(sb.String())
}

func TestParseNDStreamContextComplete(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	const records = 5000
	// A small chunk size forces many batches; the records must still arrive in
	// order and the count must match.
	r := &chunkReader{data: buildNDJSON(records), size: 4096}
	res := make(chan Stream, 1<<12)

	count, err := ParseNDStreamContext(context.Background(), r, res, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != records {
		t.Fatalf("processed = %d, want %d", count, records)
	}

	var (
		got      int64
		nextWant int64
		sawEOF   bool
		batches  int
	)
	for ev := range res {
		if ev.Error != nil {
			if ev.Error != io.EOF {
				t.Fatalf("unexpected terminal error: %v", ev.Error)
			}
			sawEOF = true
			continue
		}
		batches++
		err := ev.Value.ForEach(func(i Iter) error {
			var elem Element
			obj, err := i.Object(nil)
			if err != nil {
				return err
			}
			if e := obj.FindKey("id", &elem); e == nil {
				return errors.New("missing id")
			}
			id, err := elem.Iter.Int()
			if err != nil {
				return err
			}
			if id != nextWant {
				return fmt.Errorf("record %d out of order: got id %d, want %d", got, id, nextWant)
			}
			nextWant++
			got++
			return nil
		})
		if err != nil {
			t.Fatalf("batch: %v", err)
		}
	}
	if !sawEOF {
		t.Fatal("stream did not end with io.EOF")
	}
	if got != records {
		t.Fatalf("delivered %d records, want %d", got, records)
	}
	if batches < 2 {
		t.Fatalf("expected multiple batches, got %d", batches)
	}
}

func TestParseNDStreamContextEmptyInput(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	res := make(chan Stream, 4)
	count, err := ParseNDStreamContext(context.Background(), strings.NewReader(""), res, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Fatalf("processed = %d, want 0", count)
	}
	ev, ok := <-res
	if !ok || ev.Error != io.EOF {
		t.Fatalf("first event = %+v (open=%v), want io.EOF", ev, ok)
	}
	if _, ok := <-res; ok {
		t.Fatal("channel not closed after io.EOF")
	}
}

func TestParseNDStreamContextCancelCount(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	const records = 20000
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := &chunkReader{data: buildNDJSON(records), size: 2048}
	res := make(chan Stream, 1<<12)

	type result struct {
		count int64
		err   error
	}
	done := make(chan result, 1)
	go func() {
		count, err := ParseNDStreamContext(ctx, r, res, nil)
		done <- result{count, err}
	}()

	// Read a few records, then cancel, without draining to completion.
	var (
		delivered int64
		canceled  bool
	)
	for ev := range res {
		if ev.Error != nil {
			if ev.Error != io.EOF {
				t.Fatalf("unexpected terminal error: %v", ev.Error)
			}
			t.Fatal("cancelled stream must not end with io.EOF")
		}
		delivered += streamRecordCount(t, ev.Value)
		if !canceled && delivered >= 100 {
			cancel()
			canceled = true
		}
	}
	if !canceled {
		t.Fatal("never reached the cancellation threshold")
	}

	got := <-done
	if !errors.Is(got.err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", got.err)
	}
	if got.count != delivered {
		t.Fatalf("returned count = %d, but %d records were delivered on res", got.count, delivered)
	}
	if got.count == 0 || got.count >= records {
		t.Fatalf("expected a partial count, got %d of %d", got.count, records)
	}
}

func TestParseNDStreamContextCancelBeforeStart(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res := make(chan Stream, 4)
	count, err := ParseNDStreamContext(ctx, strings.NewReader(string(buildNDJSON(10))), res, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if count != 0 {
		t.Fatalf("processed = %d, want 0", count)
	}
	if _, ok := <-res; ok {
		t.Fatal("channel not closed")
	}
}

// TestParseNDStreamContextReclaimsReuse checks that every ParsedJson lent to a
// cancelled stream comes back. With an unbuffered result channel and no reader,
// no batch is ever delivered, so the number of objects on the reuse channel
// must be exactly what it started with.
func TestParseNDStreamContextReclaimsReuse(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	const lent = 8
	reuse := make(chan *ParsedJson, 1<<10)
	for i := 0; i < lent; i++ {
		reuse <- &ParsedJson{}
	}

	ctx, cancel := context.WithCancel(context.Background())
	r := &chunkReader{data: buildNDJSON(50000), size: 1024}
	res := make(chan Stream) // unbuffered: nothing can be delivered

	done := make(chan error, 1)
	go func() {
		_, err := ParseNDStreamContext(ctx, r, res, reuse)
		done <- err
	}()

	// Give the pipeline a moment to borrow batches, then stop it.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled stream did not return")
	}
	// Drain result events (there should be none as values; closing is enough).
	for range res {
	}

	// Every lent object is back: the ones never borrowed plus one returned for
	// every borrowed-and-dropped batch.
	var have int
	for {
		select {
		case <-reuse:
			have++
		default:
			if have != lent {
				t.Fatalf("reuse holds %d objects after cancellation, want %d", have, lent)
			}
			return
		}
	}
}

func TestParseNDStreamContextReaderError(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	// More than one chunk so at least one batch completes before the failing
	// Read; the records read successfully before the failure must be delivered,
	// then the reader error, exactly as ParseNDStream reports it.
	var sb strings.Builder
	pad := strings.Repeat("x", 1024)
	for i := 0; len(sb.String()) < streamChunkSize+2<<20; i++ {
		fmt.Fprintf(&sb, `{"id":%d,"pad":"%s"}`+"\n", i, pad)
	}
	boom := errors.New("simulated reader failure")
	r := &errAfterReader{data: []byte(sb.String()), err: boom}
	res := make(chan Stream, 1<<12)

	count, err := ParseNDStreamContext(context.Background(), r, res, nil)
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want %v", err, boom)
	}
	if count == 0 {
		t.Fatal("no record survived the reader error")
	}
	var delivered int64
	var termErr error
	for ev := range res {
		if ev.Error != nil {
			termErr = ev.Error
			continue
		}
		delivered += streamRecordCount(t, ev.Value)
	}
	if delivered != count {
		t.Fatalf("returned count = %d, but %d records were delivered", count, delivered)
	}
	if !errors.Is(termErr, boom) {
		t.Fatalf("terminal event = %v, want %v", termErr, boom)
	}
	if _, ok := <-res; ok {
		t.Fatal("channel not closed")
	}
}

// errAfterReader returns all its data, then the configured error.
type errAfterReader struct {
	data []byte
	off  int
	err  error
}

func (r *errAfterReader) Read(p []byte) (int, error) {
	if r.off >= len(r.data) {
		return 0, r.err
	}
	n := copy(p, r.data[r.off:])
	r.off += n
	return n, nil
}

func TestParseNDStreamContextParseError(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	input := []byte("{\"id\":1}\n{\"broken\":\n")
	res := make(chan Stream, 64)
	count, err := ParseNDStreamContext(context.Background(), bytes.NewReader(input), res, nil)
	if err == nil {
		t.Fatal("expected a parse error")
	}
	if count != 0 {
		t.Fatalf("processed = %d, want 0 for a failed batch", count)
	}
	var gotErr error
	for ev := range res {
		if ev.Error != nil {
			gotErr = ev.Error
			continue
		}
		t.Fatalf("unexpected value before parse error")
	}
	if gotErr == nil || !strings.Contains(gotErr.Error(), "parsing input") {
		t.Fatalf("terminal error = %v, want a parsing input error", gotErr)
	}
}

// TestParseNDStreamContextCancelStopsReads checks that no new read is issued
// after cancellation: the reader blocks after its first Read and the call still
// returns promptly.
func TestParseNDStreamContextCancelStopsReads(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	blocked := make(chan struct{})
	r := &blockingReader{
		first:   buildNDJSON(100),
		gate:    blocked,
		started: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan Stream, 1<<10)

	done := make(chan error, 1)
	go func() {
		_, err := ParseNDStreamContext(ctx, r, res, nil)
		done <- err
	}()

	// Wait until the reader is blocked in its second Read, then cancel.
	<-r.started
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("call did not return after cancel while Read was blocked")
	}
	close(blocked)
	for range res {
	}
}

// blockingReader returns first on the first Read, then blocks on gate until it
// is closed, and reports io.EOF afterwards.
type blockingReader struct {
	first   []byte
	gate    chan struct{}
	started chan struct{}
	once    sync.Once
	reads   int
}

func (r *blockingReader) Read(p []byte) (int, error) {
	r.reads++
	if r.reads == 1 {
		r.once.Do(func() { close(r.started) })
		n := copy(p, r.first)
		return n, nil
	}
	<-r.gate
	return 0, io.EOF
}

// TestReclaimStreamValue gives a dropped batch back to reuse and never blocks
// when reuse is full.
func TestReclaimStreamValue(t *testing.T) {
	var pool sync.Pool
	pool.New = func() interface{} { return make([]byte, streamChunkSize+1024) }

	reuse := make(chan *ParsedJson, 1)
	pj := &ParsedJson{Message: make([]byte, streamChunkSize+1024)}
	reclaimStreamValue(pj, reuse, &pool)
	select {
	case got := <-reuse:
		if got != pj {
			t.Fatal("wrong object returned to reuse")
		}
	default:
		t.Fatal("object was not returned to reuse")
	}

	// Full reuse must not block and must still release the input buffer.
	reuse <- &ParsedJson{}
	pj2 := &ParsedJson{Message: make([]byte, streamChunkSize+1024)}
	reclaimStreamValue(pj2, reuse, &pool)
	if len(reuse) != 1 {
		t.Fatalf("reuse length = %d, want 1", len(reuse))
	}
	if pj2.Message != nil {
		t.Fatal("Message should be cleared when reuse is full")
	}
}
