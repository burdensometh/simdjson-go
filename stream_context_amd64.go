//go:build !appengine && !noasm && gc
// +build !appengine,!noasm,gc

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
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"
)

// streamChunkSize is the amount of input read and parsed per batch.
const streamChunkSize = 10 << 20

// ParseNDStreamContext is the cancellable variant of ParseNDStream.
//
// It reads a newline delimited JSON stream and parses it in batches with a
// small pool of worker goroutines, delivering the parsed batches on res in
// input order. Unlike ParseNDStream it does not return immediately: it keeps
// reading and parsing until the stream ends or ctx is canceled, and then
// reports what was delivered.
//
// A read to the end behaves exactly like ParseNDStream: every batch is
// delivered in order, the last value on res has Error == io.EOF, res is then
// closed and the returned error is nil. The returned count is the number of
// JSON root records contained in those batches.
//
// If ctx is canceled, the stream is stopped: no further input is read, no
// further batch is handed to a worker, batches that were parsed but not yet
// delivered are dropped instead of delivered, res is closed without an io.EOF
// value, and the call returns ctx.Err() together with the number of records
// that had actually been delivered on res before the cancellation was
// observed. Batches that were queued internally but never delivered do not
// count as results.
//
// The count is in records rather than batches, because a single batch may hold
// many records. It covers exactly what the caller can observe on res: a batch
// that did reach res is counted in full even if the caller has not looked at
// it yet, while a batch that never reached res is not counted at all.
//
// reuse is optional. It is bidirectional so that batch objects lent to the
// parser for batches that will not be delivered can be handed back: every
// ParsedJson taken from reuse for a dropped batch is offered on reuse again
// before the call returns, so a caller that stops early keeps the batch
// objects it lent out. Handing a value back never blocks; if reuse has no room
// the object is released to the garbage collector after its input buffer has
// been recycled. Values in delivered batches are not returned here; the caller
// owns those, exactly as with ParseNDStream.
//
// Reading happens on its own goroutine. On a cancellation or a parse error the
// call stops issuing reads and does not wait for a Read already blocked in the
// underlying reader to return; that goroutine exits as soon as the Read does.
// The batch workers that were already started are always waited for, so no
// parse goroutine outlives the call.
func ParseNDStreamContext(ctx context.Context, r io.Reader, res chan<- Stream, reuse chan *ParsedJson) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		close(res)
		return 0, err
	}
	if !SupportedCPU() {
		err := fmt.Errorf("Host CPU does not meet target specs")
		select {
		case res <- Stream{Value: nil, Error: err}:
		case <-ctx.Done():
			err = ctx.Err()
		}
		close(res)
		return 0, err
	}

	// runCtx is canceled internally on shutdown so the reader stops even when
	// the caller did not cancel (normal end or a parse error).
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	buf := bufio.NewReaderSize(r, streamChunkSize)
	tmpPool := &sync.Pool{New: func() interface{} {
		return make([]byte, streamChunkSize+1024)
	}}
	conc := (runtime.GOMAXPROCS(0) + 1) / 2
	if conc < 1 {
		conc = 1
	}

	// dispatch carries the result channels of the batches in input order, so
	// results are delivered in the same order. Its capacity bounds the
	// read-ahead to conc batches, which also bounds the worker pool. The reader
	// is the only sender and closes it when done, always after enqueuing a
	// single terminal event (io.EOF or a read error) behind the batches read
	// before it.
	dispatch := make(chan chan Stream, conc)
	var workers streamWorkers

	go readStreamBatches(runCtx, buf, dispatch, reuse, tmpPool, &workers)

	// The forwarder runs on the caller's goroutine. It counts records only once
	// a batch has been handed to res.
	var (
		processed int64
		pending   chan Stream
	)
	for {
		if pending == nil {
			select {
			case <-ctx.Done():
				return stopStream(res, dispatch, &workers, cancelRun, reuse, tmpPool, nil, processed, ctx.Err())
			case ch, ok := <-dispatch:
				if !ok {
					// The reader closed dispatch. That happens either after it
					// queued a terminal event - which the loop already
					// delivered and returned on - or because it stopped on a
					// cancellation. Report the cancellation rather than a
					// clean end.
					workers.wait()
					close(res)
					if err := ctx.Err(); err != nil {
						return processed, err
					}
					return processed, nil
				}
				pending = ch
			}
		}

		var (
			ev Stream
			ok bool
		)
		select {
		case <-ctx.Done():
			return stopStream(res, dispatch, &workers, cancelRun, reuse, tmpPool, pending, processed, ctx.Err())
		case ev, ok = <-pending:
		}
		pending = nil
		if !ok {
			// The worker saw the cancellation before parsing and released its
			// batch without producing a value.
			continue
		}
		if ev.Error != nil {
			// A parse or read error ends the stream, as in ParseNDStream.
			// Delivering it is the last thing this call does; anything queued
			// behind it is dropped and reclaimed.
			if !deliverStream(ctx, res, ev) {
				return stopStream(res, dispatch, &workers, cancelRun, reuse, tmpPool, nil, processed, ctx.Err())
			}
			return stopStream(res, dispatch, &workers, cancelRun, reuse, tmpPool, nil, processed, terminalError(ev.Error))
		}
		// Count the records before handing the batch over: once it has been
		// delivered the caller may return it to reuse and a worker may start
		// to overwrite it, so the tape must not be read after the send.
		n := countStreamRecords(ev.Value)
		if !deliverStream(ctx, res, ev) {
			reclaimStreamValue(ev.Value, reuse, tmpPool)
			return stopStream(res, dispatch, &workers, cancelRun, reuse, tmpPool, nil, processed, ctx.Err())
		}
		processed += n
	}
}

// terminalError maps a delivered terminal event to the error the caller should
// receive: nil for a clean io.EOF, the error itself otherwise.
func terminalError(err error) error {
	if err == io.EOF {
		return nil
	}
	return err
}

// readStreamBatches is the read side. It fills dispatch with one result channel
// per batch, in input order, and closes dispatch when it stops. Ownership of
// each batch buffer passes to the worker; if a worker cannot be started the
// buffer is recycled here.
func readStreamBatches(ctx context.Context, buf *bufio.Reader, dispatch chan chan Stream, reuse chan *ParsedJson,
	tmpPool *sync.Pool, workers *streamWorkers) {
	defer close(dispatch)
	for {
		if ctx.Err() != nil {
			return
		}
		base := tmpPool.Get().([]byte)
		tmp := base[:streamChunkSize]
		n, err := buf.Read(tmp)
		if ctx.Err() != nil {
			tmpPool.Put(base)
			return
		}
		if err != nil && err != io.EOF {
			enqueueStreamEvent(ctx, dispatch, Stream{Error: err})
			tmpPool.Put(base)
			return
		}
		tmp = tmp[:n]
		// Read until a newline so a batch never ends in the middle of a record.
		if err != io.EOF {
			b, err2 := buf.ReadBytes('\n')
			if err2 != nil && err2 != io.EOF {
				enqueueStreamEvent(ctx, dispatch, Stream{Error: err2})
				tmpPool.Put(base)
				return
			}
			tmp = append(tmp, b...)
			if len(tmp) > 0 && &tmp[0] != &base[0] {
				// append grew into a new backing array; base is free again.
				tmpPool.Put(base)
			}
			// Forward io.EOF.
			err = err2
		}

		if len(tmp) > 0 {
			result := make(chan Stream, 1)
			select {
			case dispatch <- result:
			case <-ctx.Done():
				close(result)
				tmpPool.Put(tmp[:cap(tmp)])
				return
			}
			if !workers.start(func() {
				parseStreamChunk(ctx, tmp, result, reuse, tmpPool)
			}) {
				// The stream was shut down before this worker could start.
				close(result)
				tmpPool.Put(tmp[:cap(tmp)])
				return
			}
		} else {
			tmpPool.Put(base)
		}
		if err != nil {
			// Should only really be io.EOF.
			enqueueStreamEvent(ctx, dispatch, Stream{Error: err})
			return
		}
	}
}

// stopStream ends the read and parse goroutines, reclaims every batch that will
// never be delivered, closes res and returns the processed count with err.
// pending is a batch channel the forwarder had already taken off dispatch.
//
// The reader is not waited for: it may be blocked in Read, and a batch it
// enqueues after workers have stopped gets no worker, so it holds no borrowed
// object and the reader itself recycles its buffer.
func stopStream(res chan<- Stream, dispatch chan chan Stream, workers *streamWorkers, cancelRun context.CancelFunc,
	reuse chan *ParsedJson, tmpPool *sync.Pool, pending chan Stream, processed int64, err error) (int64, error) {
	cancelRun()
	// Stop starting workers and wait for the running ones, so every batch they
	// borrowed is already sitting in its buffered result channel.
	workers.stop()
	if pending != nil {
		// Non-blocking: a worker channel is buffered with at most one value and
		// a terminal-event channel is never closed, so ranging over it could
		// block forever.
		drainStreamEvents(pending, reuse, tmpPool)
	}
	// Reclaim what is buffered without blocking: a result channel still held by
	// the reader has no worker and therefore no borrowed object.
drainBuffered:
	for {
		var items chan Stream
		select {
		case ch, ok := <-dispatch:
			if !ok {
				break drainBuffered
			}
			items = ch
		default:
			break drainBuffered
		}
		drainStreamEvents(items, reuse, tmpPool)
	}
	close(res)
	return processed, err
}

// drainStreamEvents reclaims every event already buffered in items without
// blocking.
func drainStreamEvents(items chan Stream, reuse chan *ParsedJson, tmpPool *sync.Pool) {
	for {
		select {
		case ev, ok := <-items:
			if !ok {
				return
			}
			reclaimStreamValue(ev.Value, reuse, tmpPool)
		default:
			return
		}
	}
}

// streamWorkers tracks chunk parsing goroutines so a stopped stream can wait
// for them to return the batch objects they borrowed. Registration and the
// shutdown flag share a mutex so no goroutine is registered after stop starts.
type streamWorkers struct {
	mu      sync.Mutex
	stopped bool
	wg      sync.WaitGroup
}

// start runs fn on a new goroutine unless the stream has been stopped.
func (s *streamWorkers) start(fn func()) bool {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return false
	}
	s.wg.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.wg.Done()
		fn()
	}()
	return true
}

// stop prevents any further workers from being started and waits for the
// running ones to finish.
func (s *streamWorkers) stop() {
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()
	s.wg.Wait()
}

// wait waits for all started workers without preventing new ones. It must only
// be used once the reader - the only caller of start - has finished.
func (s *streamWorkers) wait() {
	s.wg.Wait()
}

// enqueueStreamEvent queues a terminal read event in input order. The result
// channel is buffered so the reader never blocks on the value; the queue send is
// abandoned if the context is canceled first.
func enqueueStreamEvent(ctx context.Context, dispatch chan chan Stream, ev Stream) {
	result := make(chan Stream, 1)
	result <- ev
	select {
	case dispatch <- result:
	case <-ctx.Done():
	}
}

// deliverStream sends ev on res unless ctx is canceled first. It reports whether
// the value was delivered.
func deliverStream(ctx context.Context, res chan<- Stream, ev Stream) bool {
	select {
	case res <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}

// parseStreamChunk parses one batch into exactly one result event. Ownership of
// data passes to this function: it either becomes the batch's Message or is
// recycled. The result channel is buffered, so a worker never blocks; when the
// context is canceled first the worker releases whatever it borrowed and closes
// the channel without sending.
func parseStreamChunk(ctx context.Context, data []byte, result chan Stream, reuse chan *ParsedJson, tmpPool *sync.Pool) {
	defer close(result)

	var pj internalParsedJson
	pj.copyStrings = true
	if reuse != nil {
		select {
		case v := <-reuse:
			if v != nil {
				if cap(v.Message) >= streamChunkSize+1024 {
					tmpPool.Put(v.Message)
					v.Message = nil
				}
				pj.ParsedJson = *v
			}
		default:
		}
	}

	if ctx.Err() != nil {
		// Canceled before parsing: hand the borrowed batch back untouched and
		// recycle the input buffer.
		releaseStreamValue(&pj.ParsedJson, reuse)
		tmpPool.Put(data[:cap(data)])
		return
	}

	parseErr := pj.parseMessage(data, true)
	if parseErr != nil {
		// The error event carries no value, so neither the input buffer nor
		// the borrowed batch is owned by anyone; reclaim both here.
		pj.ParsedJson.Message = nil
		tmpPool.Put(data[:cap(data)])
		releaseStreamValue(&pj.ParsedJson, reuse)
		result <- Stream{Value: nil, Error: fmt.Errorf("parsing input: %w", parseErr)}
		return
	}
	parsed := pj.ParsedJson
	result <- Stream{Value: &parsed}
}

// countStreamRecords counts the JSON root records held in pj.
func countStreamRecords(pj *ParsedJson) int64 {
	if pj == nil {
		return 0
	}
	it := pj.Iter()
	var n int64
	for it.Advance() == TypeRoot {
		n++
	}
	return n
}

// reclaimStreamValue hands a parsed batch that will not be delivered back to
// the caller through reuse when there is room, otherwise recycles only its
// input buffer. It never blocks.
func reclaimStreamValue(pj *ParsedJson, reuse chan *ParsedJson, tmpPool *sync.Pool) {
	if pj == nil {
		return
	}
	if reuse != nil {
		select {
		case reuse <- pj:
			return
		default:
		}
	}
	if cap(pj.Message) >= streamChunkSize+1024 {
		tmpPool.Put(pj.Message)
	}
	pj.Message = nil
}

// releaseStreamValue hands a borrowed batch that was never filled back to the
// caller. It never blocks and never touches an input buffer, since the borrower
// owns none for it.
func releaseStreamValue(pj *ParsedJson, reuse chan *ParsedJson) {
	if pj == nil || reuse == nil {
		return
	}
	select {
	case reuse <- pj:
	default:
	}
}
