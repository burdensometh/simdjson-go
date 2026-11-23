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
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/klauspost/cpuid/v2"
)

var wantFeatures = cpuid.CombineFeatures(cpuid.AVX2, cpuid.CLMUL)

// SupportedCPU will return whether the CPU is supported.
func SupportedCPU() bool {
	return cpuid.CPU.HasAll(wantFeatures)
}

// streamChunkSize is the size of the temporary read chunks, matched by
// the pool below. A little headroom is kept for the bytes appended by
// ReadBytes while completing the final newline of a chunk.
const streamChunkSize = 10 << 20

var streamBufPool = sync.Pool{New: func() interface{} {
	return make([]byte, streamChunkSize+1024)
}}

func newInternalParsedJson(reuse *ParsedJson, opts []ParserOption) (*internalParsedJson, error) {
	if !SupportedCPU() {
		return nil, errors.New("Host CPU does not meet target specs")
	}
	var pj *internalParsedJson
	if reuse != nil && reuse.internal != nil {
		pj = reuse.internal
		pj.ParsedJson = *reuse
		pj.ParsedJson.internal = nil
		reuse = &ParsedJson{}
	}
	if pj == nil {
		pj = &internalParsedJson{}
	}
	pj.copyStrings = true
	for _, opt := range opts {
		if err := opt(pj); err != nil {
			return nil, err
		}
	}
	return pj, nil
}

// Parse an object or array from a block of data and return the parsed JSON.
// An optional block of previously parsed json can be supplied to reduce allocations.
func Parse(b []byte, reuse *ParsedJson, opts ...ParserOption) (*ParsedJson, error) {
	pj, err := newInternalParsedJson(reuse, opts)
	if err != nil {
		return nil, err
	}
	err = pj.parseMessage(b, false)
	if err != nil {
		return nil, err
	}
	parsed := &pj.ParsedJson
	parsed.internal = pj
	return parsed, nil
}

// ParseND will parse newline delimited JSON objects or arrays.
// An optional block of previously parsed json can be supplied to reduce allocations.
func ParseND(b []byte, reuse *ParsedJson, opts ...ParserOption) (*ParsedJson, error) {
	pj, err := newInternalParsedJson(reuse, opts)
	if err != nil {
		return nil, err
	}
	err = pj.parseMessage(bytes.TrimSpace(b), true)
	if err != nil {
		return nil, err
	}
	return &pj.ParsedJson, nil
}

// A Stream is used to stream back results.
// Either Error or Value will be set on returned results.
type Stream struct {
	Value *ParsedJson
	Error error
}

// ParseNDStream will parse a stream and return parsed JSON to the supplied result channel.
// The method will return immediately.
// Each element is contained within a root tag.
//
//	<root>Element 1</root><root>Element 2</root>...
//
// Each result will contain an unspecified number of full elements,
// so it can be assumed that each result starts and ends with a root tag.
// The parser will keep parsing until writes to the result stream blocks.
// A stream is finished when a non-nil Error is returned.
// If the stream was parsed until the end the Error value will be io.EOF
// The channel will be closed after an error has been returned.
// An optional channel for returning consumed results can be provided.
// There is no guarantee that elements will be consumed, so always use
// non-blocking writes to the reuse channel.
func ParseNDStream(r io.Reader, res chan<- Stream, reuse <-chan *ParsedJson) {
	parseNDStream(r, res, nil, reuse)
}

// ParseNDStreamTolerant is the fault-tolerant variant of ParseNDStream.
//
// Unlike ParseNDStream, a single line that cannot be parsed does not
// terminate the stream. When a read chunk contains malformed JSON, the
// chunk is re-parsed one line at a time: every line that parses is
// emitted to res exactly as it would be in the non-tolerant stream,
// while every line that fails is delivered to bad as a BadLine carrying
// the original raw line, its 1-based line number within the whole input
// and the parse error. Chunks that parse cleanly take the same fast
// bulk path used by ParseNDStream.
//
// Ordering is preserved: valid results and quarantine reports from one
// chunk are all forwarded before any event from a later chunk, and the
// line numbers allow the original stream to be reconstructed.
//
// A failure of the underlying reader (anything other than io.EOF)
// remains fatal: the chunks already parsed successfully are forwarded
// first, then the reader error is sent to res and both channels are
// closed. A successful completion sends io.EOF to res before closing.
//
// bad must not be nil; writes to it block like writes to res, so both
// channels have to be drained until they are closed. If quarantine is
// not required, use ParseNDStream instead, which keeps the strict
// "first parse error terminates the stream" behavior.
func ParseNDStreamTolerant(r io.Reader, res chan<- Stream, bad chan<- BadLine, reuse <-chan *ParsedJson) {
	if bad == nil {
		panic("simdjson: ParseNDStreamTolerant requires a non-nil quarantine channel")
	}
	parseNDStream(r, res, bad, reuse)
}

// parseNDStream contains the shared streaming machinery. A non-nil bad
// channel selects the tolerant fault mode.
func parseNDStream(r io.Reader, res chan<- Stream, bad chan<- BadLine, reuse <-chan *ParsedJson) {
	if !SupportedCPU() {
		go func() {
			res <- Stream{
				Value: nil,
				Error: fmt.Errorf("Host CPU does not meet target specs"),
			}
			close(res)
			if bad != nil {
				close(bad)
			}
		}()
		return
	}
	buf := bufio.NewReaderSize(r, streamChunkSize)
	conc := (runtime.GOMAXPROCS(0) + 1) / 2
	queue := make(chan chan streamEvent, conc)
	go func() {
		// Forward finished items in order.
		defer close(res)
		if bad != nil {
			defer close(bad)
		}
		end := false
		for items := range queue {
			for ev := range items {
				if ev.bad != nil {
					// Quarantined line. Backpressure on bad applies
					// to the whole stream, keeping a slow consumer
					// from accumulating unbounded memory.
					bad <- *ev.bad
					continue
				}
				i := ev.Stream
				select {
				case res <- i:
				default:
					if !end {
						// Block if we haven't returned an error
						res <- i
					}
				}
				if i.Error != nil {
					end = true
				}
			}
		}
	}()
	go func() {
		defer close(queue)
		// lineNo is the 1-based number of the first line in the
		// chunk about to be read, kept continuous across chunks.
		var lineNo int64 = 1
		for {
			tmp := streamBufPool.Get().([]byte)
			tmp = tmp[:streamChunkSize]
			n, err := buf.Read(tmp)
			if err != nil && err != io.EOF {
				queueEvent(queue, streamEvent{Stream: Stream{Error: err}})
				streamBufPool.Put(tmp[:cap(tmp)])
				return
			}
			tmp = tmp[:n]
			// Read until Newline
			if err != io.EOF {
				b, err2 := buf.ReadBytes('\n')
				if err2 != nil && err2 != io.EOF {
					queueEvent(queue, streamEvent{Stream: Stream{Error: err2}})
					streamBufPool.Put(tmp[:cap(tmp)])
					return
				}
				tmp = append(tmp, b...)
				// Forward io.EOF
				err = err2
			}

			if len(tmp) > 0 {
				result := make(chan streamEvent, 1)
				queue <- result
				go func(chunk []byte, firstLine int64) {
					defer close(result)
					if bad == nil {
						result <- parseChunkStrict(chunk, reuse)
						return
					}
					// If the chunk never hands its backing array to a
					// parsed result, it stays ours and goes back to the
					// pool once processing is complete.
					if chunkOnly := parseChunkTolerant(result, chunk, firstLine, reuse); chunkOnly {
						streamBufPool.Put(chunk[:cap(chunk)])
					}
				}(tmp, lineNo)
				lineNo += int64(countLines(tmp))
			} else {
				streamBufPool.Put(tmp[:cap(tmp)])
			}
			if err != nil {
				// Should only really be io.EOF
				queueEvent(queue, streamEvent{Stream: Stream{Error: err}})
				return
			}
		}
	}()
}

// streamEvent is the unit forwarded from a chunk worker. Exactly one
// of Stream (a parsed block or a terminal error) or bad (a quarantined
// line) is meaningful.
type streamEvent struct {
	Stream
	bad *BadLine
}

// queueEvent enqueues a single terminal event, keeping the queue shape
// used for regular chunk results.
func queueEvent(queue chan chan streamEvent, ev streamEvent) {
	result := make(chan streamEvent, 1)
	queue <- result
	result <- ev
	close(result)
}

// parseChunkStrict keeps the original behavior: parse the whole chunk
// in one go and turn any failure into a terminal stream error.
func parseChunkStrict(tmp []byte, reuse <-chan *ParsedJson) streamEvent {
	var pj internalParsedJson
	pj.copyStrings = true
	select {
	case v := <-reuse:
		if cap(v.Message) >= streamChunkSize+1024 {
			streamBufPool.Put(v.Message[:cap(v.Message)])
			v.Message = nil
		}
		pj.ParsedJson = *v

	default:
	}
	parseErr := pj.parseMessage(tmp, true)
	if parseErr != nil {
		return streamEvent{Stream: Stream{
			Value: nil,
			Error: fmt.Errorf("parsing input: %w", parseErr),
		}}
	}
	parsed := pj.ParsedJson
	return streamEvent{Stream: Stream{
		Value: &parsed,
		Error: nil,
	}}
}

// parseChunkTolerant parses one chunk with per-line fault isolation.
// It first attempts the fast bulk parse; only when that fails does it
// fall back to line-by-line parsing. Events are emitted in line order
// on result. It reports whether the whole chunk buffer stayed owned by
// this worker (true) or was handed to one or more parsed results.
func parseChunkTolerant(result chan<- streamEvent, chunk []byte, firstLine int64, reuse <-chan *ParsedJson) (chunkOnly bool) {
	chunkOnly = true

	// Fast path: the common case is a fully valid chunk.
	var fast internalParsedJson
	fast.copyStrings = true
	if err := fast.parseMessage(chunk, true); err == nil {
		parsed := fast.ParsedJson
		result <- streamEvent{Stream: Stream{Value: &parsed}}
		// The result keeps chunk via pj.Message, so it must not be
		// pooled here; it is recycled through the reuse channel.
		return false
	}

	// Slow path: isolate the offending line(s). Each physical line is
	// parsed independently; failures are quarantined and parsing
	// continues with the next line.
	lineNo := firstLine
	remaining := chunk
	for len(remaining) > 0 {
		i := bytes.IndexByte(remaining, '\n')
		var line []byte
		if i < 0 {
			line, remaining = remaining, nil
		} else {
			line, remaining = remaining[:i+1], remaining[i+1:]
		}

		// Blank lines are legal separators in NDJSON; skip them but
		// keep accounting for their line numbers.
		if len(bytes.TrimSpace(line)) == 0 {
			lineNo++
			continue
		}

		var pj internalParsedJson
		pj.copyStrings = true
		select {
		case v := <-reuse:
			if cap(v.Message) >= streamChunkSize+1024 {
				streamBufPool.Put(v.Message[:cap(v.Message)])
				v.Message = nil
			}
			pj.ParsedJson = *v
		default:
		}

		if perr := pj.parseMessage(line, true); perr != nil {
			raw := make([]byte, len(line))
			copy(raw, line)
			result <- streamEvent{bad: &BadLine{
				Line: lineNo,
				Raw:  raw,
				Err:  fmt.Errorf("parsing input line %d: %w", lineNo, perr),
			}}
		} else {
			parsed := pj.ParsedJson
			result <- streamEvent{Stream: Stream{Value: &parsed}}
			// pj.Message points into chunk, which therefore stays in
			// use until the consumer is done with this result.
			chunkOnly = false
		}
		lineNo++
	}
	return chunkOnly
}

// countLines counts the number of physical lines in b. A chunk ending
// with a newline counts the line it terminates; a trailing fragment
// without a newline counts as one (possibly still open) line.
func countLines(b []byte) int {
	n := bytes.Count(b, []byte{'\n'})
	if len(b) > 0 && b[len(b)-1] != '\n' {
		n++
	}
	return n
}
