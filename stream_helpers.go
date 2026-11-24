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
	"io"
	"sync"
)

// streamBufferSize is the chunk size used when reading NDJSON streams.
const streamBufferSize = 10 << 20

// bufferPool returns a pool of reusable read buffers for NDJSON streaming.
// It is shared by the SIMD and portable stream implementations.
func bufferPool() *sync.Pool {
	return &sync.Pool{New: func() interface{} {
		return make([]byte, streamBufferSize+1024)
	}}
}

// parseNDStreamPortable reads NDJSON from buf and parses each chunk using the
// portable pure Go parser, forwarding results to res in input order.
// The channel is closed when the stream ends (io.EOF) or an error occurs.
func parseNDStreamPortable(buf *bufio.Reader, res chan<- Stream, reuse <-chan *ParsedJson,
	copyStrings bool, tmpSize int, tmpPool *sync.Pool) {
	go func() {
		defer close(res)
		for {
			tmp := tmpPool.Get().([]byte)
			tmp = tmp[:tmpSize]
			n, err := buf.Read(tmp)
			if err != nil && err != io.EOF {
				res <- Stream{Error: err}
				return
			}
			tmp = tmp[:n]
			// Read until newline.
			if err != io.EOF {
				b, err2 := buf.ReadBytes('\n')
				if err2 != nil && err2 != io.EOF {
					res <- Stream{Error: err2}
					return
				}
				tmp = append(tmp, b...)
				// Forward io.EOF.
				err = err2
			}

			if len(bytes.TrimSpace(tmp)) > 0 {
				var pj internalParsedJson
				pj.copyStrings = copyStrings
				pj.parser = ParserGo
				pj.activeParser = ParserGo
				select {
				case v := <-reuse:
					if cap(v.Message) >= tmpSize+1024 {
						tmpPool.Put(v.Message)
						v.Message = nil
					}
					pj.ParsedJson = *v
				default:
				}
				if parseErr := pj.parseMessagePortable(tmp, true); parseErr != nil {
					res <- Stream{Error: parseErr}
					return
				}
				parsed := pj.ParsedJson
				res <- Stream{Value: &parsed, Error: nil}
			} else {
				tmpPool.Put(tmp)
			}
			if err != nil {
				// Should only really be io.EOF.
				res <- Stream{Error: err}
				return
			}
		}
	}()
}
