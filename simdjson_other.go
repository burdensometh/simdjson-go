//go:build !amd64 || appengine || !gc || noasm
// +build !amd64 appengine !gc noasm

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
	"io"
)

// SupportedCPU will return whether the CPU is supported.
// On builds without the SIMD implementation the CPU is never considered
// supported, but the portable pure Go parser (see WithParser) can still be
// used.
func SupportedCPU() bool {
	return false
}

// newInternalParsedJson prepares a ParsedJson for the portable parser.
// Requesting ParserSIMD explicitly remains an error on these builds.
func newInternalParsedJson(reuse *ParsedJson, opts []ParserOption) (*internalParsedJson, error) {
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
	// Reset parser selection so a reused ParsedJson cannot leak a previous
	// WithParser choice; ParserAuto is the documented default.
	pj.parser = ParserAuto
	for _, opt := range opts {
		if err := opt(pj); err != nil {
			return nil, err
		}
	}
	if pj.parser == ParserSIMD {
		return nil, errors.New("Host CPU does not meet target specs")
	}
	// Both ParserAuto and ParserGo resolve to the portable implementation.
	pj.parser = ParserGo
	return pj, nil
}

// Parse an object or array from a block of data and return the parsed JSON.
// An optional block of previously parsed json can be supplied to reduce allocations.
//
// On platforms without the SIMD implementation this always uses the portable
// pure Go parser unless ParserSIMD is explicitly requested.
func Parse(b []byte, reuse *ParsedJson, opts ...ParserOption) (*ParsedJson, error) {
	pj, err := newInternalParsedJson(reuse, opts)
	if err != nil {
		return nil, err
	}
	if err = pj.parseMessagePortable(b, false); err != nil {
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
	if err = pj.parseMessagePortable(bytes.TrimSpace(b), true); err != nil {
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
//
// This implementation uses the portable pure Go parser.
func ParseNDStream(r io.Reader, res chan<- Stream, reuse <-chan *ParsedJson, opts ...ParserOption) {
	copyStrings := true
	var tmp internalParsedJson
	for _, opt := range opts {
		if err := opt(&tmp); err != nil {
			go func() {
				res <- Stream{Error: err}
				close(res)
			}()
			return
		}
	}
	copyStrings = tmp.copyStrings
	if tmp.parser == ParserSIMD {
		go func() {
			res <- Stream{Error: errors.New("Host CPU does not meet target specs")}
			close(res)
		}()
		return
	}
	const tmpSize = streamBufferSize
	buf := bufio.NewReaderSize(r, tmpSize)
	parseNDStreamPortable(buf, res, reuse, copyStrings, tmpSize, bufferPool())
}
