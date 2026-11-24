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

// Parser selects which concrete parsing implementation is used to turn
// a JSON message into the internal tape format.
//
// The SIMD implementation (ParserSIMD) requires a CPU with AVX2 and CLMUL
// support. The portable implementation (ParserGo) is pure Go, uses no
// assembly or SIMD instructions and produces exactly the same tape,
// string buffer and number tags as the SIMD implementation, so iteration,
// projection and serialization work identically regardless of the path
// selected.
type Parser int

const (
	// ParserAuto selects the SIMD implementation when the host CPU supports
	// it and otherwise falls back to the portable pure Go implementation.
	// This is the default.
	ParserAuto Parser = iota

	// ParserSIMD forces the SIMD implementation.
	// Parsing returns an error if the host CPU does not support it.
	ParserSIMD

	// ParserGo forces the portable, pure Go implementation.
	// It works on every architecture and does not require SIMD support.
	ParserGo
)

// String returns a human readable identifier of the parser.
// The returned value is stable and can be used to report which parsing
// path produced a ParsedJson.
func (p Parser) String() string {
	switch p {
	case ParserAuto:
		return "auto"
	case ParserSIMD:
		return "simd"
	case ParserGo:
		return "go"
	default:
		return "unknown"
	}
}

// ParserOption is a parser option.
type ParserOption func(pj *internalParsedJson) error

// WithCopyStrings will copy strings so they no longer reference the input.
// For enhanced performance, simdjson-go can point back into the original JSON buffer for strings,
// however this can lead to issues in streaming use cases scenarios, or scenarios in which
// the underlying JSON buffer is reused. So the default behaviour is to create copies of all
// strings (not just those transformed anyway for unicode escape characters) into the separate
// Strings buffer (at the expense of using more memory and less performance).
// Default: true - strings are copied.
func WithCopyStrings(b bool) ParserOption {
	return func(pj *internalParsedJson) error {
		pj.copyStrings = b
		return nil
	}
}

// WithParser selects the parsing implementation used by Parse, ParseND and
// ParseNDStream.
//
// ParserAuto (the default) keeps the historical behaviour on machines with a
// supported CPU (AVX2 + CLMUL) while transparently enabling parsing on
// machines without SIMD support through the portable pure Go implementation.
//
// ParserSIMD always uses the SIMD implementation and returns an error on
// unsupported CPUs.
//
// ParserGo always uses the portable pure Go implementation. The resulting
// ParsedJson exposes the selected implementation through its Parser method.
func WithParser(p Parser) ParserOption {
	return func(pj *internalParsedJson) error {
		pj.parser = p
		return nil
	}
}
