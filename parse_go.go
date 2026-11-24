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
	"unicode/utf8"
)

const (
	// UTF-16 surrogate halves, see RFC 8259 section 7.
	surrogateMin = 0xD800
	highSurMax   = 0xDBFF
	lowSurMax    = 0xDFFF
)

func isHighSurrogate(cp int) bool { return cp >= surrogateMin && cp <= highSurMax }
func isLowSurrogate(cp int) bool  { return cp >= highSurMax+1 && cp <= lowSurMax }

// errPortableParse is returned for any malformed JSON encountered by the
// portable parser. Exact wording is deliberately generic, matching the
// SIMD implementation which does not report per-token error messages.
var errPortableParse = errors.New("bad JSON parsing (portable parser)")

// goScope records an open container (root, object or array) so that the
// opening tape entry can be back-filled with the location of its matching
// closing entry, exactly as the SIMD stage 2 machine does.
type goScope struct {
	// openLoc is the tape index of the opening tag.
	openLoc uint64
	// kind is one of TagRoot, TagObjectStart or TagArrayStart.
	kind Tag
}

// portableParser is a hand-written, recursive-descent style JSON parser that
// emits the exact same tape format as the SIMD implementation.
//
// It deliberately reuses the shared, architecture independent number parser
// (parseNumber) and the same tag constants, so integers, unsigned integers,
// floats, null values and overflow flags are byte-for-byte identical to the
// SIMD path regardless of the host architecture.
type portableParser struct {
	pj     *internalParsedJson
	msg    []byte
	off    int
	ndjson bool

	// scopes holds open containers. A root is always on the stack.
	scopes []goScope
}

// parseMessagePortable parses msg (already trimmed of surrounding whitespace
// by the caller) and fills pj.Tape and pj.Strings. When ndjson is true the
// message may contain several newline delimited root elements.
func (pj *internalParsedJson) parseMessagePortable(msg []byte, ndjson bool) error {
	pj.Message = bytes.TrimSpace(msg)
	pj.initializePortable(len(pj.Message))
	pj.ndjson = 0
	if ndjson {
		pj.ndjson = 1
	}
	pj.isvalid = false
	pj.activeParser = ParserGo

	p := &portableParser{
		pj:     pj,
		msg:    pj.Message,
		ndjson: ndjson,
		scopes: make([]goScope, 0, 16),
	}
	if err := p.run(); err != nil {
		return err
	}
	pj.isvalid = true
	return nil
}

// initializePortable allocates the tape and string buffer using the same
// sizing heuristics as the SIMD initialize() routine.
func (pj *internalParsedJson) initializePortable(size int) {
	avgTapeSize := size * 15 / 100
	if cap(pj.Tape) < avgTapeSize {
		pj.Tape = make([]uint64, 0, avgTapeSize)
	}
	pj.Tape = pj.Tape[:0]

	stringsSize := size / 10
	if stringsSize < 128 {
		stringsSize = 128
	}
	if pj.Strings != nil && cap(pj.Strings.B) >= stringsSize {
		pj.Strings.B = pj.Strings.B[:0]
	} else {
		pj.Strings = &TStrings{make([]byte, 0, stringsSize)}
	}
	pj.containingScopeOffset = pj.containingScopeOffset[:0]
}

func (p *portableParser) run() error {
	// An empty message produces no tape, mirroring the SIMD machine which
	// never reaches its success state with content.
	p.skipInlineWhitespace()
	if p.off >= len(p.msg) {
		return errPortableParse
	}

	for {
		// Every iteration opens one root element.
		rootOpen := p.pj.get_current_loc()
		p.pj.write_tape(0, byte(TagRoot))
		p.scopes = append(p.scopes, goScope{openLoc: rootOpen, kind: TagRoot})

		if err := p.parseValue(); err != nil {
			return err
		}

		// Close the root, back-filling its opening entry. The root points one
		// entry past itself, matching the ndjson tape layout described in the
		// README.
		scope := p.scopes[len(p.scopes)-1]
		p.scopes = p.scopes[:len(p.scopes)-1]
		endLoc := p.pj.get_current_loc() + 1
		p.pj.annotate_previousloc(scope.openLoc, endLoc)
		p.pj.write_tape(scope.openLoc, byte(TagRoot))

		if !p.ndjson {
			p.skipInlineWhitespace()
			if p.off != len(p.msg) {
				return errPortableParse
			}
			return nil
		}

		// NDJSON: roots must be separated by at least one newline.
		// Token level parsing accepts any JSON whitespace, so the newline
		// requirement is enforced here between roots.
		sawNewline := false
		for p.off < len(p.msg) {
			switch p.msg[p.off] {
			case ' ', '\t', '\r':
				p.off++
			case '\n':
				sawNewline = true
				p.off++
			default:
				goto nextRoot
			}
		}
	nextRoot:
		if p.off >= len(p.msg) {
			return nil
		}
		if !sawNewline {
			return errPortableParse
		}
		// Consume any number of empty lines.
		for p.off < len(p.msg) && p.msg[p.off] == '\n' {
			p.off++
		}
		p.skipInlineWhitespace()
		if p.off >= len(p.msg) {
			return nil
		}
	}
}

// parseValue parses a single JSON value at the current offset.
func (p *portableParser) parseValue() error {
	p.skipInlineWhitespace()
	if p.off >= len(p.msg) {
		return errPortableParse
	}
	switch p.msg[p.off] {
	case '{':
		return p.parseObject()
	case '[':
		return p.parseArray()
	case '"':
		return p.parseStringValue()
	case 't':
		if !p.matchAtom("true") {
			return errPortableParse
		}
		p.pj.write_tape(0, byte(TagBoolTrue))
		return nil
	case 'f':
		if !p.matchAtom("false") {
			return errPortableParse
		}
		p.pj.write_tape(0, byte(TagBoolFalse))
		return nil
	case 'n':
		if !p.matchAtom("null") {
			return errPortableParse
		}
		p.pj.write_tape(0, byte(TagNull))
		return nil
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		if !addNumber(p.msg[p.off:], &p.pj.ParsedJson) {
			return errPortableParse
		}
		// Advance over the raw number characters. parseNumber stops at the
		// first structural/whitespace character.
		p.advanceNumber()
		return nil
	default:
		return errPortableParse
	}
}

func (p *portableParser) parseObject() error {
	openLoc := p.pj.get_current_loc()
	p.pj.write_tape(0, byte(TagObjectStart))
	p.scopes = append(p.scopes, goScope{openLoc: openLoc, kind: TagObjectStart})
	p.off++ // consume '{'

	p.skipInlineWhitespace()
	if p.off < len(p.msg) && p.msg[p.off] == '}' {
		p.off++
		return p.closeScope(TagObjectEnd)
	}

	for {
		p.skipInlineWhitespace()
		if p.off >= len(p.msg) || p.msg[p.off] != '"' {
			return errPortableParse
		}
		// Key
		if err := p.parseStringValue(); err != nil {
			return err
		}
		p.skipInlineWhitespace()
		if p.off >= len(p.msg) || p.msg[p.off] != ':' {
			return errPortableParse
		}
		p.off++ // consume ':'
		// Value
		if err := p.parseValue(); err != nil {
			return err
		}

		p.skipInlineWhitespace()
		if p.off >= len(p.msg) {
			return errPortableParse
		}
		switch p.msg[p.off] {
		case ',':
			p.off++
			continue
		case '}':
			p.off++
			return p.closeScope(TagObjectEnd)
		default:
			return errPortableParse
		}
	}
}

func (p *portableParser) parseArray() error {
	openLoc := p.pj.get_current_loc()
	p.pj.write_tape(0, byte(TagArrayStart))
	p.scopes = append(p.scopes, goScope{openLoc: openLoc, kind: TagArrayStart})
	p.off++ // consume '['

	p.skipInlineWhitespace()
	if p.off < len(p.msg) && p.msg[p.off] == ']' {
		p.off++
		return p.closeScope(TagArrayEnd)
	}

	for {
		if err := p.parseValue(); err != nil {
			return err
		}
		p.skipInlineWhitespace()
		if p.off >= len(p.msg) {
			return errPortableParse
		}
		switch p.msg[p.off] {
		case ',':
			p.off++
			continue
		case ']':
			p.off++
			return p.closeScope(TagArrayEnd)
		default:
			return errPortableParse
		}
	}
}

// closeScope writes the closing tag of the innermost container and back-fills
// the opening tag's payload. The SIMD machine writes the closing tag first and
// then annotates the opening tag with the tape position *after* the closing
// tag, so the same ordering and value is reproduced here.
func (p *portableParser) closeScope(closeTag Tag) error {
	if len(p.scopes) == 0 {
		return errPortableParse
	}
	scope := p.scopes[len(p.scopes)-1]
	p.scopes = p.scopes[:len(p.scopes)-1]
	// The closing tag carries the location of its matching opening tag.
	p.pj.write_tape(scope.openLoc, byte(closeTag))
	// Back-fill the opening tag with the position after the closing tag.
	p.pj.annotate_previousloc(scope.openLoc, p.pj.get_current_loc())
	return nil
}

// matchAtom verifies that the literal at the current offset equals want and
// that it is followed by a structural or whitespace character (or end of
// input). On success it advances past the literal.
func (p *portableParser) matchAtom(want string) bool {
	if p.off+len(want) > len(p.msg) || string(p.msg[p.off:p.off+len(want)]) != want {
		return false
	}
	after := p.off + len(want)
	if after < len(p.msg) && isNotStructuralOrWhitespace(p.msg[after]) != 0 {
		return false
	}
	p.off = after
	return true
}

// advanceNumber moves the offset past the number that parseNumber just read.
// It mirrors the character classification used by parseNumber.
func (p *portableParser) advanceNumber() {
	for p.off < len(p.msg) {
		t := isNumberRune[p.msg[p.off]]
		if t == 0 || t == isEOVFlag {
			return
		}
		p.off++
	}
}

// parseStringValue parses a quoted string and writes the two tape entries
// (payload/length) used by the shared tape machinery.
func (p *portableParser) parseStringValue() error {
	start := p.off // index of the opening quote in pj.Message
	if start >= len(p.msg) || p.msg[start] != '"' {
		return errPortableParse
	}

	// First pass: locate the closing quote, validate escapes and raw UTF-8,
	// and determine whether the string must be copied into the string buffer.
	i := start + 1
	needCopy := p.pj.copyStrings
	for {
		if i >= len(p.msg) {
			return errPortableParse
		}
		c := p.msg[i]
		if c == '"' {
			break
		}
		if c == '\\' {
			// Decode the escape to validate it and to know how many input
			// bytes it consumes. Escapes always force a copy because the
			// decoded bytes differ from the raw message bytes.
			consumed, _, err := p.decodeEscape(i)
			if err != nil {
				return err
			}
			needCopy = true
			i += consumed
			continue
		}
		if c < 0x20 {
			// Unescaped control characters are invalid.
			return errPortableParse
		}
		if c < utf8.RuneSelf {
			i++
			continue
		}
		// Validate a raw multi-byte UTF-8 sequence and skip it as a whole.
		r, size := utf8.DecodeRune(p.msg[i:])
		if r == utf8.RuneError && size == 1 {
			return errPortableParse
		}
		i += size
	}
	rawEnd := i // index of the closing quote

	if !needCopy {
		// Point directly into the message: payload is the offset of the first
		// content byte, length the number of content bytes.
		payload := uint64(start + 1)
		length := uint64(rawEnd - (start + 1))
		p.pj.write_tape(payload, byte(TagString))
		p.pj.Tape = append(p.pj.Tape, length)
		p.off = rawEnd + 1
		return nil
	}

	// Second pass: decode the string into the dedicated string buffer.
	bufStart := len(p.pj.Strings.B)
	j := start + 1
	for j < rawEnd {
		c := p.msg[j]
		if c == '\\' {
			consumed, decoded, err := p.decodeEscape(j)
			if err != nil {
				return err
			}
			p.pj.Strings.B = append(p.pj.Strings.B, decoded...)
			j += consumed
			continue
		}
		if c < utf8.RuneSelf {
			p.pj.Strings.B = append(p.pj.Strings.B, c)
			j++
			continue
		}
		r, size := utf8.DecodeRune(p.msg[j:])
		if r == utf8.RuneError && size == 1 {
			return errPortableParse
		}
		p.pj.Strings.B = append(p.pj.Strings.B, p.msg[j:j+size]...)
		j += size
	}

	payload := STRINGBUFBIT | uint64(bufStart)
	length := uint64(len(p.pj.Strings.B) - bufStart)
	p.pj.write_tape(payload, byte(TagString))
	p.pj.Tape = append(p.pj.Tape, length)
	p.off = rawEnd + 1
	return nil
}

// decodeEscape decodes the escape sequence beginning at the backslash located
// at index i. It returns the number of message bytes consumed (including the
// backslash) and the UTF-8 bytes the escape represents.
//
// The validation and surrogate handling match the SIMD string parser:
// simple escapes are restricted to the same accepted set, \u requires four
// hex digits, and a high surrogate must be immediately followed by a
// \uXXXX low surrogate; anything else yields the Unicode replacement
// character.
func (p *portableParser) decodeEscape(i int) (consumed int, decoded []byte, err error) {
	if i+1 >= len(p.msg) {
		return 0, nil, errPortableParse
	}
	switch p.msg[i+1] {
	case '"':
		return 2, []byte{'"'}, nil
	case '\\':
		return 2, []byte{'\\'}, nil
	case '/':
		return 2, []byte{'/'}, nil
	case 'b':
		return 2, []byte{'\b'}, nil
	case 'f':
		return 2, []byte{'\f'}, nil
	case 'n':
		return 2, []byte{'\n'}, nil
	case 'r':
		return 2, []byte{'\r'}, nil
	case 't':
		return 2, []byte{'\t'}, nil
	case 'u':
		// Fall through to unicode handling below.
	default:
		return 0, nil, errPortableParse
	}

	cp, ok := p.parseHex4(i + 2)
	if !ok {
		return 0, nil, errPortableParse
	}
	consumed = 6

	switch {
	case isHighSurrogate(cp):
		// A high surrogate must be immediately followed by \uXXXX and the
		// following code unit must be a low surrogate. The SIMD decoder
		// rejects anything else rather than emitting a replacement rune.
		if i+12 > len(p.msg) || p.msg[i+6] != '\\' || p.msg[i+7] != 'u' {
			return 0, nil, errPortableParse
		}
		low, ok := p.parseHex4(i + 8)
		if !ok || !isLowSurrogate(low) {
			return 0, nil, errPortableParse
		}
		consumed = 12
		r := 0x10000 + (cp-surrogateMin)<<10 + (low - (highSurMax + 1))
		var tmp [4]byte
		n := utf8.EncodeRune(tmp[:], rune(r))
		return consumed, tmp[:n], nil
	case isLowSurrogate(cp):
		// A lone low surrogate is encoded as a raw 3-byte sequence (WTF-8),
		// matching the SIMD decoder, e.g. 񀀀 -> ed b0 80.
		return consumed, encodeSurrogate3(cp), nil
	default:
		var tmp [4]byte
		n := utf8.EncodeRune(tmp[:], rune(cp))
		return consumed, tmp[:n], nil
	}
}

// encodeSurrogate3 encodes a code point in the surrogate range (0xD800 to
// 0xDFFF) using the same raw 3-byte UTF-8 style encoding the SIMD decoder
// emits for lone low surrogates.
func encodeSurrogate3(cp int) []byte {
	return []byte{
		0xE0 | byte(cp>>12),
		0x80 | byte((cp>>6)&0x3F),
		0x80 | byte(cp&0x3F),
	}
}

// parseHex4 reads exactly four hexadecimal digits starting at i and returns
// their numeric value. It returns false if fewer than four valid digits are
// available.
func (p *portableParser) parseHex4(i int) (int, bool) {
	if i+4 > len(p.msg) {
		return 0, false
	}
	v := 0
	for k := 0; k < 4; k++ {
		c := p.msg[i+k]
		var d byte
		switch {
		case c >= '0' && c <= '9':
			d = c - '0'
		case c >= 'a' && c <= 'f':
			d = c - 'a' + 10
		case c >= 'A' && c <= 'F':
			d = c - 'A' + 10
		default:
			return 0, false
		}
		v = v<<4 | int(d)
	}
	return v, true
}

// skipInlineWhitespace skips any JSON whitespace between tokens. All four
// JSON whitespace characters (space, tab, carriage return, newline) are
// legal inside a single document; NDJSON root separation is validated
// separately in run().
func (p *portableParser) skipInlineWhitespace() {
	for p.off < len(p.msg) {
		switch p.msg[p.off] {
		case ' ', '\t', '\r', '\n':
			p.off++
		default:
			return
		}
	}
}
