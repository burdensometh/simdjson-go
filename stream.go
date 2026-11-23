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

// BadLine describes a single NDJSON line that could not be parsed.
// It is delivered to the quarantine channel of a tolerant stream
// (see ParseNDStreamTolerant) so the original line and its position
// can be inspected without stopping the rest of the stream.
type BadLine struct {
	// Line is the 1-based line number of the raw line within the
	// complete input stream. Numbering is continuous across the
	// internal read chunks, and blank lines are counted as well.
	Line int64

	// Raw is the original, unparsed line, including its trailing
	// newline when one was present. The slice is copied and therefore
	// remains valid after the parser reuses its internal buffers.
	Raw []byte

	// Err is the error returned while attempting to parse the line.
	Err error
}
