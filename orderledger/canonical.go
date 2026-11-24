/*
 * MinIO Cloud Storage, (C) 2026 MinIO, Inc.
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

package orderledger

import (
	"fmt"
	"math"
	"sort"
	"strconv"
)

// canonicalValue appends a deterministic, injective encoding of v to dst.
//
// The encoding is not meant to be emitted as JSON to a consumer; it only has
// to be stable so two decoded records that carry the same values produce the
// same bytes. Object keys are sorted, and numbers use a shortest round-trip
// form where JSON integer and float notations of the same value coincide
// (10, 10.0 and 1e1 all encode as 10).
func canonicalValue(dst []byte, v interface{}) ([]byte, error) {
	switch t := v.(type) {
	case nil:
		return append(dst, "null"...), nil
	case bool:
		if t {
			return append(dst, "true"...), nil
		}
		return append(dst, "false"...), nil
	case string:
		return appendCanonicalString(dst, t), nil
	case int64:
		return strconv.AppendInt(dst, t, 10), nil
	case uint64:
		return strconv.AppendUint(dst, t, 10), nil
	case float64:
		return appendCanonicalFloat(dst, t), nil
	case []interface{}:
		dst = append(dst, '[')
		for i, elem := range t {
			if i > 0 {
				dst = append(dst, ',')
			}
			var err error
			dst, err = canonicalValue(dst, elem)
			if err != nil {
				return nil, err
			}
		}
		return append(dst, ']'), nil
	case map[string]interface{}:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		dst = append(dst, '{')
		for i, k := range keys {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendCanonicalString(dst, k)
			dst = append(dst, ':')
			var err error
			dst, err = canonicalValue(dst, t[k])
			if err != nil {
				return nil, err
			}
		}
		return append(dst, '}'), nil
	}
	return nil, fmt.Errorf("orderledger: cannot canonicalize value of type %T", v)
}

// appendCanonicalString appends a byte-wise escaped string.
// Unlike JSON escaping invalid UTF-8 is kept as-is (escaped per byte), so two
// distinct byte strings never share a canonical encoding.
func appendCanonicalString(dst []byte, s string) []byte {
	const hexDigits = "0123456789abcdef"
	dst = append(dst, '"')
	for i := 0; i < len(s); i++ {
		b := s[i]
		switch {
		case b == '"':
			dst = append(dst, '\\', '"')
		case b == '\\':
			dst = append(dst, '\\', '\\')
		case b < 0x20:
			dst = append(dst, '\\', 'u', '0', '0', hexDigits[b>>4], hexDigits[b&0xf])
		default:
			dst = append(dst, b)
		}
	}
	return append(dst, '"')
}

// appendCanonicalFloat appends a float in a form that coincides with the
// integer encoding for integral values, so int/uint/float notations of the
// same number compare equal.
func appendCanonicalFloat(dst []byte, f float64) []byte {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return strconv.AppendFloat(dst, f, 'g', -1, 64)
	}
	if f == math.Trunc(f) && math.Abs(f) < 1e21 {
		return strconv.AppendFloat(dst, f, 'f', -1, 64)
	}
	return strconv.AppendFloat(dst, f, 'g', -1, 64)
}
