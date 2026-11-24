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
	"errors"
	"fmt"
	"strconv"
)

// ErrBadVersion is returned (wrapped) when a version field cannot be
// interpreted as an integer.
var ErrBadVersion = errors.New("orderledger: invalid version")

// Version is the version of an order update record.
//
// Versions are integers and can be negative. They are normalized from JSON
// integers, or from JSON strings holding a decimal integer (an optional
// leading '-' is accepted). This means that 3, "3" and "003" all decode to
// the same Version, while 1.5, "v2", true or null do not decode at all.
//
// The zero Version is the valid version 0.
type Version struct {
	neg bool
	mag uint64
}

// ParseVersion parses a decimal integer version string.
// Leading zeros are accepted, a leading '-' is accepted, and a leading '+'
// or surrounding whitespace is not.
func ParseVersion(s string) (Version, error) {
	orig := s
	neg := false
	if len(s) > 0 && s[0] == '-' {
		neg = true
		s = s[1:]
	}
	if len(s) == 0 {
		return Version{}, fmt.Errorf("%w: %q is not an integer", ErrBadVersion, orig)
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return Version{}, fmt.Errorf("%w: %q is not an integer", ErrBadVersion, orig)
		}
	}
	mag, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return Version{}, fmt.Errorf("%w: %q: %v", ErrBadVersion, orig, err)
	}
	if mag == 0 {
		neg = false
	}
	return Version{neg: neg, mag: mag}, nil
}

// versionFromValue converts a decoded JSON scalar to a Version.
func versionFromValue(v interface{}) (Version, error) {
	switch t := v.(type) {
	case int64:
		return versionFromInt64(t), nil
	case uint64:
		return Version{mag: t}, nil
	case string:
		return ParseVersion(t)
	case float64:
		return Version{}, fmt.Errorf("%w: %v is a float; versions must be integers", ErrBadVersion, t)
	default:
		return Version{}, fmt.Errorf("%w: got %T; versions must be integers", ErrBadVersion, v)
	}
}

func versionFromInt64(v int64) Version {
	if v >= 0 {
		return Version{mag: uint64(v)}
	}
	// Handle math.MinInt64 without overflowing.
	return Version{neg: true, mag: uint64(-(v + 1)) + 1}
}

// Compare returns -1, 0 or 1 if v is less than, equal to or greater than o.
func (v Version) Compare(o Version) int {
	switch {
	case v.neg && !o.neg:
		return -1
	case !v.neg && o.neg:
		return 1
	case v.neg:
		// Both negative: the larger magnitude is the smaller version.
		switch {
		case v.mag > o.mag:
			return -1
		case v.mag < o.mag:
			return 1
		default:
			return 0
		}
	default:
		switch {
		case v.mag < o.mag:
			return -1
		case v.mag > o.mag:
			return 1
		default:
			return 0
		}
	}
}

// Equal reports whether v and o are the same version.
func (v Version) Equal(o Version) bool {
	return v == o
}

// String returns the version as a decimal integer.
func (v Version) String() string {
	if v.neg {
		return "-" + strconv.FormatUint(v.mag, 10)
	}
	return strconv.FormatUint(v.mag, 10)
}
