package simdjson

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

// WithRawNumbers enables preservation of the original lexical (decimal)
// representation of the numeric values of the specified object fields.
//
// Some identifiers, for example log order ids, can exceed the range of a
// uint64. Storing such a number as a float64 loses precision: two distinct
// ids that round to the same float64 would become the same key. When this
// option lists a field, the number paired with that field name is kept as the
// exact bytes that appeared in the JSON input instead of being converted to an
// int64/uint64/float64.
//
// The matching is by object field name only, at any nesting depth. Numbers
// outside of objects (for example bare array elements) are never affected.
// The option is opt-in: without it every number is still parsed and accessed
// exactly as before through Int, Uint, Float, Interface, etc.
//
// The preserved value is read with Iter.RawNumber (or Iter.String) and is
// emitted verbatim when the parsed data is re-serialized as JSON, so its
// identity stays exact through marshal and serialize/deserialize round trips.
//
// The exact lexical text of every input shape is retained:
//   - Integer notation ("123456789012345678901") is kept as the raw digits,
//     with no float rounding.
//   - Fixed-point/decimal notation ("12.340") is kept character for character,
//     including trailing zeros, which a float would drop.
//   - Exponential notation ("1.2e3", "7E-2") is kept with its original
//     mantissa, exponent sign and letter case.
func WithRawNumbers(fields ...string) ParserOption {
	return func(pj *internalParsedJson) error {
		if pj.rawNumberFields == nil {
			pj.rawNumberFields = make(map[string]struct{}, len(fields))
		}
		for _, f := range fields {
			if f == "" {
				continue
			}
			pj.rawNumberFields[f] = struct{}{}
		}
		return nil
	}
}
