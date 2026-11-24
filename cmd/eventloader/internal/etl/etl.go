// Package etl implements version-aware ingestion of payment/order events.
//
// Two upstream schema versions store the same business concepts (business
// time and amount) under different JSON paths. The warehouse reads one
// canonical field definition, so before any structured extraction the source
// version must be identified explicitly and the matching path mapping must be
// selected. Rows whose version cannot be identified are never guessed: they
// go to the backfill pending list for manual completion.
package etl

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/minio/simdjson-go"
)

// Canonical warehouse field names. Both source versions are normalized onto
// this single field definition.
const (
	FieldEventID      = "event_id"
	FieldBusinessTime = "business_time"
	FieldAmount       = "amount"
)

// Version values emitted in the backfill result.
const (
	VersionV1      = "v1"
	VersionV2      = "v2"
	VersionUnknown = "unknown"
)

// Conflict policy applied when BOTH the old and the new field are present on
// the same row. The contract is "new version wins":
//
//  1. The value read from the path mapped to the row's declared source
//     version remains the canonical value.
//  2. The shadow (other-version) field is recorded for audit and a conflict
//     flag is raised, but it never overwrites the canonical value.
const ConflictPolicy = "new-version-wins"

// Path is a JSON object path expressed as simdjson FindElement path segments.
type Path []string

// String renders the path in JSONPath-ish notation, e.g. $.event.time.
func (p Path) String() string {
	var b strings.Builder
	b.WriteByte('$')
	for _, seg := range p {
		b.WriteByte('.')
		b.WriteString(seg)
	}
	return b.String()
}

// FieldMapping binds one canonical field to the path where a given source
// version stores it.
type FieldMapping struct {
	Field string
	Path  Path
}

// VersionMapping is the complete path mapping for one source version.
type VersionMapping struct {
	Version string
	// VersionPath is the top-level field carrying the schema version tag.
	VersionPath Path
	// Mappings maps each canonical field to its version-specific path.
	Mappings map[string]FieldMapping
}

// PathOf returns the configured path for a canonical field.
func (m VersionMapping) PathOf(field string) (Path, bool) {
	fm, ok := m.Mappings[field]
	return fm.Path, ok
}

// DefaultMappings is the explicit source-version -> path mapping table.
//
// v1 (legacy, nested envelope):
//
//	$.event.event_id
//	$.event.time           business time, RFC3339 string
//	$.payment.amount       amount, number (string-encoded numbers accepted)
//
// v2 (current, flattened envelope):
//
//	$.event_id
//	$.biz_time             business time, RFC3339 string
//	$.amount               amount, number (string-encoded numbers accepted)
//
// The version tag itself is read from the top-level "schema_version" field;
// both versions carry it so the mapping can be selected before extraction.
func DefaultMappings() map[string]VersionMapping {
	return map[string]VersionMapping{
		VersionV1: {
			Version:     VersionV1,
			VersionPath: Path{"schema_version"},
			Mappings: map[string]FieldMapping{
				FieldEventID:      {Field: FieldEventID, Path: Path{"event", "event_id"}},
				FieldBusinessTime: {Field: FieldBusinessTime, Path: Path{"event", "time"}},
				FieldAmount:       {Field: FieldAmount, Path: Path{"payment", "amount"}},
			},
		},
		VersionV2: {
			Version:     VersionV2,
			VersionPath: Path{"schema_version"},
			Mappings: map[string]FieldMapping{
				FieldEventID:      {Field: FieldEventID, Path: Path{"event_id"}},
				FieldBusinessTime: {Field: FieldBusinessTime, Path: Path{"biz_time"}},
				FieldAmount:       {Field: FieldAmount, Path: Path{"amount"}},
			},
		},
	}
}

// CanonicalRecord is one normalized, warehouse-ready row. It carries the
// extracted values plus full lineage: which version was detected, which path
// each value actually came from, and any old/new field conflict observed.
type CanonicalRecord struct {
	LineNo int64 `json:"line_no"`

	// Canonical warehouse fields.
	EventID      string  `json:"event_id"`
	BusinessTime string  `json:"business_time"`
	Amount       float64 `json:"amount"`
	// AmountRaw preserves the original lexical representation for audit.
	AmountRaw string `json:"amount_raw"`

	// Lineage.
	SourceVersion  string `json:"source_version"`
	ConflictPolicy string `json:"conflict_policy"`
	// UsedPaths states the actual path read for every canonical field.
	UsedPaths map[string]string `json:"used_paths"`
	// Conflicts lists fields for which both old and new paths were present.
	Conflicts []FieldConflict `json:"conflicts,omitempty"`
}

// FieldConflict records one old/new field collision on the same row.
type FieldConflict struct {
	Field       string `json:"field"`
	ChosenPath  string `json:"chosen_path"`
	ChosenValue string `json:"chosen_value"`
	ShadowPath  string `json:"shadow_path"`
	ShadowValue string `json:"shadow_value"`
	Resolution  string `json:"resolution"`
}

// PendingReason codes used in the backfill pending list.
const (
	ReasonVersionUnknown = "version_unknown"
	ReasonFieldMissing   = "field_missing"
	ReasonFieldInvalid   = "field_invalid"
)

// PendingItem is a row that could not be backfilled and needs manual work.
type PendingItem struct {
	LineNo int64 `json:"line_no"`
	// DetectedVersion is the raw version tag seen, "" when absent.
	DetectedVersion string `json:"detected_version,omitempty"`
	Reason          string `json:"reason"`
	// Detail explains which field/path triggered the item.
	Detail string `json:"detail,omitempty"`
	// RawLine is the original NDJSON line, retained for manual completion.
	RawLine string `json:"raw_line"`
}

// Summary reports the counts of an ingestion run.
type Summary struct {
	TotalLines    int            `json:"total_lines"`
	Backfilled    int            `json:"backfilled"`
	Pending       int            `json:"pending"`
	Conflicts     int            `json:"conflicts"`
	ByVersion     map[string]int `json:"by_version"`
	PendingReason map[string]int `json:"pending_reason"`
}

func newSummary() *Summary {
	return &Summary{
		ByVersion:     map[string]int{},
		PendingReason: map[string]int{},
	}
}

// normalizeVersion maps a raw top-level schema_version tag onto a known
// source version. Numeric (1/2) and string ("1","v1",...) spellings are all
// accepted. The second return value is false when the tag is absent or does
// not identify a known version.
func normalizeVersion(raw string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "v1":
		return VersionV1, true
	case "2", "v2":
		return VersionV2, true
	default:
		return VersionUnknown, false
	}
}

// readScalar reads a scalar value at path from root iterator iter. Null and
// container types are rejected. Numbers and strings both return their lexical
// form; numbers additionally return a parsed float64 when requested.
type scalar struct {
	typ   simdjson.Type
	text  string
	num   float64
	isNum bool
}

func readScalar(iter *simdjson.Iter, path Path) (scalar, error) {
	var elem simdjson.Element
	e, err := iter.FindElement(&elem, path...)
	if err != nil {
		return scalar{}, err
	}
	switch e.Type {
	case simdjson.TypeString:
		b, err := e.Iter.StringBytes()
		if err != nil {
			return scalar{}, err
		}
		return scalar{typ: simdjson.TypeString, text: string(b)}, nil
	case simdjson.TypeInt, simdjson.TypeUint, simdjson.TypeFloat:
		f, err := e.Iter.Float()
		if err != nil {
			return scalar{}, err
		}
		// Lexical form via re-marshal keeps the representation faithful enough
		// for audit; callers that need the exact bytes should retain RawLine.
		return scalar{typ: e.Type, text: formatFloat(f), num: f, isNum: true}, nil
	case simdjson.TypeNull:
		return scalar{}, fmt.Errorf("path %s is null", path.String())
	default:
		return scalar{}, fmt.Errorf("path %s holds non-scalar type %s", path.String(), e.Type)
	}
}

// readStringScalar accepts a JSON string, or a JSON number/string whose text
// is returned (used for string-encoded amount fields).
func readText(iter *simdjson.Iter, path Path) (string, error) {
	s, err := readScalar(iter, path)
	if err != nil {
		return "", err
	}
	return s.text, nil
}

// pathPresent reports whether path resolves to any value (including null).
func pathPresent(iter *simdjson.Iter, path Path) bool {
	var elem simdjson.Element
	_, err := iter.FindElement(&elem, path...)
	return err == nil
}

// extractOne reads one canonical field through its version-mapped path and
// simultaneously inspects the other version's path to enforce the conflict
// policy when both old and new fields are present on the same row.
type fieldResult struct {
	text     string
	num      float64
	isNum    bool
	path     string
	conflict *FieldConflict
	missing  bool
	invalid  string
}

func extractField(iter *simdjson.Iter, field string, active, other VersionMapping) fieldResult {
	activePath := active.Mappings[field].Path
	otherPath := other.Mappings[field].Path

	sc, err := readScalar(iter, activePath)
	res := fieldResult{path: activePath.String()}
	if err != nil {
		if errors.Is(err, simdjson.ErrPathNotFound) {
			res.missing = true
			return res
		}
		res.invalid = err.Error()
		return res
	}
	res.text = sc.text
	res.isNum = sc.isNum
	res.num = sc.num

	// Conflict detection: the shadow path exists AND holds a non-null value.
	if pathPresent(iter, otherPath) {
		if shadow, sErr := readScalar(iter, otherPath); sErr == nil {
			if shadow.text != sc.text {
				res.conflict = &FieldConflict{
					Field:       field,
					ChosenPath:  activePath.String(),
					ChosenValue: sc.text,
					ShadowPath:  otherPath.String(),
					ShadowValue: shadow.text,
					Resolution:  ConflictPolicy,
				}
			}
		}
	}
	return res
}

// validateBusinessTime enforces the RFC3339 warehouse contract while keeping
// the original lexical value as the canonical output.
func validateBusinessTime(raw string) error {
	if _, err := time.Parse(time.RFC3339, raw); err != nil {
		return fmt.Errorf("business_time %q is not RFC3339: %w", raw, err)
	}
	return nil
}

// parseAmount accepts a JSON number or a string-encoded number.
func parseAmount(fr fieldResult) (float64, string, error) {
	if fr.isNum {
		return fr.num, fr.text, nil
	}
	// String-encoded amount.
	var f float64
	if _, err := fmt.Sscanf(strings.TrimSpace(fr.text), "%g", &f); err != nil {
		return 0, fr.text, fmt.Errorf("amount %q is not numeric: %w", fr.text, err)
	}
	return f, fr.text, nil
}

// ProcessRow applies version selection, path mapping and conflict resolution
// to a single parsed NDJSON row. Exactly one of *CanonicalRecord or
// *PendingItem is returned.
func ProcessRow(iter simdjson.Iter, lineNo int64, rawLine string, mappings map[string]VersionMapping) (*CanonicalRecord, *PendingItem) {
	v1 := mappings[VersionV1]
	v2 := mappings[VersionV2]

	// ---- Step 1: identify source version BEFORE structured extraction. ----
	var versionElem simdjson.Element
	_, vErr := iter.FindElement(&versionElem, v1.VersionPath...)
	rawVersion := ""
	if vErr == nil {
		switch versionElem.Type {
		case simdjson.TypeString:
			b, _ := versionElem.Iter.StringBytes()
			rawVersion = string(b)
		case simdjson.TypeInt, simdjson.TypeUint, simdjson.TypeFloat:
			if f, err := versionElem.Iter.Float(); err == nil {
				rawVersion = formatFloat(f)
			}
		default:
			// Booleans, nulls, objects: leave rawVersion empty so the row
			// is routed to the pending list as an unknown version.
		}
	}
	version, known := normalizeVersion(rawVersion)
	if !known {
		return nil, &PendingItem{
			LineNo:          lineNo,
			DetectedVersion: rawVersion,
			Reason:          ReasonVersionUnknown,
			Detail:          fmt.Sprintf("schema_version %q does not match a known version", rawVersion),
			RawLine:         rawLine,
		}
	}

	// ---- Step 2: select the mapping for the declared source version. ----
	active := mappings[version]
	other := v1
	if version == VersionV1 {
		other = v2
	}

	rec := &CanonicalRecord{
		LineNo:         lineNo,
		SourceVersion:  version,
		ConflictPolicy: ConflictPolicy,
		UsedPaths:      map[string]string{},
	}
	var pending *PendingItem
	addPending := func(reason, detail string) {
		if pending == nil {
			pending = &PendingItem{
				LineNo:          lineNo,
				DetectedVersion: rawVersion,
				Reason:          reason,
				RawLine:         rawLine,
			}
		}
		if pending.Detail != "" {
			pending.Detail += "; "
		}
		pending.Detail += detail
	}

	// ---- Step 3: extract every canonical field through its mapped path. ----
	idFR := extractField(&iter, FieldEventID, active, other)
	timeFR := extractField(&iter, FieldBusinessTime, active, other)
	amtFR := extractField(&iter, FieldAmount, active, other)

	for _, fr := range []struct {
		field string
		fr    fieldResult
	}{
		{FieldEventID, idFR},
		{FieldBusinessTime, timeFR},
		{FieldAmount, amtFR},
	} {
		if fr.fr.missing {
			addPending(ReasonFieldMissing,
				fmt.Sprintf("%s not found at mapped path %s", fr.field, fr.fr.path))
			continue
		}
		if fr.fr.invalid != "" {
			addPending(ReasonFieldInvalid,
				fmt.Sprintf("%s at %s: %s", fr.field, fr.fr.path, fr.fr.invalid))
			continue
		}
		rec.UsedPaths[fr.field] = fr.fr.path
		if fr.fr.conflict != nil {
			rec.Conflicts = append(rec.Conflicts, *fr.fr.conflict)
		}
	}

	// Field-level validation / type conversion.
	if !idFR.missing && idFR.invalid == "" {
		rec.EventID = idFR.text
	}
	if !timeFR.missing && timeFR.invalid == "" {
		if err := validateBusinessTime(timeFR.text); err != nil {
			addPending(ReasonFieldInvalid, err.Error())
			delete(rec.UsedPaths, FieldBusinessTime)
		} else {
			rec.BusinessTime = timeFR.text
		}
	}
	if !amtFR.missing && amtFR.invalid == "" {
		f, raw, err := parseAmount(amtFR)
		if err != nil {
			addPending(ReasonFieldInvalid, err.Error())
			delete(rec.UsedPaths, FieldAmount)
		} else {
			rec.Amount = f
			rec.AmountRaw = raw
		}
	}

	if pending != nil {
		// A row missing/invalid on any canonical field cannot be warehoused.
		return nil, pending
	}
	return rec, nil
}

// Run parses an NDJSON byte slice and splits it into backfilled canonical
// records and a pending list. lineNo is 1-based and counts non-empty lines,
// matching one JSON value per line.
func Run(ndjson []byte, mappings map[string]VersionMapping) ([]CanonicalRecord, []PendingItem, *Summary, error) {
	pj, err := simdjson.ParseND(ndjson, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parse NDJSON: %w", err)
	}

	records := make([]CanonicalRecord, 0)
	pending := make([]PendingItem, 0)
	summary := newSummary()

	// Map each parsed root (one JSON value per non-empty line) to its
	// physical 1-based line number, so blank lines still occupy a number.
	nonEmptyLines := make([]int64, 0, 256)
	var lineNo int64
	for i := 0; i < len(ndjson); i++ {
		lineNo++
		start := i
		for i < len(ndjson) && ndjson[i] != '\n' {
			i++
		}
		if i > start { // non-empty line
			nonEmptyLines = append(nonEmptyLines, lineNo)
		}
	}

	idx := 0
	err = pj.ForEach(func(iter simdjson.Iter) error {
		var thisLine int64
		var rawLine string
		if idx < len(nonEmptyLines) {
			thisLine = nonEmptyLines[idx]
			rawLine = rawLineOf(ndjson, thisLine)
		} else {
			thisLine = int64(idx + 1)
		}
		idx++

		summary.TotalLines++
		rec, pend := ProcessRow(iter, thisLine, rawLine, mappings)
		switch {
		case rec != nil:
			records = append(records, *rec)
			summary.Backfilled++
			summary.ByVersion[rec.SourceVersion]++
			summary.Conflicts += len(rec.Conflicts)
		default:
			pending = append(pending, *pend)
			summary.Pending++
			summary.PendingReason[pend.Reason]++
			summary.ByVersion[VersionUnknown]++
		}
		return nil
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("iterate NDJSON: %w", err)
	}
	return records, pending, summary, nil
}

// rawLineOf returns the raw bytes of the given 1-based physical line,
// excluding its trailing newline.
func rawLineOf(ndjson []byte, lineNo int64) string {
	start := 0
	var cur int64 = 1
	for i := 0; i < len(ndjson); i++ {
		if ndjson[i] != '\n' {
			continue
		}
		if cur == lineNo {
			return string(ndjson[start:i])
		}
		cur++
		start = i + 1
	}
	if cur == lineNo {
		return string(ndjson[start:])
	}
	return ""
}

func formatFloat(f float64) string {
	return fmt.Sprintf("%g", f)
}
