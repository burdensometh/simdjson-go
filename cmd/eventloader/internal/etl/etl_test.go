package etl

import (
	"testing"

	"github.com/minio/simdjson-go"
)

func parseOne(t *testing.T, line string) simdjson.Iter {
	t.Helper()
	pj, err := simdjson.ParseND([]byte(line+"\n"), nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	var got simdjson.Iter
	if err := pj.ForEach(func(i simdjson.Iter) error {
		got = i
		return nil
	}); err != nil {
		t.Fatalf("foreach: %v", err)
	}
	return got
}

func TestNormalizeVersion(t *testing.T) {
	cases := map[string]struct {
		raw   string
		want  string
		known bool
	}{
		"v1 tag":     {"v1", VersionV1, true},
		"v2 tag":     {"v2", VersionV2, true},
		"numeric 1":  {"1", VersionV1, true},
		"numeric 2":  {"2", VersionV2, true},
		"upper case": {"V2", VersionV2, true},
		"whitespace": {" v1 ", VersionV1, true},
		"unknown v3": {"v3", VersionUnknown, false},
		"empty":      {"", VersionUnknown, false},
		"garbage":    {"banana", VersionUnknown, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			v, known := normalizeVersion(tc.raw)
			if v != tc.want || known != tc.known {
				t.Fatalf("normalizeVersion(%q) = %q,%v want %q,%v", tc.raw, v, known, tc.want, tc.known)
			}
		})
	}
}

func TestV1UsesLegacyPaths(t *testing.T) {
	line := `{"schema_version":"v1","event":{"event_id":"E-1001","time":"2026-11-20T10:15:30Z"},"payment":{"amount":120.50}}`
	rec, pend := ProcessRow(parseOne(t, line), 1, line, DefaultMappings())
	if pend != nil {
		t.Fatalf("unexpected pending: %+v", pend)
	}
	if rec.SourceVersion != VersionV1 {
		t.Fatalf("version = %q, want v1", rec.SourceVersion)
	}
	if rec.EventID != "E-1001" || rec.BusinessTime != "2026-11-20T10:15:30Z" || rec.Amount != 120.50 {
		t.Fatalf("unexpected values: %+v", rec)
	}
	wantPaths := map[string]string{
		FieldEventID:      "$.event.event_id",
		FieldBusinessTime: "$.event.time",
		FieldAmount:       "$.payment.amount",
	}
	for f, want := range wantPaths {
		if rec.UsedPaths[f] != want {
			t.Errorf("used path for %s = %q, want %q", f, rec.UsedPaths[f], want)
		}
	}
	if len(rec.Conflicts) != 0 {
		t.Fatalf("unexpected conflicts: %+v", rec.Conflicts)
	}
}

func TestV2UsesFlattenedPaths(t *testing.T) {
	line := `{"schema_version":"v2","event_id":"E-2002","biz_time":"2026-11-20T11:20:45Z","amount":88}`
	rec, pend := ProcessRow(parseOne(t, line), 2, line, DefaultMappings())
	if pend != nil {
		t.Fatalf("unexpected pending: %+v", pend)
	}
	if rec.SourceVersion != VersionV2 {
		t.Fatalf("version = %q, want v2", rec.SourceVersion)
	}
	if rec.EventID != "E-2002" || rec.Amount != 88 || rec.AmountRaw != "88" {
		t.Fatalf("unexpected values: %+v", rec)
	}
	wantPaths := map[string]string{
		FieldEventID:      "$.event_id",
		FieldBusinessTime: "$.biz_time",
		FieldAmount:       "$.amount",
	}
	for f, want := range wantPaths {
		if rec.UsedPaths[f] != want {
			t.Errorf("used path for %s = %q, want %q", f, rec.UsedPaths[f], want)
		}
	}
}

func TestConflictV1RowKeepsLegacyValue(t *testing.T) {
	// v1 row carrying both legacy and new fields: active (v1) path must win.
	line := `{"schema_version":"v1","event":{"event_id":"E-1003","time":"2026-11-21T09:00:00Z"},"payment":{"amount":300},"amount":315.75,"biz_time":"2026-11-21T09:05:00Z"}`
	rec, pend := ProcessRow(parseOne(t, line), 3, line, DefaultMappings())
	if pend != nil {
		t.Fatalf("unexpected pending: %+v", pend)
	}
	if rec.Amount != 300 {
		t.Fatalf("amount = %v, want legacy-path value 300 (new-version-wins on a v1 row means mapped path wins)", rec.Amount)
	}
	if rec.BusinessTime != "2026-11-21T09:00:00Z" {
		t.Fatalf("business_time = %q, want legacy value", rec.BusinessTime)
	}
	if len(rec.Conflicts) != 2 {
		t.Fatalf("conflicts = %d, want 2 (amount + business_time): %+v", len(rec.Conflicts), rec.Conflicts)
	}
	for _, c := range rec.Conflicts {
		if c.Resolution != ConflictPolicy {
			t.Errorf("resolution = %q, want %q", c.Resolution, ConflictPolicy)
		}
		if c.ChosenPath != rec.UsedPaths[c.Field] {
			t.Errorf("chosen path %q not equal used path %q for %s", c.ChosenPath, rec.UsedPaths[c.Field], c.Field)
		}
	}
	// Shadow values must be retained for audit.
	byField := map[string]FieldConflict{}
	for _, c := range rec.Conflicts {
		byField[c.Field] = c
	}
	if byField[FieldAmount].ShadowValue != "315.75" || byField[FieldAmount].ShadowPath != "$.amount" {
		t.Errorf("amount shadow not recorded: %+v", byField[FieldAmount])
	}
}

func TestConflictV2RowKeepsFlattenedValue(t *testing.T) {
	line := `{"schema_version":"v2","event_id":"E-2004","biz_time":"2026-11-21T14:30:00Z","amount":42.10,"event":{"event_id":"E-2004-LEGACY","time":"2026-11-21T14:00:00Z"},"payment":{"amount":40.00}}`
	rec, pend := ProcessRow(parseOne(t, line), 4, line, DefaultMappings())
	if pend != nil {
		t.Fatalf("unexpected pending: %+v", pend)
	}
	if rec.Amount != 42.10 {
		t.Fatalf("amount = %v, want v2-path value 42.10", rec.Amount)
	}
	if rec.EventID != "E-2004" {
		t.Fatalf("event_id = %q, want v2 value E-2004", rec.EventID)
	}
	// event_id, business time (shadow differs), amount (shadow differs).
	if len(rec.Conflicts) != 3 {
		t.Fatalf("conflicts = %d, want 3: %+v", len(rec.Conflicts), rec.Conflicts)
	}
}

func TestConflictEqualValuesNotFlagged(t *testing.T) {
	// Same value under both paths: present in both but no divergence.
	line := `{"schema_version":"v2","event_id":"E-5","biz_time":"2026-11-21T14:30:00Z","amount":40,"payment":{"amount":40}}`
	rec, pend := ProcessRow(parseOne(t, line), 5, line, DefaultMappings())
	if pend != nil {
		t.Fatalf("unexpected pending: %+v", pend)
	}
	if len(rec.Conflicts) != 0 {
		t.Fatalf("equal old/new values must not conflict: %+v", rec.Conflicts)
	}
}

func TestUnknownVersionGoesPending(t *testing.T) {
	line := `{"event":{"event_id":"E-9005","time":"2026-11-22T08:00:00Z"},"payment":{"amount":9.99}}`
	rec, pend := ProcessRow(parseOne(t, line), 5, line, DefaultMappings())
	if rec != nil {
		t.Fatalf("unknown-version row must not produce a record: %+v", rec)
	}
	if pend.Reason != ReasonVersionUnknown {
		t.Fatalf("reason = %q, want %q", pend.Reason, ReasonVersionUnknown)
	}
	if pend.RawLine != line || pend.LineNo != 5 {
		t.Fatalf("pending must retain raw line and line number: %+v", pend)
	}

	lineV3 := `{"schema_version":"v3","amount":1}`
	_, pend3 := ProcessRow(parseOne(t, lineV3), 6, lineV3, DefaultMappings())
	if pend3 == nil || pend3.Reason != ReasonVersionUnknown || pend3.DetectedVersion != "v3" {
		t.Fatalf("v3 tag must be pending with raw tag: %+v", pend3)
	}
}

func TestMissingMappedFieldGoesPending(t *testing.T) {
	// v2 row without the mapped amount field, even though nothing else is wrong.
	line := `{"schema_version":"v2","event_id":"E-2008","biz_time":"2026-11-22T18:45:00Z"}`
	rec, pend := ProcessRow(parseOne(t, line), 8, line, DefaultMappings())
	if rec != nil {
		t.Fatalf("row missing a mapped canonical field must be pending: %+v", rec)
	}
	if pend.Reason != ReasonFieldMissing {
		t.Fatalf("reason = %q, want %q", pend.Reason, ReasonFieldMissing)
	}
	if !contains(pend.Detail, "$.amount") {
		t.Fatalf("detail must name the mapped path: %q", pend.Detail)
	}
}

func TestInvalidFieldGoesPending(t *testing.T) {
	// Unparseable business time; amount is a string-encoded number and is fine.
	line := `{"schema_version":"v1","event":{"event_id":"E-1007","time":"not-a-timestamp"},"payment":{"amount":"55.25"}}`
	rec, pend := ProcessRow(parseOne(t, line), 7, line, DefaultMappings())
	if rec != nil {
		t.Fatalf("invalid field must be pending: %+v", rec)
	}
	if pend.Reason != ReasonFieldInvalid || !contains(pend.Detail, "RFC3339") {
		t.Fatalf("pending = %+v, want invalid RFC3339 detail", pend)
	}
}

func TestStringEncodedAmount(t *testing.T) {
	line := `{"schema_version":1,"event":{"event_id":"E-1009","time":"2026-11-23T07:07:07Z"},"payment":{"amount":"77.7"}}`
	rec, pend := ProcessRow(parseOne(t, line), 9, line, DefaultMappings())
	if pend != nil {
		t.Fatalf("unexpected pending: %+v", pend)
	}
	if rec.Amount != 77.7 || rec.AmountRaw != "77.7" {
		t.Fatalf("amount = %v (raw %q), want 77.7", rec.Amount, rec.AmountRaw)
	}
	if rec.SourceVersion != VersionV1 {
		t.Fatalf("numeric schema_version 1 must normalize to v1")
	}
}

func TestRunEndToEndLineageAndCounts(t *testing.T) {
	ndjson := []byte("" +
		`{"schema_version":"v1","event":{"event_id":"A","time":"2026-11-20T10:00:00Z"},"payment":{"amount":1}}` + "\n" +
		"\n" + // blank line must not consume a line number
		`{"schema_version":"v2","event_id":"B","biz_time":"2026-11-20T11:00:00Z","amount":2}` + "\n" +
		`{"schema_version":"v9","amount":3}` + "\n",
	)
	records, pending, summary, err := Run(ndjson, DefaultMappings())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if summary.TotalLines != 3 || summary.Backfilled != 2 || summary.Pending != 1 {
		t.Fatalf("summary counts wrong: %+v", summary)
	}
	if summary.ByVersion[VersionV1] != 1 || summary.ByVersion[VersionV2] != 1 || summary.ByVersion[VersionUnknown] != 1 {
		t.Fatalf("by-version counts wrong: %+v", summary.ByVersion)
	}
	if summary.PendingReason[ReasonVersionUnknown] != 1 {
		t.Fatalf("pending reason counts wrong: %+v", summary.PendingReason)
	}
	if records[0].LineNo != 1 || records[1].LineNo != 3 {
		t.Fatalf("line numbers must skip blank line: %d, %d", records[0].LineNo, records[1].LineNo)
	}
	if pending[0].LineNo != 4 || pending[0].RawLine == "" {
		t.Fatalf("pending row must carry line number and raw line: %+v", pending[0])
	}
	// Every backfilled row states the actual paths used.
	for _, r := range records {
		if len(r.UsedPaths) != 3 {
			t.Fatalf("record missing used-path lineage: %+v", r)
		}
		if r.ConflictPolicy != ConflictPolicy {
			t.Fatalf("conflict policy must be stated on every record")
		}
	}
}

func TestRunRejectsMalformedJSON(t *testing.T) {
	if _, _, _, err := Run([]byte("{not json}\n"), DefaultMappings()); err == nil {
		t.Fatal("malformed NDJSON must return an error")
	}
}

func TestNumericVersionV2(t *testing.T) {
	line := `{"schema_version":2,"event_id":"N2","biz_time":"2026-11-23T07:07:07Z","amount":2}`
	rec, pend := ProcessRow(parseOne(t, line), 1, line, DefaultMappings())
	if pend != nil {
		t.Fatalf("unexpected pending: %+v", pend)
	}
	if rec.SourceVersion != VersionV2 || rec.UsedPaths[FieldAmount] != "$.amount" {
		t.Fatalf("numeric tag 2 must select v2 mapping: %+v", rec)
	}
}

func TestNonScalarVersionGoesPending(t *testing.T) {
	for _, tag := range []string{"true", "null", `{"x":1}`, `[1]`} {
		line := `{"schema_version":` + tag + `,"amount":1}`
		rec, pend := ProcessRow(parseOne(t, line), 1, line, DefaultMappings())
		if rec != nil || pend == nil || pend.Reason != ReasonVersionUnknown {
			t.Fatalf("tag %s must be pending as unknown version, got rec=%+v pend=%+v", tag, rec, pend)
		}
	}
}

func TestNullMappedFieldGoesPending(t *testing.T) {
	line := `{"schema_version":"v2","event_id":"E","biz_time":null,"amount":1}`
	rec, pend := ProcessRow(parseOne(t, line), 1, line, DefaultMappings())
	if rec != nil || pend == nil {
		t.Fatalf("null mapped field must be pending: rec=%+v pend=%+v", rec, pend)
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (indexOf(haystack, needle) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
