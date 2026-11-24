package logpart

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func mustLocation(t *testing.T, spec string) *time.Location {
	t.Helper()
	loc, err := LoadLocation(spec)
	if err != nil {
		t.Fatalf("LoadLocation(%q): %v", spec, err)
	}
	return loc
}

func TestLoadLocation(t *testing.T) {
	for _, spec := range []string{"UTC", "Asia/Shanghai", "America/New_York", "Local", "+08:00", "-0530", "+08"} {
		if _, err := LoadLocation(spec); err != nil {
			t.Errorf("LoadLocation(%q): %v", spec, err)
		}
	}
	loc := mustLocation(t, "+08:00")
	if name, off := time.Now().In(loc).Zone(); off != 8*3600 || name != "+08:00" {
		t.Errorf("+08:00 zone = %q/%d", name, off)
	}
	loc = mustLocation(t, "-0530")
	if _, off := time.Now().In(loc).Zone(); off != -(5*3600 + 30*60) {
		t.Errorf("-0530 offset = %d", off)
	}
	if _, err := LoadLocation("not/a-zone"); err == nil {
		t.Error("LoadLocation(not/a-zone) succeeded")
	}
	if _, err := LoadLocation(""); err == nil {
		t.Error("LoadLocation(\"\") succeeded")
	}
}

func TestParseClock(t *testing.T) {
	sh := mustLocation(t, "Asia/Shanghai")
	type want struct {
		event string // RFC3339 in target zone
		naive bool
	}
	cases := []struct {
		clock string
		want  want
	}{
		// Offset-aware: converted into the target zone.
		{"2026-11-23T10:00:00+08:00", want{"2026-11-23T10:00:00+08:00", false}},
		{"2026-11-23T10:00:00Z", want{"2026-11-23T18:00:00+08:00", false}},
		{"2026-11-23T23:30:00-05:00", want{"2026-11-24T12:30:00+08:00", false}}, // crosses midnight
		{"2026-11-23T10:00:00.123456789Z", want{"2026-11-23T18:00:00.123456789+08:00", false}},
		{"2026-11-23T10:00:00+0800", want{"2026-11-23T10:00:00+08:00", false}},
		{"2026-11-23 10:00:00+08:00", want{"2026-11-23T10:00:00+08:00", false}},
		// Timezone-less: read as wall time in the target zone.
		{"2026-11-23T10:00:00", want{"2026-11-23T10:00:00+08:00", true}},
		{"2026-11-23 10:00:00", want{"2026-11-23T10:00:00+08:00", true}},
		{"2026-11-23 10:00:00.5", want{"2026-11-23T10:00:00.5+08:00", true}},
		{"2026/11/23 10:00:00", want{"2026-11-23T10:00:00+08:00", true}},
		{"2026-11-23", want{"2026-11-23T00:00:00+08:00", true}},
	}
	for _, c := range cases {
		got, naive, err := ParseClock(c.clock, sh)
		if err != nil {
			t.Errorf("ParseClock(%q): %v", c.clock, err)
			continue
		}
		if gs := got.In(sh).Format(time.RFC3339Nano); gs != c.want.event || naive != c.want.naive {
			t.Errorf("ParseClock(%q) = %s naive=%v, want %s naive=%v", c.clock, gs, naive, c.want.event, c.want.naive)
		}
	}

	// Epochs are absolute instants.
	epoch := time.Date(2026, 11, 24, 12, 0, 0, 0, sh).Unix()
	got, naive, err := ParseClock(fmt.Sprint(epoch), sh)
	if err != nil || naive || !got.Equal(time.Unix(epoch, 0)) {
		t.Errorf("epoch seconds = %v naive=%v err=%v", got, naive, err)
	}
	got, _, err = ParseClock(fmt.Sprint(epoch*1000+123), sh)
	if err != nil || !got.Equal(time.UnixMilli(epoch*1000+123)) {
		t.Errorf("epoch millis = %v err=%v", got, err)
	}
	got, _, err = ParseClock(fmt.Sprintf("%d.5", epoch), sh)
	if err != nil || !got.Equal(time.Unix(epoch, 5e8)) {
		t.Errorf("epoch fraction = %v err=%v", got, err)
	}
}

func TestParseClockInvalid(t *testing.T) {
	sh := mustLocation(t, "Asia/Shanghai")
	for _, clock := range []string{
		"", "   ", "null", "not-a-clock", "2026-13-01T00:00:00Z", // month 13
		"2026-02-30T00:00:00Z",      // Feb 30
		"2026-11-23T25:00:00Z",      // hour 25
		"2026-11-23T10:60:00Z",      // minute 60
		"2026-11-23T10:00:00+25:00", // offset 25h
		"2026-11-23T10:00",          // too short for any layout
		"12345",                     // not an epoch length
	} {
		if got, _, err := ParseClock(clock, sh); err == nil {
			t.Errorf("ParseClock(%q) = %v, want error", clock, got)
		}
	}
}

func TestExtractClock(t *testing.T) {
	cases := []struct {
		line  string
		clock string
		ok    bool
	}{
		{`{"event_time":"2026-11-24T02:00:00+08:00","msg":"x"}`, "2026-11-24T02:00:00+08:00", true},
		{`{"@timestamp":"2026-11-24T02:00:00Z"}`, "2026-11-24T02:00:00Z", true},
		{`{"ts":1732400000}`, "1732400000", true},
		{`{"ts":null}`, "null", true},
		{`{"msg":"no time"}`, "", false},
		{`{broken json`, "", false},
		{"2026-11-23T10:00:00Z INFO x", "2026-11-23T10:00:00Z", true},
		{"2026-11-23 10:00:00 INFO x", "2026-11-23 10:00:00", true},
		{"[2026-11-23T10:00:00Z] INFO x", "2026-11-23T10:00:00Z", true},
		{"2026-11-23 INFO date-only", "2026-11-23", true},
		{"2026-11-23T10:00:00Z", "2026-11-23T10:00:00Z", true},
		{"2026-13-40T99:99:99Z bad clock", "2026-13-40T99:99:99Z", true}, // clock-shaped, for repair
		{"no clock here", "", false},
		{"", "", false},
	}
	for _, c := range cases {
		clock, ok := ExtractClock(c.line, nil)
		if clock != c.clock || ok != c.ok {
			t.Errorf("ExtractClock(%q) = %q,%v; want %q,%v", c.line, clock, ok, c.clock, c.ok)
		}
	}
	// Custom field list takes precedence.
	clock, ok := ExtractClock(`{"mytime":"2026-11-24T02:00:00Z"}`, []string{"mytime"})
	if !ok || clock != "2026-11-24T02:00:00Z" {
		t.Errorf("custom field = %q,%v", clock, ok)
	}
}

func TestSourceDayFromName(t *testing.T) {
	for _, c := range []struct {
		name string
		day  string
		ok   bool
	}{
		{"2026-11-24.log", "2026-11-24", true},
		{"2026-11-24", "2026-11-24", true},
		{"2026-11-24.ndjson.gz", "2026-11-24", true},
		{"/var/log/app/2026-11-24.log", "2026-11-24", true},
		{"app.log", "", false},
		{"2026-13-01.log", "", false},
		{"2026-11-24.log.bak", "2026-11-24", true}, // extensions are ignored
		{"2026-11-24.v2.log", "2026-11-24", true},  // rotation/version suffixes too
		{"2026-11-24extra.log", "", false},         // trailing junk: not a pure day
	} {
		day, ok := SourceDayFromName(c.name)
		if day != c.day || ok != c.ok {
			t.Errorf("SourceDayFromName(%q) = %q,%v; want %q,%v", c.name, day, ok, c.day, c.ok)
		}
	}
}

// TestRepartitionEndToEnd streams two source-day files — each ending with
// late records from the previous business day — and checks source order,
// original clocks, target days and the repair column.
func TestRepartitionEndToEnd(t *testing.T) {
	loc := mustLocation(t, "Asia/Shanghai")
	dir := t.TempDir()
	epoch := time.Date(2026, 11, 24, 12, 0, 0, 0, loc).Unix()

	files := map[string][]string{
		"2026-11-23.log": {
			"2026-11-23T00:12:01+08:00 INFO boot",
			"2026-11-23T09:00:00+08:00 INFO a",
			"2026-11-23T23:59:59+08:00 INFO late-night",
			"2026-11-22T23:58:10+08:00 INFO arrived-late-1", // late tail: previous business day
			"2026-11-22T23:59:55+08:00 INFO arrived-late-2",
		},
		"2026-11-24.log": {
			"2026-11-24T08:00:00+08:00 INFO morning",
			"2026-11-24 03:00:00 INFO naive",
			"2026-11-23T23:58:00+08:00 INFO arrived-late", // late: belongs to 2026-11-23
			"no-clock-at-all",
			`{"event_time":"2026-11-24T02:00:00+08:00","msg":"json ok"}`,
			`{"event_time":"not-a-clock","msg":"json bad"}`,
			"2026-11-24T16:00:00-05:00 INFO cross-midnight", // = 2026-11-25T05:00+08:00
			fmt.Sprintf(`{"ts":%d}`, epoch),
			"", // blank line: skipped
		},
	}
	var wantOrder []string
	for _, name := range []string{"2026-11-23.log", "2026-11-24.log"} {
		lines := files[name]
		body := strings.Join(lines, "\n") + "\n"
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		for _, l := range lines {
			if strings.TrimSpace(l) != "" {
				wantOrder = append(wantOrder, l)
			}
		}
	}

	rep := New(loc, nil)
	var recs []Record
	emit := func(r Record) error { recs = append(recs, r); return nil }
	for _, name := range []string{"2026-11-23.log", "2026-11-24.log"} {
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		day, ok := SourceDayFromName(name)
		if !ok {
			t.Fatalf("SourceDayFromName(%q) failed", name)
		}
		if err := rep.Process(context.Background(), f, day, emit); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}

	if len(recs) != len(wantOrder) {
		t.Fatalf("got %d records, want %d", len(recs), len(wantOrder))
	}
	// Source order: Seq is 0..n-1 and Raw replays the input exactly.
	for i, r := range recs {
		if r.Seq != int64(i) {
			t.Errorf("recs[%d].Seq = %d", i, r.Seq)
		}
		if r.Raw != wantOrder[i] {
			t.Errorf("recs[%d].Raw = %q, want %q", i, r.Raw, wantOrder[i])
		}
	}

	type exp struct {
		day, event, invalid string
		late, naive         bool
		source              string
		line                int
	}
	want := []exp{
		{day: "2026-11-23", event: "2026-11-23T00:12:01+08:00", source: "2026-11-23", line: 1},
		{day: "2026-11-23", event: "2026-11-23T09:00:00+08:00", source: "2026-11-23", line: 2},
		{day: "2026-11-23", event: "2026-11-23T23:59:59+08:00", source: "2026-11-23", line: 3},
		{day: "2026-11-22", event: "2026-11-22T23:58:10+08:00", late: true, source: "2026-11-23", line: 4},
		{day: "2026-11-22", event: "2026-11-22T23:59:55+08:00", late: true, source: "2026-11-23", line: 5},
		{day: "2026-11-24", event: "2026-11-24T08:00:00+08:00", source: "2026-11-24", line: 1},
		{day: "2026-11-24", event: "2026-11-24T03:00:00+08:00", naive: true, source: "2026-11-24", line: 2},
		{day: "2026-11-23", event: "2026-11-23T23:58:00+08:00", late: true, source: "2026-11-24", line: 3},
		{invalid: "no-clock-at-all", source: "2026-11-24", line: 4},
		{day: "2026-11-24", event: "2026-11-24T02:00:00+08:00", source: "2026-11-24", line: 5},
		{invalid: "not-a-clock", source: "2026-11-24", line: 6},
		{day: "2026-11-25", event: "2026-11-25T05:00:00+08:00", source: "2026-11-24", line: 7}, // cross-midnight, not late
		{day: "2026-11-24", event: "2026-11-24T12:00:00+08:00", source: "2026-11-24", line: 8},
	}
	for i, w := range want {
		r := recs[i]
		if r.Day != w.day || r.Event != w.event || r.InvalidClock != w.invalid ||
			r.Late != w.late || r.Naive != w.naive || r.Source != w.source || r.Line != w.line {
			t.Errorf("recs[%d] = %+v, want day=%q event=%q invalid=%q late=%v naive=%v source=%q line=%d",
				i, r, w.day, w.event, w.invalid, w.late, w.naive, w.source, w.line)
		}
		// Exactly one of Day / InvalidClock is set.
		if (r.Day == "") == (r.InvalidClock == "") {
			t.Errorf("recs[%d]: day=%q invalid_clock=%q, want exactly one set", i, r.Day, r.InvalidClock)
		}
	}
	// The original clock text is preserved verbatim on every record.
	if recs[6].Clock != "2026-11-24 03:00:00" {
		t.Errorf("naive clock = %q", recs[6].Clock)
	}
	if recs[9].Clock != "2026-11-24T02:00:00+08:00" {
		t.Errorf("json clock = %q", recs[9].Clock)
	}
}

// TestPartitionRoundTrip writes the streamed records into a partition tree
// and loads them back: cross-midnight, timezone-less and late records must
// each land in — and only in — their event-day partition.
func TestPartitionRoundTrip(t *testing.T) {
	loc := mustLocation(t, "Asia/Shanghai")
	dir := t.TempDir()
	input := strings.Join([]string{
		"2026-11-24T08:00:00+08:00 INFO morning",
		"2026-11-24 03:00:00 INFO naive",
		"2026-11-23T23:58:00+08:00 INFO arrived-late",
		"2026-11-24T16:00:00-05:00 INFO cross-midnight",
		"garbage-clock",
	}, "\n") + "\n"

	rep := New(loc, nil)
	var recs []Record
	emit := func(r Record) error { recs = append(recs, r); return nil }
	if err := rep.Process(context.Background(), strings.NewReader(input), "2026-11-24", emit); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(dir, "out")
	pw := NewPartitionWriter(out)
	for _, r := range recs {
		if err := pw.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}

	parts, err := ListPartitions(out)
	if err != nil {
		t.Fatal(err)
	}
	wantParts := []string{"2026-11-23", "2026-11-24", "2026-11-25", RepairPartition}
	if !reflect.DeepEqual(parts, wantParts) {
		t.Errorf("partitions = %v, want %v", parts, wantParts)
	}

	// Each partition holds exactly its own records, in source order.
	wantSeqs := map[string][]int64{
		"2026-11-23":    {2}, // the late record, not its source day
		"2026-11-24":    {0, 1},
		"2026-11-25":    {3}, // cross-midnight record
		RepairPartition: {4},
	}
	total := 0
	for day, seqs := range wantSeqs {
		got, err := LoadPartition(out, day)
		if err != nil {
			t.Fatal(err)
		}
		var gotSeqs []int64
		for _, r := range got {
			gotSeqs = append(gotSeqs, r.Seq)
			if r.Partition() != day {
				t.Errorf("record %+v loaded from wrong partition %q", r, day)
			}
			total++
		}
		if !reflect.DeepEqual(gotSeqs, seqs) {
			t.Errorf("partition %s seqs = %v, want %v", day, gotSeqs, seqs)
		}
	}
	if total != len(recs) {
		t.Errorf("loaded %d records, want %d", total, len(recs))
	}

	// The late record is distinguishable from on-time records of its day.
	late, err := LoadPartition(out, "2026-11-23")
	if err != nil || len(late) != 1 {
		t.Fatalf("late partition: %v %v", late, err)
	}
	if !late[0].Late || late[0].Source != "2026-11-24" {
		t.Errorf("late record not flagged: %+v", late[0])
	}
	// The timezone-less record is distinguishable via Naive.
	day24, _ := LoadPartition(out, "2026-11-24")
	if len(day24) != 2 || !day24[1].Naive || day24[0].Naive {
		t.Errorf("naive flagging wrong: %+v", day24)
	}
	// A partition that does not exist loads as empty.
	if got, err := LoadPartition(out, "2026-11-22"); err != nil || len(got) != 0 {
		t.Errorf("missing partition = %v, %v", got, err)
	}
}

// TestNaiveDST ensures timezone-less values near DST transitions parse
// deterministically and keep their calendar day.
func TestNaiveDST(t *testing.T) {
	ny := mustLocation(t, "America/New_York")
	for _, c := range []struct{ clock, day string }{
		{"2026-03-08T02:30:00", "2026-03-08"}, // spring-forward gap
		{"2026-11-01T01:30:00", "2026-11-01"}, // fall-back overlap
	} {
		got, naive, err := ParseClock(c.clock, ny)
		if err != nil {
			t.Fatalf("ParseClock(%q): %v", c.clock, err)
		}
		if !naive {
			t.Errorf("ParseClock(%q) naive=false", c.clock)
		}
		if d := got.In(ny).Format(DayLayout); d != c.day {
			t.Errorf("ParseClock(%q) day = %s, want %s", c.clock, d, c.day)
		}
	}
}

func TestProcessSkipsBlankAndCRLF(t *testing.T) {
	rep := New(time.UTC, nil)
	var recs []Record
	emit := func(r Record) error { recs = append(recs, r); return nil }
	input := "2026-11-23T10:00:00Z one\r\n\r\n   \n2026-11-23T11:00:00Z two" // no trailing newline
	if err := rep.Process(context.Background(), strings.NewReader(input), "2026-11-23", emit); err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
	if recs[0].Line != 1 || recs[1].Line != 4 {
		t.Errorf("lines = %d,%d; want 1,4", recs[0].Line, recs[1].Line)
	}
	if recs[0].Raw != "2026-11-23T10:00:00Z one" {
		t.Errorf("raw kept CR: %q", recs[0].Raw)
	}
}

func TestLateFlagNeedsDaySource(t *testing.T) {
	rep := New(time.UTC, nil)
	var recs []Record
	emit := func(r Record) error { recs = append(recs, r); return nil }
	// Source label is not a day (e.g. stdin): late detection is off.
	if err := rep.Process(context.Background(), strings.NewReader("2026-11-22T10:00:00Z x\n"), "stdin", emit); err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].Late {
		t.Errorf("late flag with non-day source: %+v", recs)
	}
}

func TestProcessContextCancel(t *testing.T) {
	rep := New(time.UTC, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := rep.Process(ctx, strings.NewReader("2026-11-23T10:00:00Z x\n"), "2026-11-23", func(Record) error { return nil })
	if err == nil {
		t.Error("canceled context: got nil error")
	}
}

func TestEmitErrorPropagates(t *testing.T) {
	rep := New(time.UTC, nil)
	boom := fmt.Errorf("boom")
	err := rep.Process(context.Background(), strings.NewReader("2026-11-23T10:00:00Z x\n"), "2026-11-23", func(Record) error { return boom })
	if err != boom {
		t.Errorf("got %v, want boom", err)
	}
}
