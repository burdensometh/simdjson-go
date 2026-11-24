// Package logpart repartitions day-organized logs into event-time partitions.
//
// Logs are stored one file per source day, and each file may end with
// late-arriving records that belong to a previous business day. logpart
// streams such logs line by line, extracts each line's event clock, converts
// it into a configured target timezone and emits one Record per line — in the
// original source order — carrying the source order (Seq), the original clock
// text (Clock/Raw) and the target partition day (Day).
//
// Lines whose clock is missing or invalid are not dropped: the offending
// value is isolated in the Record.InvalidClock column so a partition loader
// can route them to the repair partition (RepairPartition) instead of a day
// partition and fix them up later.
//
// Three easily confused kinds of values stay distinguishable for partition
// loading:
//
//   - cross-midnight values: an offset-aware clock near midnight may move to
//     a different day once converted into the target timezone; the partition
//     key always comes from the converted event time, never the file name.
//   - timezone-less (naive) values: interpreted as local wall time in the
//     target timezone and flagged with Record.Naive.
//   - late values: records whose target day is earlier than the source file
//     day (e.g. previous-business-day records appended at the end of a file)
//     are flagged with Record.Late and still land in their event-day
//     partition.
package logpart

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// DayLayout is the target partition key layout: the calendar day of the
// event time in the target timezone.
const DayLayout = "2006-01-02"

// RepairPartition is the partition key used for records whose clock is
// missing or invalid. It sorts apart from real days and is not a valid
// DayLayout value, so a partition loader can never confuse it with one.
const RepairPartition = "__repair__"

// DefaultFields are the JSON object keys probed for the event clock, in
// order, when a log line is a JSON object.
var DefaultFields = []string{"event_time", "@timestamp", "timestamp", "time", "ts", "datetime", "date"}

// Record is one streamed output row, emitted per non-blank input line in
// source order.
type Record struct {
	Seq    int64  `json:"seq"`              // global source order, 0-based, monotonic across inputs
	Source string `json:"source,omitempty"` // source day (e.g. "2026-11-24") or source label
	Line   int    `json:"line"`             // 1-based physical line number within the source
	Clock  string `json:"clock"`            // original clock text as extracted, verbatim

	Event string `json:"event,omitempty"` // normalized event instant in the target zone (RFC3339)
	Day   string `json:"day,omitempty"`   // target partition key: event day in the target zone

	// InvalidClock is the repair column: it carries the clock text that
	// could not be parsed (or the whole line when no clock was found).
	// Exactly one of Day and InvalidClock is set.
	InvalidClock string `json:"invalid_clock,omitempty"`

	Late  bool `json:"late,omitempty"`  // target day is earlier than the source day
	Naive bool `json:"naive,omitempty"` // clock carried no timezone; read in the target zone

	Raw string `json:"raw"` // the original log line, untrimmed of content
}

// Valid reports whether the record's clock parsed and a target Day was
// assigned.
func (r Record) Valid() bool { return r.Day != "" }

// Partition returns the partition key the record loads into: its target Day,
// or RepairPartition when the clock was invalid.
func (r Record) Partition() string {
	if r.Day != "" {
		return r.Day
	}
	return RepairPartition
}

// clockLayout is a candidate timestamp layout. naive layouts carry no zone
// and are parsed as wall time in the target location.
type clockLayout struct {
	layout string
	naive  bool
}

var clockLayouts = []clockLayout{
	{time.RFC3339Nano, false},                      // 2006-01-02T15:04:05[.frac]Z07:00
	{"2006-01-02T15:04:05.999999999Z0700", false},  // zone without colon: +0800
	{"2006-01-02 15:04:05.999999999Z07:00", false}, // space separator, aware
	{"2006-01-02 15:04:05.999999999Z0700", false},
	{"2006-01-02T15:04:05.999999999", true}, // naive ISO
	{"2006-01-02 15:04:05.999999999", true}, // naive, space separator
	{"2006/01/02 15:04:05.999999999", true}, // naive, slash date
	{"2006-01-02", true},                    // date only: midnight in the target zone
	{"2006/01/02", true},
}

// ParseClock parses clock text into an instant.
//
// Offset-aware values keep their own offset. Timezone-less values are read as
// local wall time in loc and reported via naive=true. Bare numbers are Unix
// epochs (10/13/16/19 digits: s/ms/µs/ns, fraction allowed) and are absolute
// instants. loc must not be nil.
func ParseClock(clock string, loc *time.Location) (t time.Time, naive bool, err error) {
	s := strings.TrimSpace(clock)
	if s == "" {
		return time.Time{}, false, errors.New("logpart: empty clock")
	}
	if t, err := parseEpochNumber(s); err == nil {
		return t, false, nil
	}
	for _, l := range clockLayouts {
		var pt time.Time
		var perr error
		if l.naive {
			pt, perr = time.ParseInLocation(l.layout, s, loc)
		} else {
			pt, perr = time.Parse(l.layout, s)
		}
		if perr == nil {
			return pt, l.naive, nil
		}
	}
	return time.Time{}, false, fmt.Errorf("logpart: unparseable clock %q", clock)
}

// parseEpochNumber parses a bare Unix epoch: an integer of 10/13/16/19 digits
// (s/ms/µs/ns) with an optional sign and fractional part.
func parseEpochNumber(s string) (time.Time, error) {
	body := s
	neg := false
	if strings.HasPrefix(body, "-") || strings.HasPrefix(body, "+") {
		neg = body[0] == '-'
		body = body[1:]
	}
	ip, fp, _ := strings.Cut(body, ".")
	if !isDigits(ip) {
		return time.Time{}, fmt.Errorf("logpart: %q is not an epoch", s)
	}
	var unit time.Duration
	switch len(ip) {
	case 10:
		unit = time.Second
	case 13:
		unit = time.Millisecond
	case 16:
		unit = time.Microsecond
	case 19:
		unit = time.Nanosecond
	default:
		return time.Time{}, fmt.Errorf("logpart: %q is not an epoch", s)
	}
	whole, err := strconv.ParseInt(ip, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	var fracNanos int64
	if fp != "" {
		if !isDigits(fp) {
			return time.Time{}, fmt.Errorf("logpart: %q is not an epoch", s)
		}
		if len(fp) > 9 {
			fp = fp[:9]
		}
		f, err := strconv.ParseInt(fp, 10, 64)
		if err != nil {
			return time.Time{}, err
		}
		// f is a fraction of one unit; scale it to nanoseconds.
		fracNanos = f * int64(unit) / pow10(int64(len(fp)))
	}
	if neg {
		whole, fracNanos = -whole, -fracNanos
	}
	var t time.Time
	switch unit {
	case time.Second:
		t = time.Unix(whole, 0)
	case time.Millisecond:
		t = time.UnixMilli(whole)
	case time.Microsecond:
		t = time.UnixMicro(whole)
	case time.Nanosecond:
		t = time.Unix(0, whole)
	}
	return t.Add(time.Duration(fracNanos)).UTC(), nil
}

func pow10(n int64) int64 {
	p := int64(1)
	for ; n > 0; n-- {
		p *= 10
	}
	return p
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// ExtractClock finds the event clock text on a log line. It reports ok=true
// when a clock-shaped value was found; the returned text may still fail
// ParseClock, in which case it belongs in the repair column. fields are the
// JSON keys probed for JSON lines; nil selects DefaultFields.
func ExtractClock(line string, fields []string) (clock string, ok bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return "", false
	}
	if strings.HasPrefix(trimmed, "{") {
		var m map[string]any
		if err := json.Unmarshal([]byte(trimmed), &m); err == nil {
			if len(fields) == 0 {
				fields = DefaultFields
			}
			for _, f := range fields {
				v, found := m[f]
				if !found {
					continue
				}
				switch tv := v.(type) {
				case string:
					return strings.TrimSpace(tv), true
				case float64:
					return strconv.FormatFloat(tv, 'f', -1, 64), true
				case nil:
					return "null", true
				case bool:
					return strconv.FormatBool(tv), true
				default:
					return fmt.Sprint(tv), true
				}
			}
			return "", false // JSON object without a known clock field
		}
		// Broken JSON: fall through to text probing.
	}
	cands := textCandidates(trimmed)
	for _, c := range cands {
		if _, _, err := ParseClock(c, time.UTC); err == nil {
			return c, true
		}
	}
	// Nothing parseable: surface the leading clock-shaped token (or a
	// "date time" token pair) so the repair column points at the offending
	// value instead of the whole line.
	toks := strings.Fields(trimmed)
	clean := func(s string) string {
		s = strings.Trim(s, "[]")
		return strings.TrimRight(s, ",;")
	}
	if len(toks) > 1 {
		if a, b := clean(toks[0]), clean(toks[1]); looksLikeClock(a) && strings.Contains(b, ":") {
			return a + " " + b, true
		}
	}
	if len(toks) > 0 {
		if one := clean(toks[0]); looksLikeClock(one) {
			return one, true
		}
	}
	return "", false
}

// textCandidates builds clock candidates from a plain-text line: the whole
// line, then the first two whitespace-separated tokens (for "date time"
// clocks), then the first token. The two-token form is tried before the
// one-token form so a date-only parse cannot swallow a following time.
func textCandidates(line string) []string {
	toks := strings.Fields(line)
	cands := make([]string, 0, 3)
	add := func(s string) {
		s = strings.Trim(s, "[]")
		s = strings.TrimRight(s, ",;")
		if s != "" {
			cands = append(cands, s)
		}
	}
	add(line)
	if len(toks) > 1 {
		add(toks[0] + " " + toks[1])
	}
	if len(toks) > 0 {
		add(toks[0])
	}
	return cands
}

// looksLikeClock reports whether s has the rough shape of a calendar clock
// (yyyy-... / yyyy/... ) or an epoch, i.e. worth isolating for repair.
func looksLikeClock(s string) bool {
	if len(s) >= 8 && isDigits(s[:4]) && (s[4] == '-' || s[4] == '/') {
		return true
	}
	return isDigits(s) && (len(s) == 10 || len(s) == 13 || len(s) == 16 || len(s) == 19)
}

// Repartitioner streams day-organized logs and emits one Record per line, in
// source order, keyed by event day in the target timezone.
type Repartitioner struct {
	loc    *time.Location
	fields []string
	seq    int64
}

// New returns a Repartitioner assigning partition days in loc (UTC when nil).
// fields are the JSON clock fields probed; nil selects DefaultFields.
func New(loc *time.Location, fields []string) *Repartitioner {
	if loc == nil {
		loc = time.UTC
	}
	if len(fields) == 0 {
		fields = DefaultFields
	}
	return &Repartitioner{loc: loc, fields: fields}
}

// Seq returns the number of records emitted so far, i.e. the next Seq value.
func (r *Repartitioner) Seq() int64 { return r.seq }

// Process streams lines from rd, calling emit once per non-blank line in
// source order. source is the source-day label (a DayLayout day when the
// input is a day-named file); it is recorded on each Record and used for late
// detection. Seq keeps increasing across calls, so processing a sequence of
// files preserves global source order. Blank lines are skipped.
func (r *Repartitioner) Process(ctx context.Context, rd io.Reader, source string, emit func(Record) error) error {
	srcDay := ""
	if t, err := time.Parse(DayLayout, source); err == nil {
		srcDay = t.Format(DayLayout)
	}
	br := bufio.NewReaderSize(rd, 1<<20)
	lineNo := 0
	for {
		if lineNo%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		line, err := br.ReadString('\n')
		if len(line) > 0 {
			line = strings.TrimSuffix(line, "\n")
			line = strings.TrimSuffix(line, "\r")
			lineNo++
			if strings.TrimSpace(line) != "" {
				if eerr := emit(r.classify(line, source, srcDay, lineNo)); eerr != nil {
					return eerr
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

// classify builds the Record for one log line.
func (r *Repartitioner) classify(raw, source, srcDay string, lineNo int) Record {
	rec := Record{Seq: r.seq, Source: source, Line: lineNo, Raw: raw}
	r.seq++

	clock, ok := ExtractClock(raw, r.fields)
	rec.Clock = clock
	if !ok {
		// No clock-shaped value anywhere: isolate the line itself for repair.
		rec.InvalidClock = strings.TrimSpace(raw)
		return rec
	}
	t, naive, err := ParseClock(clock, r.loc)
	if err != nil {
		rec.InvalidClock = clock
		return rec
	}
	lt := t.In(r.loc)
	rec.Event = lt.Format(time.RFC3339Nano)
	rec.Day = lt.Format(DayLayout)
	rec.Naive = naive
	rec.Late = srcDay != "" && rec.Day < srcDay
	return rec
}

// SourceDayFromName extracts a YYYY-MM-DD source day from a log file name
// such as "2026-11-24.log" or "2026-11-24.ndjson.gz". It reports ok=false
// when the base name (minus extensions) is not a valid day.
func SourceDayFromName(name string) (day string, ok bool) {
	base := filepath.Base(name)
	for {
		ext := filepath.Ext(base)
		if ext == "" {
			break
		}
		base = strings.TrimSuffix(base, ext)
	}
	if _, err := time.Parse(DayLayout, base); err == nil {
		return base, true
	}
	return "", false
}

// LoadLocation resolves a timezone spec: an IANA name ("Asia/Shanghai"),
// "UTC", "Local", or a fixed numeric offset ("+08:00", "-0530", "+08").
func LoadLocation(spec string) (*time.Location, error) {
	if spec == "" {
		return nil, errors.New("logpart: empty timezone spec")
	}
	if loc, err := time.LoadLocation(spec); err == nil {
		return loc, nil
	}
	for _, layout := range []string{"-07:00", "-0700", "-07"} {
		if t, err := time.Parse(layout, spec); err == nil {
			_, off := t.Zone()
			return time.FixedZone(spec, off), nil
		}
	}
	return nil, fmt.Errorf("logpart: unknown timezone %q", spec)
}

// PartitionWriter writes records into a partition directory tree:
//
//	DIR/dt=<day>/part-00000.ndjson      one NDJSON Record per line
//	DIR/dt=__repair__/part-00000.ndjson records with invalid clocks
//
// Records are appended in arrival (source) order, so each partition file
// preserves the source order of the records routed to it.
type PartitionWriter struct {
	Dir string

	files   map[string]*os.File
	bufs    map[string]*bufio.Writer
	written map[string]int64
}

// NewPartitionWriter returns a PartitionWriter rooted at dir.
func NewPartitionWriter(dir string) *PartitionWriter {
	return &PartitionWriter{
		Dir:     dir,
		files:   make(map[string]*os.File),
		bufs:    make(map[string]*bufio.Writer),
		written: make(map[string]int64),
	}
}

// Write appends rec to its partition, creating the partition on first use.
func (w *PartitionWriter) Write(rec Record) error {
	part := rec.Partition()
	bw, ok := w.bufs[part]
	if !ok {
		dir := filepath.Join(w.Dir, "dt="+part)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		f, err := os.Create(filepath.Join(dir, "part-00000.ndjson"))
		if err != nil {
			return err
		}
		w.files[part] = f
		bw = bufio.NewWriter(f)
		w.bufs[part] = bw
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := bw.Write(b); err != nil {
		return err
	}
	if err := bw.WriteByte('\n'); err != nil {
		return err
	}
	w.written[part]++
	return nil
}

// Written reports how many records were routed to each partition key.
func (w *PartitionWriter) Written() map[string]int64 {
	out := make(map[string]int64, len(w.written))
	for k, v := range w.written {
		out[k] = v
	}
	return out
}

// Close flushes and closes all partition files.
func (w *PartitionWriter) Close() error {
	var first error
	for part, bw := range w.bufs {
		if err := bw.Flush(); err != nil && first == nil {
			first = err
		}
		if err := w.files[part].Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// LoadPartition reads back all records routed to the partition for day
// (use RepairPartition for the repair partition), in stored source order. A
// missing partition yields zero records and no error.
func LoadPartition(dir, day string) ([]Record, error) {
	pdir := filepath.Join(dir, "dt="+day)
	entries, err := os.ReadDir(pdir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []Record
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".ndjson") {
			continue
		}
		f, err := os.Open(filepath.Join(pdir, e.Name()))
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
		for sc.Scan() {
			if len(strings.TrimSpace(sc.Text())) == 0 {
				continue
			}
			var rec Record
			if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
				f.Close()
				return nil, fmt.Errorf("logpart: %s/%s: %w", pdir, e.Name(), err)
			}
			out = append(out, rec)
		}
		if err := sc.Err(); err != nil {
			f.Close()
			return nil, err
		}
		f.Close()
	}
	return out, nil
}

// ListPartitions returns the partition keys present under dir (the part
// after "dt="), sorted.
func ListPartitions(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "dt=") {
			out = append(out, strings.TrimPrefix(e.Name(), "dt="))
		}
	}
	return out, nil
}
