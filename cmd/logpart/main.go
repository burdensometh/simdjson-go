// Command logpart repartitions day-organized logs into event-time partitions.
//
// It streams log files (one per source day, possibly ending with
// late-arriving records from a previous business day), converts each line's
// event clock into the target timezone and emits one record per line — in
// source order — with the source order, the original clock and the target
// day. Invalid clocks are isolated in the invalid_clock column and routed to
// the __repair__ partition.
//
// Usage:
//
//	logpart [flags] [files-or-dirs...]
//
// With no input arguments, logpart reads stdin. Directories are expanded to
// their regular files in name order. Output defaults to an NDJSON stream on
// stdout; with -out-dir it is written as partitioned files
// DIR/dt=<day>/part-00000.ndjson (plus dt=__repair__/ for invalid clocks).
package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/minio/simdjson-go/logpart"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("logpart", flag.ContinueOnError)
	tz := fs.String("tz", "UTC", "target timezone: IANA name (e.g. Asia/Shanghai), UTC, Local, or fixed offset (+08:00)")
	fields := fs.String("fields", "", "comma-separated JSON object keys probed for the event clock (default: built-in list)")
	format := fs.String("format", "ndjson", "stream output format when not using -out-dir: ndjson or csv")
	outDir := fs.String("out-dir", "", "write partitioned files under DIR/dt=<day>/ instead of streaming to stdout")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	loc, err := logpart.LoadLocation(*tz)
	if err != nil {
		fmt.Fprintln(os.Stderr, "logpart:", err)
		return 2
	}
	var flds []string
	if *fields != "" {
		for _, f := range strings.Split(*fields, ",") {
			if f = strings.TrimSpace(f); f != "" {
				flds = append(flds, f)
			}
		}
	}
	rep := logpart.New(loc, flds)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	emit, flush, finish, err := makeEmitter(*outDir, *format)
	if err != nil {
		fmt.Fprintln(os.Stderr, "logpart:", err)
		return 2
	}

	inputs := fs.Args()
	if len(inputs) == 0 {
		inputs = []string{"-"}
	}
	status := 0
	for _, in := range inputs {
		files, err := expandInput(in)
		if err != nil {
			fmt.Fprintln(os.Stderr, "logpart:", err)
			status = 1
			continue
		}
		for _, f := range files {
			if err := processOne(ctx, rep, f, emit); err != nil {
				fmt.Fprintln(os.Stderr, "logpart:", err)
				status = 1
			}
			if err := flush(); err != nil {
				fmt.Fprintln(os.Stderr, "logpart:", err)
				status = 1
			}
		}
	}
	if err := finish(); err != nil {
		fmt.Fprintln(os.Stderr, "logpart:", err)
		status = 1
	}
	return status
}

// processOne streams a single input ("-" or a file path) through the
// repartitioner.
func processOne(ctx context.Context, rep *logpart.Repartitioner, path string, emit func(logpart.Record) error) error {
	var rc io.ReadCloser
	source := ""
	if path == "-" {
		rc = os.Stdin
	} else {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		rc = f
		if day, ok := logpart.SourceDayFromName(path); ok {
			source = day
		} else {
			source = filepath.Base(path)
		}
	}
	defer rc.Close()
	return rep.Process(ctx, rc, source, emit)
}

// expandInput resolves an input argument to a list of files. A directory
// expands to its regular files in name order; "-" (stdin) and plain files
// pass through.
func expandInput(in string) ([]string, error) {
	if in == "-" {
		return []string{"-"}, nil
	}
	st, err := os.Stat(in)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() {
		return []string{in}, nil
	}
	entries, err := os.ReadDir(in)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if e.Type().IsRegular() {
			files = append(files, filepath.Join(in, e.Name()))
		}
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("%s: no log files", in)
	}
	return files, nil
}

// makeEmitter builds the record sink: either a PartitionWriter under outDir,
// or a streaming stdout writer in the given format. It returns the emit
// function, a flush called between inputs, and a finalizer.
func makeEmitter(outDir, format string) (emit func(logpart.Record) error, flush, finish func() error, err error) {
	if outDir != "" {
		pw := logpart.NewPartitionWriter(outDir)
		return pw.Write, func() error { return nil }, pw.Close, nil
	}
	w := bufio.NewWriter(os.Stdout)
	switch format {
	case "ndjson":
		emit = func(rec logpart.Record) error {
			b, err := json.Marshal(rec)
			if err != nil {
				return err
			}
			if _, err := w.Write(b); err != nil {
				return err
			}
			return w.WriteByte('\n')
		}
		flush = w.Flush
	case "csv":
		cw := csv.NewWriter(w)
		header := []string{"seq", "source", "line", "clock", "event", "day", "invalid_clock", "late", "naive", "raw"}
		if err := cw.Write(header); err != nil {
			return nil, nil, nil, err
		}
		emit = func(rec logpart.Record) error {
			return cw.Write([]string{
				strconv.FormatInt(rec.Seq, 10),
				rec.Source,
				strconv.Itoa(rec.Line),
				rec.Clock,
				rec.Event,
				rec.Day,
				rec.InvalidClock,
				strconv.FormatBool(rec.Late),
				strconv.FormatBool(rec.Naive),
				rec.Raw,
			})
		}
		flush = func() error {
			cw.Flush()
			if err := cw.Error(); err != nil {
				return err
			}
			return w.Flush()
		}
	default:
		return nil, nil, nil, fmt.Errorf("unknown -format %q (want ndjson or csv)", format)
	}
	return emit, flush, flush, nil
}
