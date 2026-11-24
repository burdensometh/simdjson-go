// Command eventloader ingests versioned payment/order event NDJSON into one
// canonical warehouse schema.
//
// Usage:
//
//	eventloader -in events.ndjson -out warehouse.ndjson -pending pending.ndjson
//
// The source version of every row is selected explicitly from its
// schema_version tag BEFORE extraction; the matching path mapping is then
// used for business time and amount. Rows with both the old and new field on
// the same line follow the documented "new-version-wins" conflict policy.
// Rows whose version is unknown (or whose mapped fields are missing/invalid)
// are written to the pending list instead of being guessed.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/minio/simdjson-go"
	"github.com/minio/simdjson-go/cmd/eventloader/internal/etl"
)

func main() {
	in := flag.String("in", "", "input NDJSON file (required)")
	out := flag.String("out", "warehouse.ndjson", "backfilled canonical NDJSON output")
	pendingPath := flag.String("pending", "pending.ndjson", "pending-list NDJSON output")
	flag.Parse()

	if *in == "" {
		fmt.Fprintln(os.Stderr, "eventloader: -in is required")
		flag.Usage()
		os.Exit(2)
	}

	raw, err := os.ReadFile(*in)
	if err != nil {
		fatal("read input", err)
	}
	if !simdjson.SupportedCPU() {
		fatal("cpu", fmt.Errorf("host CPU does not meet simdjson-go target specs"))
	}

	records, pending, summary, err := etl.Run(raw, etl.DefaultMappings())
	if err != nil {
		fatal("ingest", err)
	}

	if err := writeNDJSON(*out, records); err != nil {
		fatal("write output", err)
	}
	if err := writeNDJSON(*pendingPath, pending); err != nil {
		fatal("write pending list", err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(summary); err != nil {
		fatal("encode summary", err)
	}
	fmt.Printf("backfilled -> %s\npending    -> %s\n", *out, *pendingPath)
}

func writeNDJSON(path string, rows any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	bw := bufio.NewWriter(f)
	enc := json.NewEncoder(bw)
	enc.SetEscapeHTML(false)

	switch v := rows.(type) {
	case []etl.CanonicalRecord:
		for i := range v {
			if err := enc.Encode(v[i]); err != nil {
				return err
			}
		}
	case []etl.PendingItem:
		for i := range v {
			if err := enc.Encode(v[i]); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported row type %T", rows)
	}
	return bw.Flush()
}

func fatal(stage string, err error) {
	fmt.Fprintf(os.Stderr, "eventloader: %s: %v\n", stage, err)
	os.Exit(1)
}
