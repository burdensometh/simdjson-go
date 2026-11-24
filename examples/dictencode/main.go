// Command dictencode encodes one batch version of NDJSON events into
// dictionary-encoded fact columns plus a dictionary increment, advancing
// the cumulative business dictionary.
//
// Usage:
//
//	go run ./examples/dictencode -version 2026-11-24 [-dict dictionary.json] [-out dir] batch1.ndjson [batch2.ndjson...]
//
// All input files are chunks of the same logical batch (same -version).
// Outputs, written to -out:
//
//	facts-<version>.ndjson      one {"channel_id":...,"region_id":...} per event, null for JSON null/absent
//	increment-<version>.json    dictionary entries first seen in this version
//	dictionary.json             updated cumulative dictionary (atomic replace)
//
// Re-running the same version with the same starting dictionary.json
// reproduces byte-identical facts and increment.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	simdjson "github.com/minio/simdjson-go"
	"github.com/minio/simdjson-go/dictenc"
)

func main() {
	version := flag.String("version", "", "batch version being encoded (required)")
	dictPath := flag.String("dict", "dictionary.json", "cumulative dictionary state file")
	outDir := flag.String("out", ".", "output directory for facts and increment")
	flag.Parse()
	if *version == "" || flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}
	if !simdjson.SupportedCPU() {
		log.Fatal("Unsupported CPU")
	}

	// Load the cumulative dictionary, or start empty.
	enc := dictenc.NewEncoder()
	if f, err := os.Open(*dictPath); err == nil {
		if enc, err = dictenc.Load(f); err != nil {
			log.Fatalf("loading %s: %v", *dictPath, err)
		}
		f.Close()
	} else if !os.IsNotExist(err) {
		log.Fatalf("opening %s: %v", *dictPath, err)
	}

	// Parse every chunk of this batch version.
	pjs := make([]*simdjson.ParsedJson, 0, flag.NArg())
	for _, path := range flag.Args() {
		b, err := os.ReadFile(path)
		if err != nil {
			log.Fatalf("reading %s: %v", path, err)
		}
		pj, err := simdjson.ParseND(b, nil)
		if err != nil {
			log.Fatalf("parsing %s: %v", path, err)
		}
		pjs = append(pjs, pj)
	}

	batch, err := enc.EncodeBatch(*version, pjs...)
	if err != nil {
		log.Fatal(err)
	}

	// Emit fact columns and the dictionary increment.
	factsPath := filepath.Join(*outDir, fmt.Sprintf("facts-%s.ndjson", *version))
	if err := writeFile(factsPath, batch.WriteFactsNDJSON); err != nil {
		log.Fatal(err)
	}
	incPath := filepath.Join(*outDir, fmt.Sprintf("increment-%s.json", *version))
	if err := writeFile(incPath, batch.Increment.WriteJSON); err != nil {
		log.Fatal(err)
	}

	// Advance the cumulative dictionary atomically.
	tmp := *dictPath + ".tmp"
	if err := writeFile(tmp, enc.Save); err != nil {
		log.Fatal(err)
	}
	if err := os.Rename(tmp, *dictPath); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("version %s: %d rows, %d new dictionary entries\n", *version, batch.Rows, countEntries(batch.Increment))
	fmt.Printf("  facts:     %s\n  increment: %s\n  dict:      %s\n", factsPath, incPath, *dictPath)
}

func writeFile(path string, fn func(w io.Writer) error) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := fn(f); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func countEntries(inc *dictenc.Increment) (n int) {
	for _, es := range inc.Columns {
		n += len(es)
	}
	return n
}
