//go:build ignore
// +build ignore

// Example: produce an internal-sharing excerpt of raw transaction records.
//
// Only explicitly approved paths may appear in the published output. The
// parsed reference record is never modified and is kept for comparison.
// Run with: go run examples/redact-fields.go
package main

import (
	"fmt"
	"log"

	"github.com/minio/simdjson-go"
)

const transactions = `{"txn_id":"TX-1001","counterparty":{"name":"Acme Corp","account":"DE89-3704-0044-0532","internal_note":"do not publish"},"amount":{"value":1250.75,"currency":"EUR","fx_rate":1.08},"line_items":[{"sku":"A-1","qty":2,"cost":10.5,"secret":"x"},{"sku":"B-2","qty":1,"cost":1229.75,"secret":"y"}],"status":"settled","fraud_score":0.03}`

// approvedPaths is the explicit allowlist of everything that may be shared.
// Numeric segments address array elements, everything else object members.
var approvedPaths = []string{
	"txn_id",
	"counterparty/name",
	"counterparty/account",
	"amount/value",
	"amount/currency",
	"line_items/0/sku",
	"line_items/1/sku",
	"line_items/1/qty",
	"status",
}

func main() {
	if !simdjson.SupportedCPU() {
		log.Fatal("unsupported CPU")
	}

	// Build-time validation locates duplicate and syntactically invalid
	// entries by their position in the configuration slice.
	allowlist, buildIssues := simdjson.ParseAllowlistPaths(approvedPaths)
	for _, is := range buildIssues {
		log.Printf("config problem: %v", is)
	}

	pj, err := simdjson.Parse([]byte(transactions), nil)
	if err != nil {
		log.Fatalf("parse failed: %v", err)
	}

	// Project builds brand new JSON from the parsed result. The tape, message
	// and string buffer of pj stay untouched, so it remains the reference.
	published, issues, err := allowlist.Project(pj)
	if err != nil {
		log.Fatalf("projection failed: %v", err)
	}
	for _, is := range issues {
		log.Printf("data problem: %v", is)
	}

	fmt.Println("published excerpt:")
	fmt.Println(string(published))

	reference := pj.Iter()
	ref, err := reference.MarshalJSON()
	if err != nil {
		log.Fatalf("reference marshal failed: %v", err)
	}
	fmt.Println("\nuntouched reference record:")
	fmt.Println(string(ref))
}
