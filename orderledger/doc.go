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

// Package orderledger merges per-order update records that arrive from a
// single upstream source, and lets a downstream consumer select the final
// version of each order within an upload batch.
//
// Batches are taken directly from parsed simdjson-go results (NDJSON, or a
// single JSON array of records) and merged by order key and by version order,
// not by arrival order. The merge is stateful, so a Ledger can ingest several
// upload batches in sequence and always reports the current final event plus
// the versions it superseded.
//
// The rules applied per order key are:
//
//   - The record with the highest version wins. Every lower version is kept
//     as superseded together with the value it carried, so the selection can
//     be audited after the fact.
//   - Redelivery of a record that was already seen (same key, same version,
//     same value) is a duplicate. It does not create a new version and does
//     not change the final event. Duplicates are counted in Result.Duplicates,
//     but they are not an error.
//   - Two distinct values for the same key and version are a conflict. The
//     version is not silently resolved by arrival order: both values are
//     retained and reported as a Conflict for reconciliation. If the
//     conflicting version is the highest one, the order has no unambiguous
//     final event (Entry.FinalEvent returns nil) until it is reconciled.
//   - Records that cannot be merged (missing key or version, non-scalar key
//     or version, malformed version, record that is not an object) are
//     reported per record as an InvalidRecord instead of failing the batch.
//
// "Same value" is decided on a canonical representation of the whole record
// (object keys sorted, numbers in their shortest form), so whitespace and
// key order do not matter. 10, 10.0 and "10" as a version also compare equal.
// Fields that only carry delivery metadata (an ingestion timestamp, a broker
// offset, ...) can be excluded from that comparison with Config.IgnoreFields.
//
// An order key is identified by its text form, so the JSON string "123" and
// the integer 123 address the same order. If deliveries disagree about the
// value, that shows up as a conflict rather than as two unrelated orders.
//
// A Ledger is scoped to one source. Keys from different sources must not be
// mixed in the same Ledger.
//
// Example:
//
//	ledger := orderledger.New(orderledger.Config{})
//	res, err := ledger.IngestParsed(pj)
//	if err != nil {
//	    return err
//	}
//	for _, final := range res.Final {
//	    // final.Raw is the event to upload for final.Key.
//	    _ = final.Raw
//	}
//	for _, conflict := range res.Conflicts {
//	    // Hand over to reconciliation instead of picking one value.
//	    _ = conflict
//	}
package orderledger
