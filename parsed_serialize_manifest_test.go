/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package simdjson

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"testing"
)

func TestSerializeManifestRoundTrip(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	test := func(t *testing.T, s *Serializer) {
		for _, tt := range testCases {
			org := loadCompressed(t, tt.name)
			pj, err := Parse(org, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Run(tt.name, func(t *testing.T) {
				i := pj.Iter()
				want, err := i.MarshalJSON()
				if err != nil {
					t.Fatal(err)
				}
				output := s.SerializeWithManifest(nil, *pj)

				// Verified read must return the evidence and the records.
				pj2, m, err := s.DeserializeVerified(output, nil)
				if err != nil {
					t.Fatal(err)
				}
				if m.Format != serializedVersionManifest {
					t.Errorf("format identifier: got %d, want %d", m.Format, serializedVersionManifest)
				}
				if m.Records != 1 {
					t.Errorf("records: got %d, want 1", m.Records)
				}
				i = pj2.Iter()
				got, err := i.MarshalJSON()
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(want, got) {
					t.Fatal("output mismatch")
				}

				// Plain Deserialize must verify the manifest as well.
				if _, err := s.Deserialize(output, nil); err != nil {
					t.Fatal(err)
				}

				// The digest must be deterministic and independent
				// of serializer state and compression mode.
				output2 := NewSerializer().SerializeWithManifest(nil, *pj)
				_, m2, err := s.DeserializeVerified(output2, nil)
				if err != nil {
					t.Fatal(err)
				}
				if m.Digest != m2.Digest {
					t.Fatal("digest is not deterministic")
				}
			})
		}
	}
	t.Run("default", func(t *testing.T) {
		test(t, NewSerializer())
	})
	t.Run("none", func(t *testing.T) {
		s := NewSerializer()
		s.CompressMode(CompressNone)
		test(t, s)
	})
	t.Run("fast", func(t *testing.T) {
		s := NewSerializer()
		s.CompressMode(CompressFast)
		test(t, s)
	})
	t.Run("best", func(t *testing.T) {
		s := NewSerializer()
		s.CompressMode(CompressBest)
		test(t, s)
	})
}

func TestSerializeManifestNDJSON(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	const ndjson = `{"a":1}
{"a":2}
{"a":3}
{"a":4}
{"a":5}
`
	pj, err := ParseND([]byte(ndjson), nil)
	if err != nil {
		t.Fatal(err)
	}
	s := NewSerializer()
	output := s.SerializeWithManifest(nil, *pj)
	pj2, m, err := s.DeserializeVerified(output, nil)
	if err != nil {
		t.Fatal(err)
	}
	if m.Records != 5 {
		t.Fatalf("records: got %d, want 5", m.Records)
	}
	i := pj.Iter()
	want, err := i.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	i = pj2.Iter()
	got, err := i.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(want, got) {
		t.Fatal("output mismatch")
	}
}

func TestDeserializeManifestOldFormat(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj, err := Parse([]byte(demo_json), nil)
	if err != nil {
		t.Fatal(err)
	}
	s := NewSerializer()
	// Serialize without a manifest (old format).
	output := s.Serialize(nil, *pj)

	// Old format remains explicitly readable.
	if _, err := s.Deserialize(output, nil); err != nil {
		t.Fatal(err)
	}
	// ...but a verified read must report the missing evidence,
	// distinct from a mismatch.
	_, _, err = s.DeserializeVerified(output, nil)
	if !errors.Is(err, ErrManifestMissing) {
		t.Fatalf("want ErrManifestMissing, got %v", err)
	}
	if errors.Is(err, ErrManifestMismatch) {
		t.Fatal("missing evidence reported as mismatch")
	}
}

func TestDeserializeManifestMismatch(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj, err := Parse([]byte(demo_json), nil)
	if err != nil {
		t.Fatal(err)
	}
	newBlob := func() []byte {
		s := NewSerializer()
		s.CompressMode(CompressNone)
		return s.SerializeWithManifest(nil, *pj)
	}
	// corrupt expects the evidence to not match the content.
	corrupt := func(t *testing.T, blob []byte) {
		t.Helper()
		s := NewSerializer()
		_, _, err := s.DeserializeVerified(blob, nil)
		if !errors.Is(err, ErrManifestMismatch) {
			t.Fatalf("DeserializeVerified: want ErrManifestMismatch, got %v", err)
		}
		if errors.Is(err, ErrManifestMissing) {
			t.Fatal("mismatch reported as missing evidence")
		}
		// Plain Deserialize must reject the content as well.
		if _, err := s.Deserialize(blob, nil); !errors.Is(err, ErrManifestMismatch) {
			t.Fatalf("Deserialize: want ErrManifestMismatch, got %v", err)
		}
	}
	t.Run("digest", func(t *testing.T) {
		blob := newBlob()
		// Last byte of the digest in the manifest trailer.
		blob[len(blob)-1] ^= 0xff
		corrupt(t, blob)
	})
	t.Run("records", func(t *testing.T) {
		blob := newBlob()
		// Last byte of the record count in the manifest trailer.
		blob[len(blob)-sha256.Size-1] ^= 0xff
		corrupt(t, blob)
	})
	t.Run("format", func(t *testing.T) {
		blob := newBlob()
		// Format identifier in the manifest trailer.
		blob[len(blob)-sha256.Size-8-1] ^= 0xff
		corrupt(t, blob)
	})
	t.Run("content", func(t *testing.T) {
		blob := newBlob()
		// Corrupt the stored content itself.
		off := stringsSectionOffset(t, blob)
		blob[off] ^= 0xff
		corrupt(t, blob)
	})
}

func TestDeserializeManifestMissing(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	pj, err := Parse([]byte(demo_json), nil)
	if err != nil {
		t.Fatal(err)
	}
	s := NewSerializer()
	full := s.SerializeWithManifest(nil, *pj)

	missing := func(t *testing.T, blob []byte) {
		t.Helper()
		_, _, err := s.DeserializeVerified(blob, nil)
		if !errors.Is(err, ErrManifestMissing) {
			t.Fatalf("DeserializeVerified: want ErrManifestMissing, got %v", err)
		}
		if errors.Is(err, ErrManifestMismatch) {
			t.Fatal("missing evidence reported as mismatch")
		}
		// Plain Deserialize must reject the content as well.
		if _, err := s.Deserialize(blob, nil); !errors.Is(err, ErrManifestMissing) {
			t.Fatalf("Deserialize: want ErrManifestMissing, got %v", err)
		}
	}
	t.Run("truncated trailer", func(t *testing.T) {
		missing(t, full[:len(full)-10])
	})
	t.Run("no trailer", func(t *testing.T) {
		missing(t, full[:len(full)-manifestSize])
	})
	t.Run("bad magic", func(t *testing.T) {
		blob := append([]byte(nil), full...)
		blob[len(blob)-manifestSize] = 'X'
		missing(t, blob)
	})
}

func TestSerializeManifestDistinctContent(t *testing.T) {
	if !SupportedCPU() {
		t.SkipNow()
	}
	s := NewSerializer()
	pjA, err := Parse([]byte(`{"a":1}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	pjB, err := Parse([]byte(`{"a":2}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	blobA := s.SerializeWithManifest(nil, *pjA)
	blobB := s.SerializeWithManifest(nil, *pjB)
	digestA := blobA[len(blobA)-sha256.Size:]
	digestB := blobB[len(blobB)-sha256.Size:]
	if bytes.Equal(digestA, digestB) {
		t.Fatal("different content produced equal digests")
	}
	// Same content again must produce the same digest.
	blobA2 := NewSerializer().SerializeWithManifest(nil, *pjA)
	if !bytes.Equal(digestA, blobA2[len(blobA2)-sha256.Size:]) {
		t.Fatal("digest is not deterministic")
	}
}

// stringsSectionOffset returns the offset of the strings (message) section
// payload in a serialized blob. Only valid for CompressNone output.
func stringsSectionOffset(t *testing.T, blob []byte) int {
	t.Helper()
	r := bytes.NewReader(blob)
	readByte := func() byte {
		b, err := r.ReadByte()
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	readUvarint := func() uint64 {
		v, err := binary.ReadUvarint(r)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	readByte()                            // version
	readUvarint()                         // compressed size
	readUvarint()                         // tape size
	readUvarint()                         // strings size
	readUvarint()                         // strings block
	if size := readUvarint(); size == 0 { // strings (message) uncompressed size
		t.Fatal("no strings in batch")
	}
	readUvarint() // strings (message) compressed size
	if typ := readByte(); typ != blockTypeUncompressed {
		t.Fatalf("expected uncompressed strings block, got type %d", typ)
	}
	return len(blob) - r.Len()
}
