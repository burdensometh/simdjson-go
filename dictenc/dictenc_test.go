package dictenc

import (
	"bytes"
	"strings"
	"testing"

	simdjson "github.com/minio/simdjson-go"
)

func parseND(t *testing.T, ndjson string) *simdjson.ParsedJson {
	t.Helper()
	if !simdjson.SupportedCPU() {
		t.Skip("CPU does not support AVX2+CLMUL")
	}
	pj, err := simdjson.ParseND([]byte(ndjson), nil)
	if err != nil {
		t.Fatalf("ParseND: %v", err)
	}
	return pj
}

func factsNDJSON(t *testing.T, b *Batch) string {
	t.Helper()
	var buf bytes.Buffer
	if err := b.WriteFactsNDJSON(&buf); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func incrementJSON(t *testing.T, inc *Increment) string {
	t.Helper()
	var buf bytes.Buffer
	if err := inc.WriteJSON(&buf); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

// Labels shared across batches must keep one unified ID; a batch increment
// must only contain labels first seen in that batch.
func TestUnifiedIDsAcrossBatches(t *testing.T) {
	enc := NewEncoder() // channel, region

	b1, err := enc.EncodeBatch("v1", parseND(t,
		`{"channel":"web","region":"cn-north"}`+"\n"+
			`{"channel":"app","region":"cn-north"}`+"\n"+
			`{"channel":"web","region":"eu-west"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := factsNDJSON(t, b1),
		"{\"channel_id\":1,\"region_id\":0}\n"+
			"{\"channel_id\":0,\"region_id\":0}\n"+
			"{\"channel_id\":1,\"region_id\":1}\n"; got != want {
		t.Errorf("batch v1 facts:\ngot:  %swant: %s", got, want)
	}
	if got, want := incrementJSON(t, b1.Increment),
		"{\"version\":\"v1\",\"columns\":{\"channel\":[{\"id\":0,\"value\":\"app\"},{\"id\":1,\"value\":\"web\"}],\"region\":[{\"id\":0,\"value\":\"cn-north\"},{\"id\":1,\"value\":\"eu-west\"}]}}\n"; got != want {
		t.Errorf("batch v1 increment:\ngot:  %swant: %s", got, want)
	}

	// Batch v2 repeats "web"/"cn-north" heavily and adds "ios".
	b2, err := enc.EncodeBatch("v2", parseND(t,
		`{"channel":"web","region":"cn-north"}`+"\n"+
			`{"channel":"ios","region":"cn-north"}`+"\n"+
			`{"channel":"web","region":"cn-north"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := factsNDJSON(t, b2),
		"{\"channel_id\":1,\"region_id\":0}\n"+
			"{\"channel_id\":2,\"region_id\":0}\n"+
			"{\"channel_id\":1,\"region_id\":0}\n"; got != want {
		t.Errorf("batch v2 facts:\ngot:  %swant: %s", got, want)
	}
	// Increment holds only the new label; IDs continue the cumulative space.
	if got, want := incrementJSON(t, b2.Increment),
		"{\"version\":\"v2\",\"columns\":{\"channel\":[{\"id\":2,\"value\":\"ios\"}],\"region\":[]}}\n"; got != want {
		t.Errorf("batch v2 increment:\ngot:  %swant: %s", got, want)
	}

	// The cumulative dictionary resolves both old and new labels.
	d, _ := enc.Dictionary("channel")
	for value, want := range map[string]uint32{"app": 0, "web": 1, "ios": 2} {
		if id, ok := d.ID(value); !ok || id != want {
			t.Errorf("channel[%q] = %d, %v; want %d, true", value, id, ok, want)
		}
	}
	if v, ok := d.Value(2); !ok || v != "ios" {
		t.Errorf("channel id 2 = %q, %v; want ios, true", v, ok)
	}
}

// Re-running the same batch version against the same dictionary state must
// produce the identical mapping — even with events in a different order.
func TestDeterministicRerunSameVersion(t *testing.T) {
	const batch = `{"channel":"web","region":"cn-north"}` + "\n" +
		`{"channel":"app","region":"eu-west"}` + "\n" +
		`{"channel":"ios","region":"cn-north"}` + "\n" +
		`{"channel":"web","region":"eu-west"}`
	const shuffled = `{"channel":"ios","region":"cn-north"}` + "\n" +
		`{"channel":"web","region":"eu-west"}` + "\n" +
		`{"channel":"web","region":"cn-north"}` + "\n" +
		`{"channel":"app","region":"eu-west"}`

	encode := func(ndjson string) (string, string) {
		b, err := NewEncoder().EncodeBatch("v1", parseND(t, ndjson))
		if err != nil {
			t.Fatal(err)
		}
		return factsNDJSON(t, b), incrementJSON(t, b.Increment)
	}

	f1, i1 := encode(batch)
	f2, i2 := encode(batch)
	if f1 != f2 || i1 != i2 {
		t.Errorf("same input re-run differs:\nrun1: %s%s\nrun2: %s%s", f1, i1, f2, i2)
	}

	// Event order inside the batch must not change the label→ID mapping.
	f3, i3 := encode(shuffled)
	if i1 != i3 {
		t.Errorf("shuffled batch changed the mapping:\nordered:  %s\nshuffled: %s", i1, i3)
	}
	// Facts follow row order, so they differ from the ordered run, but every
	// row must carry the same unified IDs as the ordered mapping assigns.
	if got, want := f3, "{\"channel_id\":1,\"region_id\":0}\n"+
		"{\"channel_id\":2,\"region_id\":1}\n"+
		"{\"channel_id\":2,\"region_id\":0}\n"+
		"{\"channel_id\":0,\"region_id\":1}\n"; got != want {
		t.Errorf("shuffled facts:\ngot:  %swant: %s", got, want)
	}
}

// Re-running a batch version after its labels were absorbed must yield the
// same fact IDs and an empty increment.
func TestIdempotentRerun(t *testing.T) {
	enc := NewEncoder()
	const batch = `{"channel":"web","region":"cn-north"}` + "\n" +
		`{"channel":"app","region":""}` + "\n" +
		`{"channel":"web","region":null}`

	b1, err := enc.EncodeBatch("v1", parseND(t, batch))
	if err != nil {
		t.Fatal(err)
	}
	b2, err := enc.EncodeBatch("v1", parseND(t, batch))
	if err != nil {
		t.Fatal(err)
	}
	if !b2.Increment.Empty() {
		t.Errorf("re-run increment not empty: %s", incrementJSON(t, b2.Increment))
	}
	if f1, f2 := factsNDJSON(t, b1), factsNDJSON(t, b2); f1 != f2 {
		t.Errorf("re-run facts differ:\nrun1: %srun2: %s", f1, f2)
	}
	d, _ := enc.Dictionary("channel")
	if d.Len() != 2 {
		t.Errorf("re-run added entries: channel dict = %v", d.Entries)
	}
}

// Null, empty string and first-seen labels are three distinct things:
// null/missing → NullID and no dictionary entry; "" → its own unified ID;
// a new label → appended and reported in the increment.
func TestNullEmptyNewSemantics(t *testing.T) {
	enc := NewEncoder()
	b, err := enc.EncodeBatch("v1", parseND(t,
		`{"channel":null,"region":"cn-north"}`+"\n"+ // explicit null
			`{"region":""}`+"\n"+ // missing channel, empty region
			`{"channel":"","region":"cn-north"}`)) // empty channel
	if err != nil {
		t.Fatal(err)
	}
	ch, rg := b.Fact("channel"), b.Fact("region")
	if ch.IDs[0] != NullID {
		t.Errorf("explicit null: got id %d, want NullID", ch.IDs[0])
	}
	if ch.IDs[1] != NullID {
		t.Errorf("missing key: got id %d, want NullID", ch.IDs[1])
	}
	if ch.IDs[2] == NullID {
		t.Error("empty string must get a real ID, not NullID")
	}
	if rg.IDs[1] == NullID {
		t.Error("empty region must get a real ID, not NullID")
	}
	// Null/missing never enter the dictionary; "" does.
	d, _ := enc.Dictionary("channel")
	if d.Len() != 1 {
		t.Fatalf("channel dict = %v, want exactly one entry (empty string)", d.Entries)
	}
	if id, ok := d.ID(""); !ok || id != ch.IDs[2] {
		t.Errorf("empty string id = %d, %v; fact used %d", id, ok, ch.IDs[2])
	}
	if _, ok := d.ID("null"); ok {
		t.Error(`the string "null" must not appear unless present in data`)
	}
	// The increment reports "" as a first-seen label, null is nowhere.
	if got, want := incrementJSON(t, b.Increment),
		"{\"version\":\"v1\",\"columns\":{\"channel\":[{\"id\":0,\"value\":\"\"}],\"region\":[{\"id\":0,\"value\":\"\"},{\"id\":1,\"value\":\"cn-north\"}]}}\n"; got != want {
		t.Errorf("increment:\ngot:  %swant: %s", got, want)
	}
	// Facts keep null vs empty distinct.
	if got, want := factsNDJSON(t, b),
		"{\"channel_id\":null,\"region_id\":1}\n"+
			"{\"channel_id\":null,\"region_id\":0}\n"+
			"{\"channel_id\":0,\"region_id\":1}\n"; got != want {
		t.Errorf("facts:\ngot:  %swant: %s", got, want)
	}
}

// A saved and restored encoder must continue the cumulative mapping
// exactly as if it had never been persisted.
func TestSaveLoadRoundTrip(t *testing.T) {
	run := func(reload bool) (string, string) {
		enc := NewEncoder()
		if _, err := enc.EncodeBatch("v1", parseND(t,
			`{"channel":"web","region":"cn-north"}`+"\n"+`{"channel":"app","region":"eu-west"}`)); err != nil {
			t.Fatal(err)
		}
		if reload {
			var state bytes.Buffer
			if err := enc.Save(&state); err != nil {
				t.Fatal(err)
			}
			var err error
			enc, err = Load(&state)
			if err != nil {
				t.Fatal(err)
			}
		}
		b2, err := enc.EncodeBatch("v2", parseND(t,
			`{"channel":"ios","region":"cn-north"}`+"\n"+`{"channel":"web","region":"eu-west"}`))
		if err != nil {
			t.Fatal(err)
		}
		return factsNDJSON(t, b2), incrementJSON(t, b2.Increment)
	}
	f1, i1 := run(false)
	f2, i2 := run(true)
	if f1 != f2 || i1 != i2 {
		t.Errorf("round trip changed mapping:\nuninterrupted: %s%s\nreloaded:      %s%s", f1, i1, f2, i2)
	}
}

// A batch version split over several parsed chunks must encode exactly as
// one chunk: one increment for the whole version.
func TestMultiChunkBatch(t *testing.T) {
	single, err := NewEncoder().EncodeBatch("v1", parseND(t,
		`{"channel":"web"}`+"\n"+`{"channel":"app"}`+"\n"+`{"channel":"ios"}`))
	if err != nil {
		t.Fatal(err)
	}
	chunked, err := NewEncoder().EncodeBatch("v1",
		parseND(t, `{"channel":"web"}`+"\n"+`{"channel":"app"}`),
		parseND(t, `{"channel":"ios"}`))
	if err != nil {
		t.Fatal(err)
	}
	if f1, f2 := factsNDJSON(t, single), factsNDJSON(t, chunked); f1 != f2 {
		t.Errorf("chunked facts differ:\nsingle:  %schunked: %s", f1, f2)
	}
	if i1, i2 := incrementJSON(t, single.Increment), incrementJSON(t, chunked.Increment); i1 != i2 {
		t.Errorf("chunked increment differs:\nsingle:  %schunked: %s", i1, i2)
	}
}

func TestNonStringValueErrors(t *testing.T) {
	_, err := NewEncoder().EncodeBatch("v1", parseND(t, `{"channel":42}`))
	if err == nil || !strings.Contains(err.Error(), "expected string or null") {
		t.Fatalf("got %v, want type error", err)
	}
	_, err = NewEncoder().EncodeBatch("v1", parseND(t, `{"channel":{"x":1}}`))
	if err == nil {
		t.Fatal("object value must be rejected")
	}
}

func TestLoadRejectsBadState(t *testing.T) {
	if _, err := Load(strings.NewReader(`{"format":2,"columns":{"channel":[]}}`)); err == nil {
		t.Error("unsupported format must be rejected")
	}
	if _, err := Load(strings.NewReader(`{"format":1,"columns":{"channel":["a","a"]}}`)); err == nil {
		t.Error("duplicate entries must be rejected")
	}
	if _, err := Load(strings.NewReader(`{"format":1,"columns":{}}`)); err == nil {
		t.Error("empty column set must be rejected")
	}
}

func TestCustomColumns(t *testing.T) {
	enc := NewEncoder("region")
	b, err := enc.EncodeBatch("v1", parseND(t, `{"channel":"web","region":"cn-north"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := factsNDJSON(t, b), "{\"region_id\":0}\n"; got != want {
		t.Errorf("facts:\ngot:  %swant: %s", got, want)
	}
	if b.Fact("channel") != nil {
		t.Error("unconfigured column must not produce facts")
	}
}
