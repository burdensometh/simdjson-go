//go:build ignore
// +build ignore

package main

import (
	"fmt"
	"log"

	"github.com/minio/simdjson-go"
)

// This example expands a nested "items" array into one report row per item.
// Each row keeps the parent event's fields, the event index and the item index,
// and bad items are reported with their exact location.
func main() {
	const ndjson = `{"order":"A-1","items":[{"sku":"a","qty":1},{"sku":"b","qty":2}]}
{"order":"A-2","items":[]}
{"order":"A-3","items":"not an array"}
{"order":"A-4","items":[{"sku":"c","qty":3},7]}`

	pj, err := simdjson.ParseND([]byte(ndjson), nil)
	if err != nil {
		log.Fatal(err)
	}

	rows, err := pj.Expand(nil, []string{"items"},
		simdjson.ExpandItemType(simdjson.TypeObject),
		// Keep orders without items instead of dropping them from the report.
		simdjson.ExpandKeepEmptyParent(true),
	)
	if err != nil {
		log.Fatal(err)
	}

	for _, r := range rows {
		parent, err := r.Parent.Object(nil)
		if err != nil {
			log.Fatal(err)
		}
		order := ""
		if e := parent.FindKey("order", nil); e != nil {
			order, _ = e.Iter.String()
		}

		if r.Bad() {
			fmt.Printf("%-22s order=%-4s BAD: %v\n", r.Location(), order, r.BadItem())
			continue
		}
		if r.ItemIndex < 0 {
			fmt.Printf("%-22s order=%-4s (no items)\n", r.Location(), order)
			continue
		}
		item, err := r.Item.Interface()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%-22s order=%-4s %v\n", r.Location(), order, item)
	}

	// Output:
	// event[0].items[0]      order=A-1  map[qty:1 sku:a]
	// event[0].items[1]      order=A-1  map[qty:2 sku:b]
	// event[1].items         order=A-2  (no items)
	// event[2].items         order=A-3  BAD: simdjson: bad item at event[2].items: got string, want array ("not an array")
	// event[3].items[0]      order=A-4  map[qty:3 sku:c]
	// event[3].items[1]      order=A-4  BAD: simdjson: bad item at event[3].items[1]: got int, want object (7)
}
