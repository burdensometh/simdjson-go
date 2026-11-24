# logpart — event-time log repartitioning

`logpart` repartitions day-organized logs into **event-time** partitions.

Logs are stored one file per source day, and each file may end with
late-arriving records that belong to a previous business day. `logpart`
streams them line by line, converts each line's event clock into a configured
target timezone, and emits one record per line — **in source order** —
carrying the source order, the original clock and the target day. Invalid
clocks are isolated in their own column and routed to a repair partition.

## Stream record schema

One NDJSON object (or CSV row) per non-blank input line, in source order:

| column          | meaning                                                        |
|-----------------|----------------------------------------------------------------|
| `seq`           | global source order, 0-based, monotonic across all inputs      |
| `source`        | source day (`2026-11-24`) or source label                      |
| `line`          | 1-based physical line number within the source                 |
| `clock`         | original clock text, verbatim                                  |
| `event`         | event instant normalized to the target timezone (RFC3339)      |
| `day`           | **target partition key**: event day in the target timezone     |
| `invalid_clock` | repair column: the unparseable clock (or line, if none found)  |
| `late`          | target day is earlier than the source day (late arrival)       |
| `naive`         | clock had no timezone; read as wall time in the target zone    |
| `raw`           | the original log line                                          |

Exactly one of `day` / `invalid_clock` is set on every record.

## Correctly distinguished at partition load

The partition key always comes from the **event time**, never the file name,
so a partition loader separates the easily confused cases:

- **Cross-midnight** — an offset-aware clock near midnight may change day in
  the target zone: `2026-11-24T16:00:00-05:00` in `Asia/Shanghai` loads into
  `dt=2026-11-25`, even though no source file for that day exists yet.
- **Timezone-less** — `2026-11-24 03:00:00` is read as wall time in the
  target zone, flagged `naive:true`, and loads into `dt=2026-11-24`.
- **Late arrivals** — a record appended to `2026-11-24.log` whose event was
  `2026-11-23T23:58:00+08:00` loads into `dt=2026-11-23` flagged
  `late:true` with `source:"2026-11-24"`.
- **Invalid clocks** — `not-a-clock`, month-13 dates, missing clocks: the
  value lands in `invalid_clock` and the record loads into `dt=__repair__`
  for later fix-up, never into a day partition.

## CLI

```
logpart [flags] [files-or-dirs...]

  -tz string      target timezone: IANA name (Asia/Shanghai), UTC, Local,
                  or fixed offset +08:00 (default "UTC")
  -fields string  comma-separated JSON keys probed for the event clock
  -format string  stream format without -out-dir: ndjson or csv
  -out-dir string write partitioned files under DIR/dt=<day>/ instead of stdout
```

With no inputs, stdin is read. Directories expand to their regular files in
name order. With `-out-dir`, output lands in `DIR/dt=<day>/part-00000.ndjson`
plus `DIR/dt=__repair__/`; each partition file preserves the source order of
the records routed to it.

```console
$ logpart -tz Asia/Shanghai -out-dir out logs/
$ cat 'out/dt=2026-11-23/part-00000.ndjson'
{"seq":0,"source":"2026-11-23","line":1,"clock":"2026-11-23T00:12:01+08:00","event":"2026-11-23T00:12:01+08:00","day":"2026-11-23","raw":"2026-11-23T00:12:01+08:00 INFO boot"}
{"seq":5,"source":"2026-11-24","line":3,"clock":"2026-11-23T23:58:00+08:00","event":"2026-11-23T23:58:00+08:00","day":"2026-11-23","late":true,"raw":"2026-11-23T23:58:00+08:00 INFO arrived-late"}
```

## Library

```go
loc, _ := logpart.LoadLocation("Asia/Shanghai")
rep := logpart.New(loc, nil) // nil → default JSON clock fields
rep.Process(ctx, reader, "2026-11-24", func(rec logpart.Record) error {
    // rec.Seq / rec.Clock / rec.Day / rec.InvalidClock / rec.Late / rec.Naive
    return writer.Write(rec) // e.g. logpart.NewPartitionWriter(dir)
})
recs, _ := logpart.LoadPartition(dir, "2026-11-23") // load one day back
```

Clock extraction: for JSON lines the configured fields are probed (default
`event_time`, `@timestamp`, `timestamp`, `time`, `ts`, `datetime`, `date`);
otherwise the leading timestamp of a plain-text line is used. Accepted clock
forms: RFC3339/ISO-8601 with offset, the same without offset (naive),
`yyyy-MM-dd HH:MM:SS[.frac]` (±offset or naive), date-only, and Unix epochs
(10/13/16/19 digits: s/ms/µs/ns). Blank lines are skipped. Naive wall times
that fall in a DST gap/overlap resolve per Go's `time.ParseInLocation` and
always keep their calendar day.
