# On-disk formats

Everything the hub writes is versioned here. A release that changes a format bumps its
version number and keeps reading the previous one; `p10logs-hub --check` reports what it
finds on disk.

## Layout

```
/data
├── index.sqlite            derived catalog (rebuild with --rebuild-index)
├── wal/NNNNNN.wal          write-ahead log segments (v1)
├── days/<YYYYMMDD>/<cluster>/<namespace>/<pod-uid>/<container>/
│   ├── <unixnano>.chunk    sealed chunk (v1)
│   └── <unixnano>.open     chunk being appended (same format, no footer)
└── cache/                  local copies of chunks fetched from object storage
```

## Chunk v1

Append-only file of frames, then (sealed only) a footer.

```
frame  = magic u32 "P1LF" | metaLen u32 | payloadLen u32 | crc32 u32 (meta+payload)
         | meta JSON | zstd(payload)
payload = repeated { ts i64 nanos | stream u8 (0 stdout, 1 stderr) | len u32 | msg }
footer = footer JSON | footerLen u32 | fmagic u32 "P1LT"
```

Frame meta (JSON): `cl` cluster, `k` `ns/pod/uid/container`, `f` source file id,
`o` `[startOff, endOff)` in the source file, `n` node, `r` restart count, `end` stream
ended, `l` labels, `w` max WAL sequence covered, `min`/`max` timestamps, `c` line count.

Footer (JSON): `v` 1, `cl`, `k`, `r`, `l`, `w`, `f`/`o` (cursor of the last frame),
`min`, `max`, `lines`, `bytes` (uncompressed payload), `frames` (`off`, `len`, `min`,
`max`, `c` per frame), `bloom` (base64 of a 64 KiB trigram bloom, k=4).

Recovery rule: a reader validates CRCs frame by frame and truncates at the first bad
frame. A sealed chunk is immutable; readers seek with the footer's frame table.

## WAL v1

```
record = magic u32 "P1WA" | len u32 | crc32 u32 (body)
body   = seq u64 | metaLen u32 | meta JSON (same as frame meta) | raw payload (uncompressed)
```

Records are fsynced before the push is acknowledged. Replay skips records whose `seq`
is ≤ the stream's flushed high-water mark (`streams.wseq`, also carried in frame meta
and footers). `Checkpoint` = flush all memtables, start a new segment, delete older ones.

## Index (SQLite)

Tables `streams`, `chunks`, `cursors`. Columns added after v0.1 are created with
`ALTER TABLE … ADD COLUMN` at startup, so an old index opens under a new hub. The index
is never the source of truth: `--rebuild-index` recreates it from footers, frame meta
and the WAL.

## Wire (agent → hub) v1

zstd-compressed NDJSON, see `internal/wire`. Headers `X-P10-Cluster`, `X-P10-Node`,
`X-P10-Agent`, `X-P10-Agent-Stats`. An agent from v0.1 can push to any later hub.

## Compatibility policy

- Hub N reads chunks, WAL and index written by any hub ≤ N.
- Agent N works with hub ≥ N-1 and ≤ N+1 (wire v1 is frozen; new fields are optional).
- HTTP API `/api/v1` is frozen: fields may be added, never removed or retyped.
- Chart major versions follow the hub's on-disk format; a minor upgrade never needs
  a data migration.
