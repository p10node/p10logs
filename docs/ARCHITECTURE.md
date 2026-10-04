# p10logs architecture

Deep dive for contributors. The README covers *what* and *why*; this covers *how*.

## 1. Data flow

```
 node                                   hub (StatefulSet, 1 replica, PVC)
 ┌─────────────────────────────┐        ┌──────────────────────────────────────┐
 │ kubelet/CRI writes          │        │  POST /api/v1/push  (bearer token)   │
 │ /var/log/pods/<ns>_<pod>_   │        │        │                             │
 │   <uid>/<ctr>/<n>.log       │        │        ▼                             │
 │        │                    │  zstd  │  auth → dedup(cursor) → router       │
 │        ▼                    │ NDJSON │        │                             │
 │ p10logs-agent (DaemonSet)   │ ─────► │        ▼                             │
 │  discover → tail → parse    │ HTTP/1 │  WAL fsync → ack → memtable          │
 │  → batch → ship             │        │        │             │               │
 │  ↕ positions.json (hostPath)│        │  frame → open chunk  ├─► live bus ──► SSE /tail
 │  ↕ disk buffer (hostPath)   │        │  seal @4MiB/5min     │               │
 └─────────────────────────────┘        │  index.sqlite ◄──────┘               │
                                        │  retention loop (age, disk cap)      │
                                        │  GET /api/v1/query|streams|export    │
                                        │  embedded UI (/)                     │
                                        └──────────────────────────────────────┘
```

Nothing in this path touches the Kubernetes API server. The agent learns
`namespace / pod / uid / container / restart-count` from the file path alone.

## 2. Agent

Single static Go binary, ~15 MB image (distroless/static), runs as a DaemonSet.

### 2.1 Discovery

- Scan `/var/log/pods` every 2 s (plain directory listing, no inotify dependency) for
  `<ns>_<pod>_<uid>/<container>/<restart>.log`. A new file is picked up within one scan;
  an open file is polled every 250 ms once at EOF.
- Parse path with a fixed regex; no API calls.
- Filters (`collect.includeNamespaces` / `excludeNamespaces` / `excludeContainers`) are
  applied here, so excluded streams cost nothing.
- Self-exclusion: the agent's own pod (`POD_NAMESPACE`/`POD_NAME` env) is skipped unless
  `collect.self=true`.

### 2.2 Tailing

Per file identity `(path, inode, first line)`, one tailer goroutine:

- Open, seek to checkpointed offset (or 0 if new; **never** "end" because we want the
  backlog of a container that was already running before the agent started).
- Read in 64 KiB blocks, split lines, parse CRI format:
  `<RFC3339Nano ts> <stdout|stderr> <F|P>[:tags] <payload>`.
  `P` (partial) lines are concatenated, per `(file, stream)`, until an `F` arrives; the
  merged line keeps the timestamp of the first chunk (same rule as kubelet's
  `parseCRILog`). containerd splits at `max_container_log_line_size` = 16 384 bytes;
  CRI-O/conmon emits `P` for any 8 KiB read without a newline. Merged size is capped at
  1 MiB (then force-flushed) so a pathological writer cannot grow memory.
- Legacy docker/cri-dockerd nodes: `/var/log/pods/…/<n>.log` is a symlink into
  `/var/lib/docker/containers/<id>/<id>-json.log` (`{"log","stream","time"}` JSON). Set
  `agent.hostPaths.docker` so the symlink resolves; the parser auto-detects the format
  from the first byte (`{` → docker JSON, digit → CRI).
- Rotation handling (kubelet `container_log_manager`, checked every
  `containerLogMonitorInterval`=10 s): when `0.log` ≥ `containerLogMaxSize` (10 Mi)
  kubelet renames it to `0.log.<YYYYMMDD-HHMMSS>` and asks the runtime to reopen the
  same path → **same path, new inode**. Older rotated files are gzipped on a later
  pass and only `containerLogMaxFiles - 2` of them are kept (default 5 → 3). The tailer
  therefore keys its state by `(path, inode, first line)`, keeps draining the old inode
  to EOF, then switches to the new one. Truncation (size < offset) ends the tailer and the
  discoverer starts a new one keyed by the new first line, so the hub sees a new file.
- Backfill: when a container directory is seen for the **first time** (fresh install,
  or a node the agent never ran on), kubelet has usually already gzipped the rotated
  files, so the agent also reads `<n>.log.<ts>.gz` (gzip stream, oldest first, low
  priority, never checkpointed by offset but marked done by name). This is what makes
  "install p10logs and see the last few hours" work instead of only the current 10 MiB.
  `.gz` files that appear later are ignored: their content was already read live.
- Restarted container → new file `1.log`, `2.log`, … → new tailer, same stream key
  but with `restart` attribute, so the UI can show restart boundaries. Kubelet GC keeps
  only the current + one previous restart file by default, which is why persisting in
  the hub matters.
- Pod deleted → kubelet GC removes the whole `<ns>_<pod>_<uid>` directory once the pod
  is gone from the API. The tailer holds its fd, drains to EOF, flushes, then closes.

### 2.3 Checkpointing (positions)

`<state>/positions.json`: one JSON object, rewritten atomically (temp file + rename)
once per second when dirty, so a crash leaves either the old or the new file, never a
torn one.

```
key   = "<path>@<inode>@<fnv64 of the first line>"   (gz backfill files: the file name)
        # the first-line hash matters: kubelet deletes a rotated file once it is gzipped and the
        # kernel reuses the inode for the next 0.log; with "<path>@<inode>" alone the new file
        # inherited the old checkpoint and the hub's cursor, and every new line was dropped as a
        # duplicate until the file outgrew the old one (never, at 10 MiB rotation). A legacy
        # "<path>@<inode>" entry is adopted once by the first agent that sees the new format.
value = { offset, inode, done, seen }
```

`offset` always points at the start of a complete **logical** line: it advances only
past an `F` record (never between `P` chunks) and, with multiline enabled, only past a
flushed multiline group. It is advanced only when the batch containing the line is
**durable elsewhere**: acknowledged by the hub, or written to the disk spool (§2.5). A
restart therefore re-reads at most the last un-acked batch, and the hub's cursor dedup
(§3.2) drops those lines again. Net effect: exactly-once.

- The key includes the full path (`<uid>/<container>/<restart>.log`), which kubelet never
  reuses for different content except by truncating in place (CRI-O `log_size_max`,
  cri-dockerd); that case is caught by a size check on open and on every poll
  (size < offset → start from 0). There is no content fingerprint.
- A corrupt checkpoint file is treated as empty rather than refusing to start; dedup
  covers the re-read.
- Entries not seen for 24 h are purged on save.

### 2.4 Batching and shipping

- Lines grouped by stream key; batch flushed when `batch.maxBytes`, `batch.maxLines`, or
  `batch.flushInterval` is hit (whichever first).
- Wire format: NDJSON, zstd-compressed (`Content-Encoding: zstd`), one HTTP/1.1
  keep-alive connection per agent, HTTP/2 when the hub is behind TLS.

```
POST /api/v1/push
Authorization: Bearer <token>
X-P10-Cluster: prod-eu
X-P10-Node: ip-10-0-1-23
X-P10-Agent: 0.1.0
Content-Type: application/x-ndjson
Content-Encoding: zstd

{"k":"kube-system/coredns-7d8b/…uid…/coredns/0","f":"…uid…/coredns/0.log@inode@firstlinehash","o":[10240,12288]}
{"t":1727683200123456789,"s":"o","m":"[INFO] plugin/reload: Running configuration…"}
{"t":1727683200123999000,"s":"e","m":"[ERROR] plugin/errors: 2 example.com. A: dial tcp…"}
{"k":"…next stream…","f":"…","o":[0,4096]}
…
```

- A `k` line opens a stream section: `k` = stream key, `f` = file identity,
  `o` = `[startOffset, endOffset)` of the bytes this batch covers. Optional
  `"end":true` marks the stream as finished (file/directory removed by kubelet → pod
  gone); the hub uses it for the live/gone marker instead of asking the API server.
- Heartbeat: every `heartbeatInterval` (10 s) the agent pushes an empty batch whose
  headers carry `X-P10-Agent-Stats: files=<n>;lag_bytes=<n>;spool_bytes=<n>;dropped=<n>`.
  The hub keeps an `agents` table (cluster, node, version, last_seen, stats) shown on
  the UI "Agents" page and at `/api/v1/status`, so "node X stopped shipping 20 min
  ago" is visible without a metrics stack.
- `s` = `o`/`e` (stdout/stderr). Optional `"l":{…}` on the `k` line carries enrichment
  labels when `enrich.enabled`.
- Hub answers `204` (all accepted), `202` (accepted, some ranges deduped), `413`
  (batch too large: dropped and logged), `429` with `Retry-After` (per-cluster limit),
  `401`/`403` (re-read the token file). Anything but 2xx/413: three quick retries, then
  the batch goes to the disk spool and the shipper backs off 1 s → 30 s.

### 2.5 Disk buffer

When the hub is unreachable, batches (already zstd) are written to
`<state>/buffer/<seq>.zst` (temp file + rename) and count as acknowledged. On reconnect
the buffer drains oldest-first, and new batches queue behind it so ordering is kept.
When `buffer.maxBytes` is exceeded, the oldest files are deleted; the drop count is
reported in the heartbeat stats, on the agent's `/status` endpoint and on the hub's
"Agents" page.

### 2.6 Enrichment (optional)

One list + watch per agent against `/api/v1/pods?fieldSelector=spec.nodeName=$NODE`,
done with plain `net/http` and the mounted service-account token (no client-go, no
extra 30 MB in the binary). Only the configured label/annotation keys are kept, per
pod UID. They travel on the section header (`"l":{…}`), are stored on the stream row
(`streams.labels`, JSON) and inside every frame's meta and the chunk footer, so
`--rebuild-index` restores them. Deleted pods stay in the cache for two minutes so
their last lines are still labelled. Selector: `label=key:value` on every read API.

### 2.7 Multiline (optional)

Per-container state machine: a line that does **not** match `multiline.startPattern`
is appended to the previous line (`\n` joined). A group is emitted when the next start
line arrives, when it reaches `maxLines`, when no line has joined it for `timeout`
(checked at every batch interval), or when its file disappears. Size-triggered batch
flushes leave the open group pending, so a trace is never split by batching. Applied
after CRI partial reassembly. The default start pattern covers ISO timestamps, JSON,
`[...]`, `LEVEL `, klog `I1004 `, logfmt `time=` and IPv4-first access logs.

### 2.8 Resource discipline

- `GOMEMLIMIT` is set by the chart from the container memory limit (agent and hub).
- One goroutine per tailed file, sequential 64 KiB buffered reads, 250 ms poll at EOF.
- No per-line allocation of maps; lines are appended to a reusable `[]byte` batch buffer.
- Per-container `rateLimit.linesPerSecondPerContainer` (token bucket) protects the hub
  from a single log-spamming pod; dropped counts are reported inline.
- Backpressure is explicit: at most `ship.maxInFlight` batches are queued for the
  shipper; when it is behind, the batcher blocks and every tailer **pauses** instead of
  reading files into memory (Vector's 0.55 regression, where backlog was read into RAM, is the failure
  this avoids). Files on disk are the buffer; kubelet rotation bounds the loss window.

## 3. Hub

### 3.1 Storage layout

```
/data
├── index.sqlite               # streams + chunks catalog (SQLite, WAL mode); derived state
├── wal/NNNNNN.wal             # write-ahead log segments, fsynced before every ack
├── days/
│   └── 20260930/                                   # UTC day partition
│       └── <cluster>/<namespace>/<pod-uid>/<container>/
│           ├── <unixnano>.chunk    # sealed, immutable
│           ├── <unixnano>.chunk
│           └── <unixnano>.open     # currently appended
└── cache/                     # local copies of chunks fetched back from object storage
```

A **stream** is `(cluster, namespace, pod, pod_uid, container)`. Pod name is kept for the
UI; `pod_uid` makes a recreated pod with the same name a new stream.

Chunks live under the UTC day of their **first** line. Day partitioning (same idea as
VictoriaLogs `partitions/YYYYMMDD` and Loki's 24 h index periods) makes age-based
retention a directory removal and keeps the index small. A chunk can spill at most
`segment.maxOpenAge` (5 min) past midnight; queries use the index's `min_ts/max_ts`,
not the directory, so this is invisible, and deleting a day removes at most those
5 minutes of the following day.

### 3.2 Write path: WAL and memtables

```
push ──► WAL append + fdatasync ──► cursor ──► memtable[stream] ──► 204 to agent
                                                    │
                     ≥ 256 KiB or ≥ 30 s idle ──────┴──► one zstd frame appended to the
                                                          stream's open chunk (+ bloom)
WAL segment ≥ 64 MiB ──► flush every memtable, start a new segment, delete old ones
```

A push is acknowledged once its WAL record is on disk, so the agent may advance its
checkpoint. Lines then wait in memory until a stream has enough of them to make a
frame worth compressing; a quiet pod that logs once a minute gets one frame per
30 s window instead of one per push, which is what keeps disk usage at raw ÷ 5–8
regardless of how chatty a pod is. Live tail is served from the push path directly,
so buffering adds no latency there. Queries read memtables alongside chunks.

Every WAL record carries a global sequence number; every frame records the highest
sequence it contains (`w`) and the stream row keeps the highest flushed one
(`streams.wseq`). On startup the hub recovers open chunks, loads those marks, and
replays the WAL skipping records at or below a stream's mark: no line is lost, none
is duplicated, whether the crash happened before or after a flush. Memory for the
memtables is bounded (`memtable.maxBytes`, largest streams flush first) and the WAL
is bounded by its segment size.

### 3.3 Chunk format

Append-only file made of independent **zstd frames** (one per flush), plus a footer:

```
frame   = { magic u32, metaLen u32, payLen u32, crc32 u32,
            meta: { cluster, key, file_id, [start, end), node, restart, labels, wseq,
                    end_of_stream, min_ts, max_ts, count },
            zstd(payload) }
payload = repeated { u64 ts_nanos, u8 stream, u32 len, bytes msg }
footer  = { v, cluster, key, restart, labels, wseq, last cursor, min_ts, max_ts,
            lines, uncompressed_bytes, frame_table: [ { off, len, min, max, c } … ],
            bloom: 64 KiB trigram bloom (k=4) over lowercased tokens [a-z0-9_]+ }
```

- Sealing = write the footer and rename `.open` → `.chunk`. Sealed chunks are never
  modified, which makes retention a directory removal and object-storage offload a
  plain copy.
- The frame table lets a time-range query skip frames without decompressing them.
- The bloom filter is built over **trigrams of tokens** while the chunk is open (and
  rebuilt from frames on recovery), so it is sound for the grammar's substring
  semantics: if `error` occurs anywhere inside a token, every trigram of `error` is in
  the filter. A query skips a chunk when any positive word/phrase term has a trigram
  the filter lacks. Terms shorter than 3 token characters and regexes cannot be
  decided and fall back to a scan. Cost: 64 KiB per chunk (~1.5 % of a 4 MiB chunk),
  ~1 % false positives at 50 k distinct trigrams.
- Readers validate CRCs frame by frame and truncate an `.open` chunk at the first bad
  frame; stream identity and cursors are inside every frame, so the index is always
  re-derivable (`--rebuild-index`).

### 3.3b Object storage

With `objectStore` on, a background loop copies sealed chunks whose newest line is
older than `uploadAfter` to the bucket (key = path relative to the data dir) and marks
them `remote`. Retention evicts local copies of remote chunks, oldest first, before it
deletes any day. A query that meets an evicted chunk fetches it into `cache/` (bounded
by `cacheBytes`, LRU by access time). Deleting a day deletes its objects.
`--rebuild-index` lists the bucket and reads footers with ranged GETs, so a hub with an
empty PVC and the bucket recovers its whole catalog. The client is ~300 lines of SigV4
(`internal/s3`), tested against an in-memory fake and MinIO.

### 3.4 Index

SQLite (pure-Go `modernc.org/sqlite`, WAL mode, `synchronous=NORMAL`, one writer):

```sql
streams(id PK, cluster, ns, pod, uid, ctr, node, restarts, first_ts, last_ts, ended,
        labels, wseq,   UNIQUE(cluster, ns, pod, uid, ctr))
chunks (id PK, stream_id, path UNIQUE, day, min_ts, max_ts, lines, bytes, remote, local)
cursors(stream_id, file, end_off,   PRIMARY KEY(stream_id, file))
```

Columns added after v0.1 (`labels`, `wseq`, `remote`, `local`) are created with
`ALTER TABLE … ADD COLUMN` at startup, so an old index opens under a new hub.
Everything is small: 10 000 streams × 500 chunks each is a few tens of MB. The
`/streams` tree is built from this table on every call (the UI refreshes it every 30 s).
The index is never the source of truth: `--rebuild-index` recreates it from chunk
footers, frame meta, the WAL and, with offload on, the bucket listing.

### 3.5 Query execution

```
GET /api/v1/query?cluster=prod-eu&namespace=payments&pod=api-*&container=app
    &start=2026-09-30T10:00:00Z&end=now&q=error+!healthz+"user 42"+/timeout\s+\d+ms/
    &limit=500&dir=backward&cursor=…
```

1. Resolve streams from the index (glob on `pod`, exact on the rest).
2. Select chunks whose `[min_ts, max_ts]` intersects `[start, end]`.
3. If `q` has positive literal terms, drop chunks whose trigram bloom says "absent"
   (reported as `skipped_chunks`).
4. Open remaining chunks newest-first, use the frame table to seek, decompress frame,
   apply filter (`term`, `!term`, `"phrase"`, `/regex/`, `key=value` for JSON lines),
   emit until `limit`. Return an opaque `cursor` for the next page.
5. Hard stops: `query.maxScanBytes` (uncompressed bytes decoded) and `query.timeout`.
   The response tells the UI it was truncated so it can narrow the range.

Cost model: a query over one pod for the last hour typically touches 1–3 chunks; a
cluster-wide `error` search over 24 h touches every chunk not excluded by bloom, which
is why bloom filters exist and why `maxScanBytes` is a hard limit.

### 3.6 Live tail

```
GET /api/v1/tail?cluster=…&namespace=…&pod=…&q=…       (Server-Sent Events)
```

- The ingest path publishes each accepted batch onto an in-process bus keyed by
  `stream_id`. Subscribers register a selector (same fields as `/query`).
- Fan-out is **one upstream, many subscribers**: N browsers tailing the same pod cost
  one filter evaluation and N small writes.
- Per-client output is batched every 100 ms and capped at
  `query.tail.maxLinesPerSecondPerClient`; overflow is dropped **on the server** and
  reported as an inline `{"dropped": n}` event, so a spammy pod can never freeze the tab.
- SSE (not WebSocket) because it is one-way, proxy-friendly, the browser reconnects by
  itself, and it needs no extra ingress config. A comment ping every 15 s keeps idle
  proxies from closing the stream; the subscriber's stream set is refreshed every 5 s so
  pods created after the tail started are included.

### 3.7 Retention

Loop every 60 s:

1. Age: delete whole day directories older than the default `maxAge` (one `rm -rf`,
   then a prefix delete in the index). For per-namespace overrides, delete the matching
   `<cluster>/<namespace>` sub-directories inside each remaining day that is older than
   the override's `maxAge`.
2. Size: if total bytes > `maxDiskBytes` (default 90 % of the PVC), delete the oldest
   day directories until under the limit, but always keep the current day (so a cap
   that is too small degrades to "today only", never to zero). Never touch `.open`.
3. Delete stream rows with zero chunks and `last_ts` older than 24 h (UI still shows
   recently-gone pods for a day).
4. With object-store offload on, sealed chunks older than `uploadAfter` are copied to
   the bucket under their path relative to the data dir (plus the configured prefix) and
   marked `remote`; before step 2 deletes any day, local copies of remote chunks are
   evicted oldest-first (§3.3b). Queries read evicted chunks from the bucket through the
   local cache; a higher `ms` in the response is the only visible difference.

### 3.8 Federation

A hub with `federation.peers` fans out `/streams`, `/query`, `/tail`, `/export` and
`/status` to its peers in parallel, merges by timestamp, namespaces peer stream ids as
`<peer>/<id>` (so a `sid=eu/14` request is routed straight to `eu`), and forwards
`X-P10-Hops` to stop loops at three hops. Live tail is relayed by reading each peer's
SSE stream and rewriting ids. Peers authenticate the console with a bearer token,
which may be bound to a viewer role on the peer to limit what the console can see.
Unreachable peers are listed in `peers_failed`; the rest still answer.

### 3.9 Ingest limits

Per request: `limits.maxBatchBytes` (8 MiB uncompressed, 413 above), `limits.maxLineBytes`
(256 KiB; longer lines are truncated with a `…[truncated]` marker). Per cluster:
`limits.perCluster.bytesPerSecond` token bucket → `429` with `Retry-After`; agents back
off and spool, so one noisy spoke cannot starve the others.

### 3.10 Auth

Ingest: global `ingestTokens` (any cluster) or `clusterTokens` (each restricted to
cluster globs; a spoke's leaked token cannot write as another cluster). Read: `roles`
bind basic usernames, OIDC emails or domains, or dedicated API tokens to cluster and
namespace globs; the selector of every read API is post-filtered by the caller's role,
so scoped users cannot enumerate other namespaces through `/streams` either. With no
roles configured everyone authenticated sees everything.

UI/API modes: `none` (behind your own SSO proxy), `basic`, or `oidc` (authorization-code
flow, email/domain allow-list). Sessions are HMAC-SHA256-signed cookies; there is no
server-side session store, so a hub restart does not log anyone out.

### 3.11 UI

Embedded single-page app (one HTML file, vanilla JavaScript, no framework, no build
step), served from the hub binary, no external assets.

- Left: `cluster → namespace → pod → container` tree with live/gone markers and
  restart counts, built from one `/streams` call and refreshed every 30 s. Type-ahead
  filtering is client-side. Gone pods leave the tree 24 h after their last chunk is
  deleted by retention.
- "Agents" page: one row per node with last-seen, lag, spool size, drops.
- Main: virtualized log lines (only the visible rows plus a small margin are in the
  DOM, regardless of result size), stdout/stderr colouring, wrap toggle, JSON
  pretty-print toggle, timestamps in local/UTC/relative.
- Toolbar: time range presets, filter box (same grammar as `q`), follow toggle
  (switches to SSE; the client keeps the newest 20 000 lines), export, permalink (all
  state in the URL).
- Multi-pod tail: select several pods, lines interleaved by timestamp with a colour per
  pod; a restart boundary row is drawn where a stream's restart count changes.

## 4. Failure modes

| Failure | Behaviour |
|---|---|
| Agent restarts | Resumes from the checkpointed offset; at most one batch is re-sent and the hub's cursor dedup drops it. |
| Node reboots | Same as above; positions and the disk buffer live on a hostPath. |
| Hub down | Agent spools to disk up to `buffer.maxBytes`, then drops oldest and reports the count. A push has a bounded deadline (10 s + 1 s/MiB), so a hub pod that vanishes under an open keep-alive connection costs ~10 s, not the 60 s client timeout. |
| Hub crashes mid-write | Acked pushes are in the fsynced WAL and are replayed on start; un-acked ones are re-sent by the agent. A torn frame at the end of an `.open` chunk is truncated at the first bad CRC. No loss, no duplicates (`kill -9` scenario in `make e2e`). |
| PVC full | The disk cap (default 90 % of the PVC) exists to prevent it. If a write still fails the push gets `500`, the agent spools the batch, and ingest resumes once retention or an operator frees space. |
| Kubelet rotates a file | The old inode is drained for `RotateWait` (5 s) after the path points elsewhere, whether the discoverer or the tailer's own inode check noticed first, because the runtime may still write to it until it reopens the path. A path that is briefly absent between rename and reopen does not end the stream; two consecutive misses ~1 s apart do. |
| Kubelet rotates faster than agent reads | Loss is possible only if the agent lags more than `containerLogMaxFiles × containerLogMaxSize` (default 50 MiB per container). Rate limiting and `priorityClassName` keep the agent scheduled and fast. |
| Clock skew between nodes | Timestamps are stamped by the runtime when it reads the pipe (not by the app), with the node's clock; the hub never rewrites them. Interleaved multi-pod views may show skew. |
| Static pods | Their `uid` in the path is the config hash, not an API uid. Harmless: it is still unique per pod spec. |
| Same pod name recreated | Different `pod_uid` → separate stream; the UI groups by name and shows both. |

## 5. Sizing

Measured (v1.0, `bench/run.sh`, Apple M1 Pro laptop, local processes; idle numbers from
a kind node). Details and caveats in `bench/RESULTS.md`.

| Component | Idle (kind, Linux) | 2 000 lines/s | 10 000 lines/s |
|---|---|---|---|
| Agent RSS | 18.5 MiB | 31.3 MiB | 43.8 MiB |
| Agent CPU | ~0 | 0.3 % of a core | 3.4 % |
| Hub RSS | 24 MiB | 57.6 MiB | 100 MiB |
| Hub CPU (ingest only) | ~0 | 2.0 % | 7.2 % |
| Disk | — | raw ÷ 8.7 | raw ÷ 7.3 |

Hub memory = memtables (bounded by `storage.memtable.maxBytes`, 128 MiB default) +
open chunks (64 KiB bloom each) + query buffers (decoded frames, up to `maxScanBytes`
per query). It does not grow with ingest rate beyond the memtable budget.
Example: 30 nodes, 15 GB/day raw, 7 days ≈ 15–20 GB on disk. For reference,
VictoriaMetrics' 2026 collector benchmark at 10 k lines/s measured vlagent at
28 MiB / 0.06 core, Fluent Bit 78 MiB / 0.26, Vector 154 MiB / 0.41.

## 6. Test strategy

| Layer | What | Where |
|---|---|---|
| CRI parser | containerd / CRI-O / docker-json lines, path parsing, `P`/`F` partial-line reassembly | `internal/cri` |
| Tailer | kubelet-style rename + recreate, truncation, delete while reading, resume from checkpoint | `internal/tail` |
| Chunk | frame round trip, torn-tail truncation, footer recovery | `internal/chunk` |
| WAL | append, replay, truncation at a bad record | `internal/wal` |
| Store | ingest + cursor dedup + query + recovery; WAL crash recovery with no loss / no dup; offload → evict → fetch → `--rebuild-index` from the bucket | `internal/store` |
| Bloom / filter | trigram bloom never false-negative; filter grammar | `internal/bloom`, `internal/query` |
| S3 client | SigV4 client against an in-memory fake | `internal/s3` |
| End to end (local) | real agent + hub over a fake `/var/log/pods`: ingest, filter, SSE tail, agent restart, rotation, `kill -9` hub, pod deletion, auth, cluster tokens, roles, rebuild, federation, S3 offload (needs Docker) | `hack/e2e-local.sh`, CI |
| Batcher | multiline join, `multiline.timeout`, flush on stream end, size-triggered flush keeps the open group, rate-limit marker and checkpoint advance over dropped lines | `internal/ship` |
| End to end (kind) | chart install, real kubelet files, enrichment, token consistency, `helm upgrade` keeps data | `hack/e2e-kind.sh`, CI |
| End to end (multi-cluster kind) | 3-node hub cluster + spoke cluster over the docker network: DaemonSet on tainted control plane, cross-cluster queries, enrichment, restart boundaries, 20 KiB partial lines, multiline traces, rate-limit markers, per-cluster tokens, CronJob / Job / init-container logs, hub outage → spool → drain with no loss / no dup | `hack/e2e-multi.sh`, `hack/demo/`, CI |
| Load | 2 k and 10 k lines/s, RSS / CPU / disk | `bench/run.sh` → `bench/RESULTS.md`, manual |

## 7. Repository layout

```
p10logs/
├── cmd/p10logs-agent/       # agent main
├── cmd/p10logs-hub/         # hub main (+ --rebuild-index, --check)
├── internal/
│   ├── cri/                 # CRI + docker-json line parser, path parser, partial-line merger
│   ├── tail/                # discovery, tailer (rotation, truncation, deletion), positions, gz backfill
│   ├── ship/                # batcher (multiline, rate limit, sections), HTTP client, spool, shipper
│   ├── agent/               # agent config + orchestration
│   ├── wire/                # push format constants/types
│   ├── wal/                 # write-ahead log segments, replay
│   ├── chunk/               # frame writer/reader, footer, crash recovery
│   ├── bloom/               # trigram bloom filter per chunk
│   ├── index/               # SQLite catalog (streams, chunks, cursors)
│   ├── store/               # WAL + memtables, dedup, seal, query, retention, offload, rebuild
│   ├── s3/                  # minimal SigV4 object-store client (+ in-memory fake)
│   ├── federation/          # peer fan-out, id routing, hop guard
│   ├── tailbus/             # in-process pub/sub for live tail
│   ├── query/               # filter grammar
│   ├── auth/                # ingest/API tokens, basic, OIDC sessions
│   ├── api/                 # HTTP handlers, SSE, export, status, metrics
│   └── hub/                 # hub config
├── ui/                      # single-file console, embedded with go:embed
├── charts/p10logs/          # Helm chart
├── hack/                    # dev configs, e2e-local.sh, e2e-kind.sh, e2e-multi.sh, kind/ configs, demo/ workloads
└── docs/
```

Language: Go 1.26+, no CGO (`modernc.org/sqlite`, `klauspost/compress/zstd`,
`coreos/go-oidc`). Images: distroless static, linux/amd64 + linux/arm64.
