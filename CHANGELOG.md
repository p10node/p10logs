# Changelog

## 1.0.0 — 2026-10-01

Format and API freeze (see `docs/FORMATS.md`).

- Storage v2: write-ahead log + per-stream memtables. Pushes are fsynced to the WAL and
  acknowledged, then written to chunks as large frames (≥ 256 KiB or 30 s), so quiet
  pods no longer produce one tiny frame per second. `kill -9` recovery replays the WAL
  with per-stream high-water marks: no loss, no duplicates.
- Federation: a hub can front other hubs. `/streams`, `/query`, `/tail`, `/export`,
  `/status` fan out and merge by timestamp; peer stream ids are `<peer>/<id>`; loops
  are stopped by `X-P10-Hops`.
- Object-storage offload: sealed chunks older than `uploadAfter` are copied to any
  S3-compatible bucket (built-in SigV4 client, no SDK), local copies are evicted first
  when the disk cap is hit, queries fetch cold chunks transparently, `--rebuild-index`
  lists the bucket.
- Per-cluster ingest tokens (`auth.clusterTokens`) and viewer roles
  (`auth.roles`: users / OIDC domains / API tokens scoped to cluster and namespace globs).
- `p10logs-hub --check` verifies chunks, WAL and index.
- Helm: `values.schema.json`, Artifact Hub metadata, upgrade path tested on kind.
- Docs site (mkdocs) and this changelog.

## 0.2.0 — 2026-10-01

- Label/annotation enrichment via one node-scoped pod watch (no client-go);
  `label=key:value` selectors.
- Trigram bloom filter per chunk; `skipped_chunks` in query stats.
- Fixed: Helm rendered two different ingest tokens (agent 401 forever); spool now
  drains on a timer; heartbeat failures are logged.
- CI (unit, local e2e, chart, kind) and release (multi-arch images, OCI chart) workflows.
- Benchmarks in `bench/`.

## 0.1.0 — 2026-09-30

- First working release: agent, hub, UI, Helm chart.
