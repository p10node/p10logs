# Changelog

## 1.1.0 — 2026-10-04

Real-cluster readiness. No format or API changes.

- Multi-node, multi-cluster test environment on kind: `make e2e-multi` builds the images,
  creates a 3-node hub cluster (tainted control plane, hub as NodePort 30080 on the host)
  and a 1-node spoke cluster that pushes over the docker network with a per-cluster token,
  deploys the demo workloads to both, and checks DaemonSet placement on every node,
  cross-cluster queries, label enrichment, restart boundaries, CRI partial-line
  reassembly (20 KiB lines), multiline stack traces, rate-limit markers, token scoping,
  short-lived pods (CronJob, Job with init container) and a hub outage (spoke spools, then
  drains with no loss and no duplicates). It prints the UI URL and credentials.
- Demo workloads in `hack/demo/` (busybox only): JSON API with a sidecar, crash-looping
  worker, nginx-style frontend, 400 lines/s spammer, 20 KiB line writer, Spring-style app
  with stack traces, a CronJob and a Job with an init container, plus a two-container
  payments gateway. `make demo` applies them to the current context.
- Agent, multiline: groups now honour `multiline.timeout` (they were flushed at every
  batch interval, so a trace written across an interval tick was split); a group is
  flushed as soon as its file disappears; size-triggered flushes now work in multiline
  mode (a batch could grow past `batch.maxBytes` / `maxLines` until the next tick). Unit
  tests for the batcher (multiline join, timeout, stream end, size flush, rate-limit
  marker and checkpoint advance).
- Agent, checkpoints: a file is now identified by path, inode **and a hash of its first
  line**. Kubelet deletes a rotated file after gzipping it and the kernel reuses the freed
  inode for the next `0.log`; keyed by path and inode alone, the new file inherited the old
  checkpoint and the hub's per-file cursor, so every new line was dropped as a duplicate
  until the file outgrew the old one, which at 10 MiB rotation never happens. Observed on
  kind and on k3s after ~3 hours (one container silently stopped at its second rotation).
  The fingerprint covers the first line or the first 64 KiB, whichever is shorter (a
  containerd record can be 16 KiB before its first newline). Legacy `path@inode`
  checkpoints are adopted once, so upgrading does not re-send files.
  In-place truncation (CRI-O `log_size_max`) now also starts a new identity instead of
  reusing the key.
- Agent, tailer: after kubelet renames `0.log`, the runtime keeps writing to the old
  inode until it reopens the path. The tailer's own 2 s inode check used to stop reading
  as soon as it saw the path point at a new file, losing those lines about one rotation
  in eight (caught by `make e2e` step 4 under load). It now drains the old inode for the
  rotation grace period, and ends a stream only when the path is still gone on a second
  check ~1 s later (the path is briefly absent between rename and reopen). Regression
  tests in `internal/tail`.
- Agent, shipping: a push now has a bounded deadline (10 s plus 1 s per MiB; dial 5 s,
  response header 10 s). Before, when the hub pod vanished while a keep-alive connection
  was open, the next push hung for the full 60 s client timeout before the agent started
  spooling, and every tailer on the node paused behind it (measured on kind: 64 s, now
  ~10 s). Found by the multi-cluster e2e's hub-outage check.
- Chart: `hub.service.nodePort` (fixed NodePort for kind / dev clusters); the default
  `agent.multiline.startPattern` also recognises klog (`I1004 `), logfmt (`time=`) and
  access logs starting with an IPv4 address, so enabling multiline no longer glues
  system-component lines together.
- UI, sidebar: pods are grouped under their workload (Deployment / StatefulSet / DaemonSet /
  Job / CronJob / Pod) with a kind badge, pod count, restart total and one click to tail
  every container of the workload; workloads with more than five pods (CronJob runs) start
  folded. Under the search box: unselect all (with count), expand all, collapse all, a
  "live" toggle that hides pods that no longer exist, a namespace selector and workload-kind
  chips. The search also matches workload names, kinds and labels.
- Agent, enrichment: when `agent.enrich.enabled`, every stream carries a synthetic
  `_owner` label (`Deployment/api`, `CronJob/report`, …) derived from the pod's
  `ownerReferences` without extra API calls; `label=_owner:Deployment/api` works as a
  selector. Without enrichment the UI guesses the kind from the pod name (DaemonSet and
  Job pods are indistinguishable by name and are shown as `ds·job`).
- UI, log view: only visible rows are rendered. Fixed-height mode was already virtualised;
  wrap mode now keeps a window of 300 rows in the DOM and reveals earlier rows as you
  scroll up (first from memory, then one page from the hub). Rows are rendered once and
  cached; live batches append instead of rebuilding the whole view; the in-memory buffer
  is capped at 10 000 lines and older lines are re-fetched on scroll-up, so a pod logging
  hundreds of lines per second no longer makes the tab lag.
- `hack/incus-k3s.sh`: create N Incus VMs on a host and join them into one k3s cluster
  (used for the first multi-node test outside docker). `make images-tar ARCH=amd64`
  cross-builds for the target nodes.
- API: relative times accept days and weeks (`start=-7d`, `-2w`, `-1.5d`). Go's
  `ParseDuration` rejects `d`, so `-7d` silently became the one-hour default: the UI's
  7d preset returned *fewer* lines than 24h. Unit test for `parseTime`, e2e check with a
  two-day-old line.
- Auth, first run: `ui.mode: basic` is now a login form with a session cookie. Without a
  configured password the hub seeds `admin` / `admin` and forces a new password on the
  first sign-in (stored as PBKDF2-SHA256 in `/data/auth/users.json`, so it survives restarts
  and upgrades); "password" in the header changes it later. A password from
  `hub.auth.ui.basic.password` / `existingSecret` still works and skips onboarding. HTTP
  Basic headers keep working for scripts. The session key is generated once under
  `/data/auth/session.key`, so sessions survive hub restarts. The chart no longer
  generates a random password secret by default.
- UI: in fixed-height mode a message with embedded newlines (a joined stack trace) grew
  its row beyond the 20 px the virtual list assumes, so the scroll height drifted and the
  bottom could not be reached (jumping to it emptied the view and clamped the scroll
  back). Rows are now clamped to one line with newlines shown as ⏎; Wrap mode still shows
  them in full.
- UI: an empty result says when the newest line of the selection was and offers the
  range that covers it ("No lines in the last 1h · newest 1h 21m ago · Show 6h").
- UI: the address bar always equals the permalink (selection, filter, range, follow, wrap,
  JSON, timestamp mode, tab), so copying the URL works as well as the Permalink button;
  wrap / JSON / timestamp mode are restored from the URL on load.
- UI: a round ⤓ button in the bottom-right corner of the log view appears whenever you are
  scrolled up and jumps back to the newest line (also the End key); the "↓ N new lines"
  pill keeps counting what arrived meanwhile.
- UI: the "Pick a container on the left" overlay never went away because its `display:grid`
  rule beat the `hidden` attribute; a global `[hidden]{display:none!important}` fixes it.
- UI: the Agents tab rendered its tiles and table inside the (hidden) stream-tree column
  and left the main area empty; it now spans the full width. Switching tabs updates the
  URL hash, so `#agents` links and reloads land on the right tab.
- `make images-tar` saves both images to one tarball for nodes without registry access
  (`docker load` / `ctr -n k8s.io images import`); `make kind-up`, `make kind-down`.
- `make e2e`: the object-storage step now waits until every chunk is sealed and copied
  before wiping the data dir, so the rebuild-from-bucket check is exact instead of
  timing-dependent; the test hub for that step is killed on exit; test hubs bind
  `127.0.0.1` explicitly (on some macOS hosts IPv4 connections to a dual-stack `:port`
  listener hang in SYN_SENT once Docker networks exist).
- License: copyright notice filled in (2026 p10node), `NOTICE` file, OCI license/source
  labels on both images.
- CI: `kind-multi` job runs the multi-cluster e2e.

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
