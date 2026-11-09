# Benchmark results

Method: `bench/run.sh <lines/s> <seconds> <pods>` starts one hub and one agent as local
processes, then `bench/loadgen.py` writes CRI-format JSON lines (~180 bytes) into a fake
`/var/log/pods` tree at the target rate, rotating files at 10 MiB the way kubelet does.
RSS is the peak resident set sampled every 2 s with `ps`; CPU is the mean `%cpu` over the
run (100 % = one core). "Ratio" is raw log bytes divided by bytes on the hub's disk.

Numbers below are from a developer laptop (see the machine line), not from a Linux
container with cgroup limits; treat them as relative, and re-run on your own hardware.

## 2026-11-09 · v1.1.6 · Linux VM, 4 vCPU Xeon E5-2683 v4 @ 2.1 GHz, 4 GiB, Ubuntu 24.04.5 (kernel 6.8)

Incus/KVM virtual machine on a shared host; agent, hub and the generator run as local
processes inside the VM, nothing else on it. `bench/run.sh` now also reports the lines the
generator wrote, the lines the hub ingested, and the seconds the agent needed to drain its
disk spool after the generator stopped. CPU is the mean of `ps %cpu`, which on Linux is
cumulative CPU time over elapsed time.

| lines/s | pods | dur | written | ingested | drain | agent RSS | agent CPU | hub RSS | hub CPU | raw | on disk | ratio |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
|   2000 |   20 |  60s |   119500 |   119500 |   6s |   26.6 MiB |   4.1% |   49.6 MiB |   6.2% |   28.2 MiB |    4.3 MiB |  6.6x |
|  10000 |   50 |  60s |   598000 |   598000 |   3s |   39.8 MiB |  14.3% |   62.4 MiB |  33.7% |  141.0 MiB |   22.5 MiB |  6.3x |
|  30000 |  100 |  60s |  1795500 |  1795500 |  33s |   80.3 MiB |  22.6% |  106.8 MiB |  33.5% |  423.7 MiB |   71.9 MiB |  5.9x |

Notes:

- RSS is lower than on macOS (Go's allocator returns memory to Linux more eagerly) and
  CPU is higher: one of these 2.1 GHz Xeon vCPUs does roughly a third of the work of an
  M1 Pro core per second.
- At 30 000 lines/s the generator kept its rate (29.9 k lines/s written), the agent shipped
  about 15 k lines/s live and put the rest in its disk spool (`agent.buffer`, default
  256 MiB), then drained the backlog in 33 s once the generator stopped. Nothing was lost
  or duplicated (written = ingested); the backlog is visible as `spool_bytes` on the
  agent's `/status` and on the Agents page. The live ceiling of one agent comes from the
  batch shape, not CPU: a batch is at most 1 MiB / 5 000 lines (`batch.maxBytes`,
  `batch.maxLines`) and the shipper pushes batches one at a time, so the ceiling is one
  batch per push round trip (zstd + HTTP + the hub's WAL fsync, ~330 ms on this VM's
  disk); `ship.maxInFlight` (default 4) only bounds how many finished batches may queue
  for the shipper before the reader pauses. Raise `batch.maxLines` / `batch.maxBytes` if a
  single node really produces more than ~15 k lines/s. The hub stayed at a third of one
  vCPU throughout, so ingest was not its bottleneck.
- The compression ratio is lower than in the 2026-10-01 rows (5.9–6.6× vs 7.3–8.7×) and
  more honest: those rows measured `hub/days` 3 s after the generator stopped, while part
  of the data was still in the memtable and WAL; the drain wait now lets the hub seal
  what it received before the directory is measured.

### Steady state on a real cluster · k3s v1.36, 3 nodes, 34 days

p10logs 1.1.2 (agent and hub core are unchanged through 1.1.6; later releases changed the
UI, chart and dependencies) on the Incus k3s cluster used for development: 3 VMs with
4 vCPU / 8 GiB each, demo workloads from `hack/demo/` (JSON API + sidecar, crash loops with
Go panics, nginx access logs, a 400 lines/s spammer, 20 KiB lines, Java stack traces,
CronJob and Job pods), enrichment and multiline on, 8-day retention. Read from
`/api/v1/status`, `/proc/<pid>/status` on the nodes and `kubectl top` on 2026-11-09.

| | |
|---|---|
| Uptime | 33.8 days, 0 restarts (hub and all 3 agents) |
| Ingested | 651.7 M lines, 67.4 GB raw since start (~2.0 GB/day; 223 lines/s at the time of reading) |
| Catalog | 5 404 streams, 7 209 chunks, 8 days kept |
| On disk | 1.88 GB for the 8 retained days (~16 GB raw → ≈8.5×), WAL 49 MB |
| Hub RSS | 71 MiB at the time of reading, 218 MiB peak (`VmHWM`); `kubectl top` 229 Mi includes page cache. Chart defaults: request 128 Mi, limit 1 Gi |
| Hub CPU | 25 mCPU (`kubectl top`) |
| Agent RSS | 15, 28 and 29 MiB on the three nodes, 32 MiB peak; 14–26 mCPU each |

The hub peak comes from the memtable budget (`storage.memtable.maxBytes`, default
128 MiB) plus WAL replay buffers after the one restart boundary the cluster saw at
install time; steady-state RSS sits well below the 128 Mi request.

## 2026-10-01 · v0.2 · Apple M1 Pro, 16 GB, macOS 27 (local processes, no cgroup limits)

| lines/s | pods | dur | ingested | agent RSS | agent CPU | hub RSS | hub CPU | raw | on disk | ratio |
|---|---|---|---|---|---|---|---|---|---|---|
|   2000 |   20 |  60s |   111900 |   31.5 MiB |   0.9% |   32.0 MiB |   2.1% |   40.1 MiB |    5.1 MiB |  7.9x |
|  10000 |   50 |  60s |   561500 |   42.6 MiB |   4.6% |   42.9 MiB |   9.5% |  150.2 MiB |   28.0 MiB |  5.4x |

Notes: the generator's JSON lines are ~180 bytes with a small vocabulary, so the
compression ratio is optimistic for very diverse logs and pessimistic for repetitive
ones. Hub CPU includes one `fdatasync` per push (≈ 1 push/s/agent); queries and tail
clients are not included. Image sizes: agent 16 MB, hub 23 MB (distroless).

### Idle on a kind node (Linux, distroless images, v0.2)

Read from `/proc/<pid>/status` inside the kind node after the e2e run (11 files tailed,
19 streams, a few hundred lines ingested):

| process | VmRSS |
|---|---|
| p10logs-agent | 18.5 MiB |
| p10logs-hub | 23.6 MiB |

## 2026-10-01 · v1.0 (WAL + memtable storage) · same machine

| lines/s | pods | dur | ingested | agent RSS | agent CPU | hub RSS | hub CPU | raw | on disk | ratio |
|---|---|---|---|---|---|---|---|---|---|---|
|   2000 |   20 |  60s |   107200 |   31.3 MiB |   0.3% |   57.6 MiB |   2.0% |   40.1 MiB |    4.6 MiB |  8.7x |
|  10000 |   50 |  60s |   539500 |   43.8 MiB |   3.4% |  100.3 MiB |   7.2% |  150.2 MiB |   20.7 MiB |  7.3x |

Compared with v0.2: frames are now ≥ 256 KiB (or 30 s of a stream) instead of one per
push, so compression improved from 5.4–7.9× to 7.3–8.7× and would improve far more on
real clusters with many quiet pods. Hub RSS grew by the memtable budget in use
(bounded by `storage.memtable.maxBytes`, default 128 MiB) plus WAL buffers; agent CPU
dropped because the hub acknowledges faster (one WAL fsync per push, no per-stream
chunk fsync).
