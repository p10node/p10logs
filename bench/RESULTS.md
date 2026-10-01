# Benchmark results

Method: `bench/run.sh <lines/s> <seconds> <pods>` starts one hub and one agent as local
processes, then `bench/loadgen.py` writes CRI-format JSON lines (~180 bytes) into a fake
`/var/log/pods` tree at the target rate, rotating files at 10 MiB the way kubelet does.
RSS is the peak resident set sampled every 2 s with `ps`; CPU is the mean `%cpu` over the
run (100 % = one core). "Ratio" is raw log bytes divided by bytes on the hub's disk.

Numbers below are from a developer laptop (see the machine line), not from a Linux
container with cgroup limits; treat them as relative, and re-run on your own hardware.

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
