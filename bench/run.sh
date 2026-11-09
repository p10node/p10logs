#!/usr/bin/env bash
# Measure agent + hub RSS/CPU at a given ingest rate on this machine.
# Columns: target rate, pods, duration, lines written by the generator, lines the hub ingested,
# seconds the agent needed to drain its spool after the generator stopped, peak RSS / mean CPU.
# usage: bench/run.sh <lines_per_sec> [seconds] [pods]
set -euo pipefail
cd "$(dirname "$0")/.."
RATE=${1:-2000}; SECS=${2:-60}; PODS=${3:-20}
ROOT=$(mktemp -d /tmp/p10logs-bench.XXXX); PORT=$((30000 + RANDOM % 10000))
cleanup(){ kill ${HUB:-} ${AG:-} 2>/dev/null || true; rm -rf "$ROOT"; }
trap cleanup EXIT
mkdir -p "$ROOT/pods" "$ROOT/hub" "$ROOT/agent"
printf 'listen: "127.0.0.1:%s"\ndataDir: %s/hub\nauth: { ingestToken: b, ui: { mode: none } }\n' $PORT "$ROOT" > "$ROOT/hub.yaml"
printf 'cluster: bench\nhub: { url: "http://127.0.0.1:%s", token: b }\npaths: { pods: %s/pods, state: %s/agent }\nhealth: ":0"\n' $PORT "$ROOT" "$ROOT" > "$ROOT/agent.yaml"
./bin/p10logs-hub --config "$ROOT/hub.yaml" 2>/dev/null & HUB=$!
sleep 0.5
./bin/p10logs-agent --config "$ROOT/agent.yaml" 2>/dev/null & AG=$!
sleep 0.5
python3 bench/loadgen.py "$ROOT/pods" $PODS $RATE $SECS > "$ROOT/loadgen.out" &
LG=$!
peak_a=0; peak_h=0; cpu_a=0; cpu_h=0; n=0
while kill -0 $LG 2>/dev/null; do
  sleep 2; n=$((n+1))
  ra=$(ps -o rss= -p $AG | tr -d ' '); rh=$(ps -o rss= -p $HUB | tr -d ' ')
  ca=$(ps -o %cpu= -p $AG | tr -d ' '); ch=$(ps -o %cpu= -p $HUB | tr -d ' ')
  [ "${ra:-0}" -gt "$peak_a" ] && peak_a=$ra; [ "${rh:-0}" -gt "$peak_h" ] && peak_h=$rh
  cpu_a=$(python3 -c "print($cpu_a+${ca:-0})"); cpu_h=$(python3 -c "print($cpu_h+${ch:-0})")
done
# drain: the agent may still be pushing spooled batches; wait until the hub's line count stops moving
ingested(){ curl -s "http://127.0.0.1:$PORT/api/v1/status" | python3 -c 'import json,sys;print(json.load(sys.stdin)["hub"]["ingest_lines_total"])'; }
written=$(sed -n 's/^wrote \([0-9]*\) lines$/\1/p' "$ROOT/loadgen.out")
drain=0; lines=$(ingested)
while [ $drain -lt 180 ]; do
  sleep 3; drain=$((drain+3)); now=$(ingested)
  [ "$now" = "$lines" ] && [ "$now" -ge "${written:-0}" ] && break
  [ "$now" = "$lines" ] && [ $drain -ge 9 ] && break   # stalled below written: report as is
  lines=$now
done
disk=$(du -sk "$ROOT/hub/days" | cut -f1)
raw=$(du -sk "$ROOT/pods" | cut -f1)
python3 - "$RATE" "$SECS" "$PODS" "$peak_a" "$peak_h" "$cpu_a" "$cpu_h" "$n" "$lines" "$disk" "$raw" "${written:-0}" "$drain" <<'PY'
import sys
rate,secs,pods,pa,ph,ca,ch,n,lines,disk,raw,written,drain=[float(x) for x in sys.argv[1:]]
print(f"| {int(rate):>6} | {int(pods):>4} | {int(secs):>3}s | {int(written):>8} | {int(lines):>8} | {int(drain):>3}s | {pa/1024:6.1f} MiB | {ca/n:5.1f}% | {ph/1024:6.1f} MiB | {ch/n:5.1f}% | {raw/1024:6.1f} MiB | {disk/1024:6.1f} MiB | {raw/max(disk,1):4.1f}x |")
PY
