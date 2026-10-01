#!/usr/bin/env bash
# Local end-to-end: real hub + real agent over a fake /var/log/pods tree.
# Verifies: ingest, query, filter, tail (SSE), agent restart (no dup / no loss),
# kubelet-style rotation, hub restart recovery, gz backfill, pod deletion → stream ended.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$(mktemp -d /tmp/p10logs-e2e.XXXX)
PODS=$ROOT/pods; HUBD=$ROOT/hub; AGD=$ROOT/agent
mkdir -p "$PODS" "$HUBD" "$AGD"
PORT=$((20000 + RANDOM % 20000))
TOKEN=e2e-token
cleanup(){ kill ${HUB_PID:-} ${AG_PID:-} ${HUBB_PID:-} 2>/dev/null || true; rm -rf "$ROOT"; }
trap cleanup EXIT
pass(){ echo "  ✔ $1"; }
fail(){ echo "  ✘ $1"; exit 1; }
# CRI line generator (portable: macOS date has no %N). usage: gen N "text %d" [stderr]
gen(){ python3 - "$1" "$2" "${3:-stdout}" <<'PY'
import sys,time
n,fmt,stream=int(sys.argv[1]),sys.argv[2],sys.argv[3]
for i in range(1,n+1):
    t=time.time_ns()+i  # strictly increasing
    print(time.strftime("%Y-%m-%dT%H:%M:%S",time.gmtime(t//10**9))+".%09dZ %s F %s"%(t%10**9,stream,fmt.replace("%d",str(i))))
PY
}
line(){ gen 1 "$1"; }

cat > "$ROOT/hub.yaml" <<Y
listen: ":$PORT"
dataDir: $HUBD
auth:
  ingestToken: $TOKEN
  apiTokens: [fedtok]
  clusterTokens: [ { token: ctok, clusters: [e2e] } ]
  roles:
    - { name: admin, users: [u] }
    - { name: payments-only, apiTokens: [paytok], namespaces: [payments] }
  ui: { mode: basic, basic: { username: u, password: p } }
storage: { segment: { targetBytes: 65536, maxOpenAge: 5m } }
Y
cat > "$ROOT/agent.yaml" <<Y
cluster: e2e
hub: { url: "http://127.0.0.1:$PORT", token: $TOKEN }
paths: { pods: $PODS, state: $AGD }
backfill: { compressed: true }
batch: { flushInterval: 200ms }
heartbeatInterval: 1s
health: ":0"
Y
start_hub(){ ./bin/p10logs-hub --config "$ROOT/hub.yaml" 2>"$ROOT/hub.log" & HUB_PID=$!; for i in $(seq 1 50); do curl -fs "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1 && return; sleep 0.1; done; cat "$ROOT/hub.log"; fail "hub did not start"; }
start_agent(){ ./bin/p10logs-agent --config "$ROOT/agent.yaml" 2>>"$ROOT/agent.log" & AG_PID=$!; }
q(){ curl -fs -u u:p "http://127.0.0.1:$PORT/api/v1/query?$1"; }
count(){ q "$1" | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["lines"]))'; }
wait_count(){ for i in $(seq 1 60); do [ "$(count "$1")" = "$2" ] && return 0; sleep 0.25; done; echo "got $(count "$1") want $2"; q "$1" | head -c 600; echo; return 1; }

echo "1. ingest + query"
P1=$PODS/payments_api-1_uid-0001/app; mkdir -p "$P1"
gen 50 "req %d GET /v1/charges 200" > "$P1/0.log"
line '{"level":"error","msg":"boom","user":42}' >> "$P1/0.log"
# gz backfill fixture: a rotated file that kubelet already compressed
{ line "old rotated line 1"; line "old rotated line 2"; } | gzip > "$P1/0.log.20260929-100000.gz"
start_hub; start_agent
wait_count "namespace=payments&start=-1h&limit=1000" 53 && pass "53 lines (50 live + 1 json + 2 gz backfill)" || fail "ingest"
[ "$(count 'namespace=payments&start=-1h&q=level%3Derror')" = 1 ] && pass "filter level=error" || fail "filter"
[ "$(count 'namespace=payments&start=-1h&q=rotated')" = 2 ] && pass "gz backfill searchable" || fail "backfill"

echo "2. live tail (SSE)"
( curl -sN -u u:p "http://127.0.0.1:$PORT/api/v1/tail?namespace=payments" > "$ROOT/tail.out" & echo $! > "$ROOT/tail.pid" )
sleep 0.5
line "tail me please" >> "$P1/0.log"
for i in $(seq 1 40); do grep -q "tail me please" "$ROOT/tail.out" 2>/dev/null && break; sleep 0.1; done
grep -q "tail me please" "$ROOT/tail.out" && pass "SSE delivered new line" || fail "tail"
kill "$(cat "$ROOT/tail.pid")" 2>/dev/null || true

echo "3. agent restart: no loss, no duplicates"
kill -TERM $AG_PID; wait $AG_PID 2>/dev/null || true
gen 20 "after restart %d" >> "$P1/0.log"
start_agent
wait_count "namespace=payments&start=-1h&limit=1000" 74 && pass "74 lines exactly once across agent restart" || fail "agent restart"

echo "4. kubelet-style rotation"
mv "$P1/0.log" "$P1/0.log.20260930-120000"
line "written to rotated file after rename" >> "$P1/0.log.20260930-120000"
gen 5 "new file %d" > "$P1/0.log"
wait_count "namespace=payments&start=-1h&limit=1000" 80 && pass "rotation: old tail + new file" || fail "rotation"

echo "5. container restart file + second pod + multi-pod query"
gen 3 "restart one %d" > "$P1/1.log"
P2=$PODS/db_postgres-0_uid-0002/postgres; mkdir -p "$P2"
gen 7 "LOG: checkpoint %d" stderr > "$P2/0.log"
wait_count "cluster=e2e&start=-1h&limit=1000" 90 && pass "cluster-wide 90 lines" || fail "multi"
[ "$(count 'pod=postgres-*&start=-1h')" = 7 ] && pass "glob pod=postgres-*" || fail "glob"

echo "6. hub restart (crash-style, no seal): recovery + dedup"
kill -KILL $HUB_PID; wait $HUB_PID 2>/dev/null || true
start_hub
[ "$(count 'cluster=e2e&start=-1h&limit=1000')" = 90 ] && pass "all 90 lines after hub kill -9" || fail "hub recovery"
gen 4 "post hub restart %d" >> "$P1/0.log"
wait_count "cluster=e2e&start=-1h&limit=1000" 94 && pass "ingest resumes, spool drained, no dups" || fail "post-restart"

echo "7. pod deletion → stream ended, logs kept"
rm -rf "$PODS/db_postgres-0_uid-0002"
for i in $(seq 1 40); do curl -fs -u u:p "http://127.0.0.1:$PORT/api/v1/streams" | grep -q '"live":false' && break; sleep 0.25; done
curl -fs -u u:p "http://127.0.0.1:$PORT/api/v1/streams" | grep -q '"live":false' && pass "pod marked gone" || fail "stream end"
[ "$(count 'pod=postgres-*&start=-1h')" = 7 ] && pass "deleted pod's logs still queryable" || fail "kept"

echo "8. status + agents + export + auth"
curl -fs -u u:p "http://127.0.0.1:$PORT/api/v1/status" | grep -q '"node"' && pass "agent heartbeat visible" || fail "status"
[ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/api/v1/status")" = 401 ] && pass "UI auth enforced (401 without creds)" || fail "auth"
[ "$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'X-P10-Cluster: x' "http://127.0.0.1:$PORT/api/v1/push")" = 401 ] && pass "ingest auth enforced" || fail "ingest auth"
n=$(curl -fs -u u:p "http://127.0.0.1:$PORT/api/v1/export?cluster=e2e&start=-1h" | wc -l | tr -d ' ')
[ "$n" = 94 ] && pass "export 94 lines" || fail "export got $n"

echo "8b. per-cluster ingest tokens + viewer roles"
[ "$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Authorization: Bearer ctok' -H 'X-P10-Cluster: e2e' "http://127.0.0.1:$PORT/api/v1/push")" = 204 ] && pass "cluster token accepted for its cluster" || fail "cluster token"
[ "$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Authorization: Bearer ctok' -H 'X-P10-Cluster: other' "http://127.0.0.1:$PORT/api/v1/push")" = 401 ] && pass "cluster token rejected for another cluster" || fail "cluster token scope"
np=$(curl -fs -H 'Authorization: Bearer paytok' "http://127.0.0.1:$PORT/api/v1/query?cluster=e2e&start=-1h&limit=1000" | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["lines"]))')
nd=$(curl -fs -H 'Authorization: Bearer paytok' "http://127.0.0.1:$PORT/api/v1/query?pod=postgres-*&start=-1h&limit=1000" | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["lines"]))')
[ "$np" = 87 ] && [ "$nd" = 0 ] && pass "role payments-only sees 87 payments lines, 0 db lines" || fail "roles: payments=$np db=$nd"
curl -fs -H 'Authorization: Bearer paytok' "http://127.0.0.1:$PORT/api/v1/streams" | grep -q '"db"' && fail "role leaks db namespace in streams" || pass "role hides db namespace in streams"

echo "9. rebuild-index from files"
kill -TERM $HUB_PID; wait $HUB_PID 2>/dev/null || true
rm -f "$HUBD/index.sqlite"*
./bin/p10logs-hub --config "$ROOT/hub.yaml" --rebuild-index 2>>"$ROOT/hub.log"
start_hub
[ "$(count 'cluster=e2e&start=-1h&limit=1000')" = 94 ] && pass "index rebuilt: 94 lines" || fail "rebuild"
echo "10. federation: console hub B federating A"
PORT2=$((PORT + 1))
cat > "$ROOT/hubB.yaml" <<Y
listen: ":$PORT2"
dataDir: $ROOT/hubB
auth: { ingestToken: other, ui: { mode: none } }
federation: { enabled: true, peers: [ { name: a, url: "http://127.0.0.1:$PORT", token: fedtok } ] }
Y
./bin/p10logs-hub --config "$ROOT/hubB.yaml" 2>"$ROOT/hubB.log" & HUBB_PID=$!
for i in $(seq 1 50); do curl -fs "http://127.0.0.1:$PORT2/healthz" >/dev/null 2>&1 && break; sleep 0.1; done
nb=$(curl -fs "http://127.0.0.1:$PORT2/api/v1/query?cluster=e2e&start=-1h&limit=1000" | python3 -c 'import json,sys;d=json.load(sys.stdin);ls=d["lines"];print(len(ls), all(l["sid"].startswith("a/") for l in ls))')
[ "$nb" = "94 True" ] && pass "query via B returns A's 94 lines with sid a/…" || fail "federated query: $nb"
curl -fs "http://127.0.0.1:$PORT2/api/v1/streams" | grep -q '"hub":"a"' && pass "streams via B tagged hub=a" || fail "federated streams"
sid=$(curl -fs "http://127.0.0.1:$PORT2/api/v1/streams" | python3 -c 'import json,sys;d=json.load(sys.stdin);print([x["id"] for c in d["clusters"] for n in c["namespaces"] for p in n["pods"] if p["name"]=="api-1" for x in p["containers"]][0])')
n=$(curl -fs "http://127.0.0.1:$PORT2/api/v1/query?sid=$sid&start=-1h&limit=1000" | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["lines"]))')
[ "$n" -gt 80 ] && pass "query by routed sid $sid ($n lines)" || fail "sid routing got $n"
( curl -sN "http://127.0.0.1:$PORT2/api/v1/tail?namespace=payments" > "$ROOT/tailB.out" & echo $! > "$ROOT/tailB.pid" )
sleep 0.7
line "federated tail line" >> "$P1/0.log"
for i in $(seq 1 50); do grep -q "federated tail line" "$ROOT/tailB.out" 2>/dev/null && break; sleep 0.1; done
grep -q '"sid":"a/' "$ROOT/tailB.out" && grep -q "federated tail line" "$ROOT/tailB.out" && pass "tail via B relays A's live lines" || fail "federated tail: $(head -c 300 "$ROOT/tailB.out")"
kill "$(cat "$ROOT/tailB.pid")" 2>/dev/null || true
curl -fs "http://127.0.0.1:$PORT2/api/v1/status" | grep -q '"name":"a","ok":true' || curl -fs "http://127.0.0.1:$PORT2/api/v1/status" | grep -q '"ok":true' && pass "status via B shows peer a ok" || fail "federated status"
[ "$(curl -s -o /dev/null -w '%{http_code}' -H 'X-P10-Hops: 9' "http://127.0.0.1:$PORT2/api/v1/status")" = 508 ] && pass "hop guard" || fail "hop guard"
n=$(curl -fs "http://127.0.0.1:$PORT2/api/v1/export?cluster=e2e&start=-1h" | wc -l | tr -d ' ')
[ "$n" = 95 ] && pass "export via B: 95 lines" || fail "federated export got $n"
kill $HUBB_PID 2>/dev/null || true

if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 && [ -z "${SKIP_MINIO:-}" ]; then
echo "11. object storage offload (S3-compatible: versitygw, real SigV4)"
MPORT=$((PORT + 2)); MC=p10logs-e2e-s3
docker rm -f $MC >/dev/null 2>&1 || true
docker run -d --rm --name $MC -p $MPORT:7070 -e ROOT_ACCESS_KEY=minio -e ROOT_SECRET_KEY=minio123 versity/versitygw:latest posix /tmp >/dev/null
trap 'docker rm -f $MC >/dev/null 2>&1 || true; cleanup' EXIT
for i in $(seq 1 120); do curl -s -o /dev/null "http://127.0.0.1:$MPORT/" 2>/dev/null && break; sleep 0.5; done
# the hub creates the bucket itself at startup (EnsureBucket)
PORT3=$((PORT + 3))
cat > "$ROOT/hubC.yaml" <<Y
listen: ":$PORT3"
dataDir: $ROOT/hubC
auth: { ingestToken: ctok3, ui: { mode: none } }
storage:
  segment: { targetBytes: 1024, maxOpenAge: 1s }
  memtable: { flushBytes: 512, flushAge: 1s }
  objectStore: { enabled: true, endpoint: "http://127.0.0.1:$MPORT", bucket: p10logs, pathStyle: true, accessKey: minio, secretKey: minio123, uploadAfter: 1s, interval: 2s }
Y
./bin/p10logs-hub --config "$ROOT/hubC.yaml" 2>"$ROOT/hubC.log" & HUBC_PID=$!
for i in $(seq 1 50); do curl -fs "http://127.0.0.1:$PORT3/healthz" >/dev/null 2>&1 && break; sleep 0.1; done
# push 3 batches directly (no agent) so chunks seal quickly
for b in 1 2 3; do
  printf '{"k":"cold/pod/uid-c/app","f":"f","o":[%d,%d],"r":0}\n' $((b*1000)) $((b*1000+1000)) > "$ROOT/batch.ndjson"
  for i in $(seq 1 12); do printf '{"t":%d,"s":"o","m":"cold batch %d line %d padding-padding-padding-padding-padding"}\n' "$(python3 -c 'import time;print(time.time_ns())')" $b $i >> "$ROOT/batch.ndjson"; done
  curl -fs -o /dev/null -X POST -H 'Authorization: Bearer ctok3' -H 'X-P10-Cluster: cold' --data-binary @"$ROOT/batch.ndjson" "http://127.0.0.1:$PORT3/api/v1/push"
  sleep 1.5
done
ok=0; for i in $(seq 1 40); do r=$(curl -s "http://127.0.0.1:$PORT3/api/v1/status" | python3 -c 'import json,sys;print(json.load(sys.stdin)["hub"]["remote_chunks"])'); [ "${r:-0}" -ge 1 ] && ok=1 && break; sleep 0.5; done
[ $ok = 1 ] && pass "chunks offloaded to S3 ($r)" || fail "offload: $(tail -3 "$ROOT/hubC.log")"
nchunks=$(find "$ROOT/hubC/days" -name '*.chunk' | wc -l | tr -d ' ')
find "$ROOT/hubC/days" -name '*.chunk' -delete
n=$(curl -fs "http://127.0.0.1:$PORT3/api/v1/query?cluster=cold&start=-1h&q=cold&limit=100" | python3 -c 'import json,sys;d=json.load(sys.stdin);print(len(d["lines"]))')
[ "$n" = 36 ] && pass "all 36 lines readable with local chunk files deleted (fetched from S3)" || fail "cold read got $n of 36 (sealed chunks: $nchunks)"
kill -TERM $HUBC_PID; wait $HUBC_PID 2>/dev/null || true
rm -rf "$ROOT/hubC"; mkdir -p "$ROOT/hubC"
./bin/p10logs-hub --config "$ROOT/hubC.yaml" --rebuild-index 2>>"$ROOT/hubC.log"
./bin/p10logs-hub --config "$ROOT/hubC.yaml" 2>>"$ROOT/hubC.log" & HUBC_PID=$!
for i in $(seq 1 50); do curl -fs "http://127.0.0.1:$PORT3/healthz" >/dev/null 2>&1 && break; sleep 0.1; done
n=$(curl -fs "http://127.0.0.1:$PORT3/api/v1/query?cluster=cold&start=-1h&limit=100" | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["lines"]))')
[ "$n" -ge 24 ] && pass "empty data dir + bucket → --rebuild-index recovers $n lines" || fail "rebuild from bucket got $n"
./bin/p10logs-hub --config "$ROOT/hubC.yaml" --check 2>/dev/null | grep -q '"ok":true' && pass "--check ok" || fail "--check"
kill $HUBC_PID 2>/dev/null || true
docker rm -f $MC >/dev/null 2>&1 || true
else
echo "11. object storage (skipped: docker unavailable)"
fi
echo "ALL E2E PASSED"
