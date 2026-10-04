#!/usr/bin/env bash
# Multi-node + multi-cluster end-to-end on kind, as close to a real deployment as docker gets:
#
#   kind "p10logs-hub"   : 1 control-plane (tainted) + 2 workers. Hub (NodePort 30080 → host) + agents.
#   kind "p10logs-spoke" : 1 node, agents only, pushes to the hub over the docker network with a
#                          per-cluster token. Multiline reassembly on.
#   hack/demo/           : workloads on both clusters (JSON, crash loops, stack traces, 20 KiB lines,
#                          a 400 lines/s spammer, a CronJob, a Job with an init container).
#
# Checks: DaemonSet on every node incl. the tainted control plane, agents from both clusters
# heartbeat, cross-cluster queries, label enrichment, restart boundaries, CRI partial-line
# reassembly, multiline stack traces, per-container rate limit markers, per-cluster token
# enforcement, short-lived pods kept after deletion, and a hub outage: spoke spools to disk,
# then drains with no loss and no duplicates.
#
# Needs docker, kind, helm, kubectl, curl, python3. Leaves both clusters running and prints
# the UI URL and credentials at the end. Tear down: make kind-down
set -euo pipefail
cd "$(dirname "$0")/.."
HUB=${HUB_CLUSTER:-p10logs-hub}
SPOKE=${SPOKE_CLUSTER:-p10logs-spoke}
PORT=${HUB_PORT:-30080}
VER=$(awk '/^version:/{print $2}' charts/p10logs/Chart.yaml)
KH="kubectl --context kind-$HUB"
KS="kubectl --context kind-$SPOKE"
pass(){ echo "  ✔ $1"; }
fail(){ echo "  ✘ $1"; echo "--- hub pods"; $KH -n p10logs get pods -o wide || true; echo "--- spoke agent log"; $KS -n p10logs logs ds/p10logs-agent --tail=30 || true; exit 1; }
t0=$(date +%s)

echo "1. images ($VER)"
docker build -q --build-arg VERSION=$VER -f Dockerfile.agent -t ghcr.io/p10node/p10logs-agent:$VER . >/dev/null
docker build -q --build-arg VERSION=$VER -f Dockerfile.hub   -t ghcr.io/p10node/p10logs-hub:$VER   . >/dev/null
pass "built"

echo "2. kind clusters"
kind get clusters 2>/dev/null | grep -qx "$HUB"   || kind create cluster --config hack/kind/hub.yaml   --wait 180s >/dev/null
kind get clusters 2>/dev/null | grep -qx "$SPOKE" || kind create cluster --config hack/kind/spoke.yaml --wait 120s >/dev/null
kind load docker-image --name "$HUB"   ghcr.io/p10node/p10logs-agent:$VER ghcr.io/p10node/p10logs-hub:$VER >/dev/null
kind load docker-image --name "$SPOKE" ghcr.io/p10node/p10logs-agent:$VER >/dev/null
HUB_NODES=$($KH get nodes --no-headers | wc -l | tr -d ' ')
HUB_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$HUB-control-plane")
[ "$HUB_NODES" = 3 ] && pass "hub cluster: 3 nodes (control-plane $HUB_IP)" || fail "hub cluster has $HUB_NODES nodes, want 3"
$KH get nodes -o jsonpath='{.items[?(@.metadata.labels.node-role\.kubernetes\.io/control-plane=="")].spec.taints[*].key}' | grep -q control-plane && pass "control-plane node is tainted" || echo "  · control-plane not tainted (kind default for this version)"

echo "3. hub cluster: hub + agents (NodePort $PORT)"
RERUN_HUB=0; RERUN_SPOKE=0
helm status p10logs -n p10logs --kube-context "kind-$HUB" >/dev/null 2>&1 && RERUN_HUB=1
helm status p10logs -n p10logs --kube-context "kind-$SPOKE" >/dev/null 2>&1 && RERUN_SPOKE=1
if [ $RERUN_HUB = 1 ] && [ -z "${SPOKE_TOKEN:-}" ]; then # keep the token the spoke already uses
  SPOKE_TOKEN=$($KS -n p10logs get secret p10logs-ingest -o jsonpath='{.data.token}' 2>/dev/null | base64 -d || true)
fi
SPOKE_TOKEN=${SPOKE_TOKEN:-$(python3 -c 'import secrets;print(secrets.token_hex(24))')}
$KH create ns p10logs --dry-run=client -o yaml | $KH apply -f - >/dev/null
$KH label ns p10logs pod-security.kubernetes.io/enforce=privileged --overwrite >/dev/null
helm upgrade --install p10logs charts/p10logs -n p10logs --kube-context "kind-$HUB" \
  --set global.clusterName=hub \
  --set agent.image.pullPolicy=IfNotPresent --set hub.image.pullPolicy=IfNotPresent \
  --set hub.service.type=NodePort --set hub.service.nodePort=$PORT \
  --set hub.storage.persistence.size=5Gi --set hub.storage.retention.maxAge=72h \
  --set agent.enrich.enabled=true --set 'agent.enrich.labels={app,tier,app.kubernetes.io/name}' \
  --set agent.rateLimit.linesPerSecondPerContainer=200 \
  --set "hub.auth.clusterTokens[0].token=$SPOKE_TOKEN" --set "hub.auth.clusterTokens[0].clusters[0]=spoke" \
  --wait --timeout 300s >/dev/null
if [ $RERUN_HUB = 1 ]; then # same image tag was reloaded: restart to pick up the new binaries
  $KH -n p10logs rollout restart ds/p10logs-agent sts/p10logs-hub >/dev/null
  $KH -n p10logs rollout status sts/p10logs-hub --timeout=180s >/dev/null
fi
$KH -n p10logs rollout status ds/p10logs-agent --timeout=180s >/dev/null
URL="http://127.0.0.1:$PORT"
for i in $(seq 1 60); do curl -fs "$URL/readyz" >/dev/null 2>&1 && break; sleep 1; done
curl -fs "$URL/readyz" >/dev/null && pass "hub reachable on host $URL" || fail "hub not reachable on $URL"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$URL/api/v1/status")" = 401 ] && pass "UI auth enforced (401 without creds)" || fail "UI auth not enforced"
# first-run onboarding: admin/admin must be replaced before anything is served
PW=${E2E_UI_PASSWORD:-p10logs-e2e-pass-1}
JAR=$(mktemp)
code=$(curl -s -o /dev/null -w '%{http_code}' -c "$JAR" -d 'user=admin&password=admin' "$URL/auth/login")
if [ "$code" = 302 ]; then
  loc=$(curl -s -o /dev/null -w '%{redirect_url}' -c "$JAR" -d 'user=admin&password=admin' "$URL/auth/login")
  echo "$loc" | grep -q '/auth/setup' && pass "default admin/admin is forced to /auth/setup" || fail "default login went to $loc"
  [ "$(curl -s -o /dev/null -w '%{http_code}' -b "$JAR" "$URL/api/v1/status")" = 403 ] && pass "API refused until the password is changed" || fail "API served before setup"
  [ "$(curl -s -o /dev/null -w '%{http_code}' -b "$JAR" -c "$JAR" -d "password=$PW&password2=$PW" "$URL/auth/setup")" = 302 ] && pass "new password set through onboarding" || fail "setup"
  [ "$(curl -s -o /dev/null -w '%{http_code}' -b "$JAR" "$URL/api/v1/status")" = 200 ] && pass "session works after setup" || fail "session after setup"
  [ "$(curl -s -o /dev/null -w '%{http_code}' -u admin:admin "$URL/api/v1/status")" = 401 ] && pass "admin/admin no longer accepted" || fail "default still accepted"
else
  pass "onboarding already done on an earlier run (login with admin/admin: $code)"
fi
rm -f "$JAR"
[ "$(curl -s -o /dev/null -w '%{http_code}' -u "admin:$PW" "$URL/api/v1/status")" = 200 ] && pass "HTTP Basic with the new password works for scripts" || fail "basic header with new password"
AG=$($KH -n p10logs get pods -l app.kubernetes.io/component=agent --no-headers | grep -c Running)
[ "$AG" = 3 ] && pass "agent DaemonSet on all 3 nodes (incl. control plane)" || fail "agent pods running: $AG, want 3"

echo "4. spoke cluster: agents only → http://$HUB_IP:$PORT"
$KS create ns p10logs --dry-run=client -o yaml | $KS apply -f - >/dev/null
$KS label ns p10logs pod-security.kubernetes.io/enforce=privileged --overwrite >/dev/null
helm upgrade --install p10logs charts/p10logs -n p10logs --kube-context "kind-$SPOKE" \
  --set global.clusterName=spoke --set hub.enabled=false \
  --set agent.hub.url=http://$HUB_IP:$PORT --set agent.hub.token=$SPOKE_TOKEN \
  --set agent.image.pullPolicy=IfNotPresent \
  --set agent.enrich.enabled=true --set 'agent.enrich.labels={app,tier,app.kubernetes.io/name}' \
  --set agent.multiline.enabled=true \
  --wait --timeout 180s >/dev/null
[ $RERUN_SPOKE = 1 ] && $KS -n p10logs rollout restart ds/p10logs-agent >/dev/null
$KS -n p10logs rollout status ds/p10logs-agent --timeout=120s >/dev/null
pass "installed"

echo "5. demo workloads on both clusters"
$KH apply -f hack/demo/ >/dev/null; $KS apply -f hack/demo/ >/dev/null
for ctx in "$KH" "$KS"; do
  $ctx -n shop rollout status deploy/api --timeout=180s >/dev/null
  $ctx -n shop rollout status deploy/longline --timeout=120s >/dev/null
  $ctx -n shop rollout status deploy/java --timeout=120s >/dev/null
  $ctx -n payments rollout status deploy/gateway --timeout=120s >/dev/null
done
pass "demo running"

# ---- helpers ----
api(){ curl -fs -u "admin:$PW" "$URL/api/v1/$1"; }
q(){ api "query?$1"; }
count(){ q "$1" | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["lines"]))'; }
wait_min(){ # wait_min <query> <min count> [secs]
  for i in $(seq 1 "${3:-90}"); do n=$(count "$1" 2>/dev/null || echo 0); [ "$n" -ge "$2" ] && return 0; sleep 1; done
  echo "got $n want >= $2 for $1"; return 1; }

echo "6. both clusters visible, agents from every node"
for i in $(seq 1 60); do
  n=$(api status | python3 -c 'import json,sys,time;d=json.load(sys.stdin);now=time.time_ns();print(sum(1 for a in d["agents"] if now-a["last_seen"]<60e9))' 2>/dev/null || echo 0)
  [ "$n" = 4 ] && break; sleep 1
done
[ "$n" = 4 ] && pass "4 agents heartbeating (3 hub nodes + 1 spoke)" || fail "agents heartbeating: $n, want 4"
api status | python3 -c 'import json,sys;d=json.load(sys.stdin);cs=sorted({a["cluster"] for a in d["agents"]});assert cs==["hub","spoke"],cs' && pass "agents report clusters hub + spoke" || fail "clusters in status"
for i in $(seq 1 60); do api streams | python3 -c 'import json,sys;d=json.load(sys.stdin);cs=sorted(c["name"] for c in d["clusters"]);sys.exit(0 if cs==["hub","spoke"] else 1)' 2>/dev/null && break; sleep 1; done
api streams | python3 -c 'import json,sys;d=json.load(sys.stdin);cs=sorted(c["name"] for c in d["clusters"]);assert cs==["hub","spoke"],cs' && pass "stream tree has clusters hub + spoke" || fail "stream tree clusters"
wait_min "cluster=hub&namespace=shop&pod=api-*&start=-10m&limit=50" 10 && pass "hub cluster api logs" || fail "hub api logs"
wait_min "cluster=spoke&namespace=shop&pod=api-*&start=-10m&limit=50" 10 && pass "spoke cluster api logs (pushed cross-cluster)" || fail "spoke api logs"
wait_min "cluster=spoke&namespace=payments&start=-10m&limit=50" 5 && pass "second namespace from spoke" || fail "payments logs"
wait_min "cluster=hub&namespace=kube-system&start=-10m&limit=20" 1 && pass "system pods collected" || fail "kube-system logs"
wait_min "label=tier:edge&start=-10m&limit=20" 1 && pass "label selector tier=edge (enrichment on both clusters)" || fail "label selector"
wait_min "label=_owner:Deployment/api&start=-10m&limit=20" 1 && pass "synthetic _owner label selects the api Deployment" || fail "_owner label"
api streams | python3 -c '
import json,sys;d=json.load(sys.stdin);o={(c["name"],n["name"],p["name"]):p.get("labels",{}).get("_owner") for c in d["clusters"] for n in c["namespaces"] for p in n["pods"]}
# Job/migrate is not required: on a re-run its pod finished before the current agents started.
# The agent does not collect itself, so the DaemonSet check uses whatever DaemonSet the cluster runs (kindnet, kube-proxy).
vals=[v for v in o.values() if v]
want={"CronJob/report":any(v=="CronJob/report" for v in vals),"Deployment/api":any(v=="Deployment/api" for v in vals),"DaemonSet/*":any(v.startswith("DaemonSet/") for v in vals)}
missing=[k for k,ok in want.items() if not ok]; assert not missing, ("owners not found", missing, sorted(set(vals))[:12])' && pass "owners derived: Deployment, DaemonSet, CronJob" || fail "_owner derivation"
[ "$(count 'namespace=shop&pod=api-*&q=level%3Derror&start=-10m&limit=200')" -ge 1 ] && pass "JSON field filter level=error" || fail "level=error"
[ "$(count 'namespace=shop&pod=api-*&q=%22upstream+timeout%22&start=-10m&limit=50')" -ge 1 ] && pass "stderr lines present (phrase filter)" || fail "stderr/phrase"

echo "7. CRI partial-line reassembly (20 KiB lines, 16 KiB containerd limit)"
wait_min "cluster=hub&namespace=shop&pod=longline-*&start=-10m&limit=5" 1 >/dev/null || fail "no longline output"
L=$(q "cluster=hub&namespace=shop&pod=longline-*&start=-10m&limit=5" | python3 -c 'import json,sys;print(max(len(l["m"]) for l in json.load(sys.stdin)["lines"]))')
[ "$L" -gt 16384 ] && pass "line of $L bytes stored intact (> 16384)" || fail "longest line $L bytes: partial lines not reassembled"

echo "8. multiline stack traces (spoke: agent.multiline.enabled)"
wait_min "cluster=spoke&namespace=shop&pod=java-*&q=IllegalStateException&start=-10m&limit=5" 1 120 >/dev/null || fail "no java exception yet"
q "cluster=spoke&namespace=shop&pod=java-*&q=IllegalStateException&start=-10m&limit=5" | python3 -c '
import json,sys;ls=json.load(sys.stdin)["lines"];m=ls[0]["m"];assert "\n\tat " in m and "Caused by" in m, m[:200];print("  ·", m.count("\n")+1, "physical lines joined into one event")' && pass "stack trace joined into one line" || fail "multiline not joined"
# hub cluster has multiline off: the same trace arrives as separate lines
NH=$(count "cluster=hub&namespace=shop&pod=java-*&q=%2Fat+com.example%2F&start=-10m&limit=50")
[ "$NH" -ge 2 ] && pass "hub cluster (multiline off) keeps $NH separate 'at com.example' lines" || echo "  · hub cluster trace lines: $NH (java pod may not have thrown yet)"

echo "9. per-container rate limit (hub cluster: 200 lines/s, spammer ~400 lines/s)"
wait_min "cluster=hub&namespace=shop&pod=spammer-*&q=%22rate+limit%22&start=-10m&limit=5" 1 90 && pass "'dropped N lines (rate limit)' marker emitted" || fail "no rate-limit marker"
D=$(api status | python3 -c 'import json,sys;print(sum(a["dropped"] for a in json.load(sys.stdin)["agents"] if a["cluster"]=="hub"))')
[ "$D" -gt 0 ] && pass "agents report dropped=$D in status" || fail "dropped counter is 0"

echo "10. per-cluster ingest token"
c=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H "Authorization: Bearer $SPOKE_TOKEN" -H "X-P10-Cluster: spoke" "$URL/api/v1/push")
[ "$c" = 204 ] && pass "spoke token accepted for cluster=spoke" || fail "spoke token rejected for spoke ($c)"
c=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H "Authorization: Bearer $SPOKE_TOKEN" -H "X-P10-Cluster: hub" "$URL/api/v1/push")
[ "$c" = 401 ] && pass "spoke token rejected for cluster=hub" || fail "spoke token accepted for hub ($c)"

echo "11. restart boundaries (worker crashes every ~45 s)"
ok=0
for i in $(seq 1 240); do
  r=$(api streams | python3 -c 'import json,sys;d=json.load(sys.stdin);print(max([p["restarts"] for c in d["clusters"] for n in c["namespaces"] if n["name"]=="shop" for p in n["pods"] if p["name"].startswith("worker-")]+[0]))' 2>/dev/null || echo 0)
  [ "$r" -ge 1 ] && ok=1 && break; sleep 2
done
[ $ok = 1 ] && pass "worker restarts=$r visible in tree" || fail "worker never restarted"
q "namespace=shop&pod=worker-*&q=panic&start=-15m&limit=20" | python3 -c '
import json,sys;ls=json.load(sys.stdin)["lines"];assert ls and all(l.get("e") for l in ls),ls[:2];rs=sorted({l.get("r",0) for l in ls});print("  · panic lines from restarts",rs)' && pass "panic lines kept from previous restarts (stderr)" || fail "panic lines"

echo "12. short-lived pods: CronJob 'report' (every 2 min) and Job 'migrate' with init container"
wait_min "namespace=shop&pod=migrate-*&container=wait-db&start=-15m&limit=10" 5 && pass "init-container logs collected" || fail "init container logs"
wait_min "namespace=shop&pod=migrate-*&container=migrate&q=complete&start=-15m&limit=10" 1 && pass "job logs collected after completion" || fail "job logs"
wait_min "namespace=shop&pod=report-*&q=%22report+job+done%22&start=-15m&limit=10" 1 150 && pass "cronjob pod logs collected" || fail "cronjob logs"

echo "13. hub outage: spoke spools to disk, then drains without loss or duplicates"
$KH -n p10logs scale sts/p10logs-hub --replicas=0 >/dev/null
td=$(date +%s)
for i in $(seq 1 60); do [ "$($KH -n p10logs get pods -l app.kubernetes.io/component=hub --no-headers 2>/dev/null | wc -l | tr -d ' ')" = 0 ] && break; sleep 1; done
ok=0; for i in $(seq 1 90); do $KS -n p10logs logs ds/p10logs-agent --since=120s | grep -q 'push failed, spooling' && ok=1 && break; sleep 1; done
dt=$(( $(date +%s) - td ))
[ $ok = 1 ] && pass "spoke agent spooling ${dt}s after the hub pod went away" || fail "spoke never started spooling"
[ $dt -le 30 ] && pass "noticed within 30 s (bounded push deadline)" || fail "took ${dt}s to notice: push hung on a dead connection"
$KH -n p10logs scale sts/p10logs-hub --replicas=1 >/dev/null
$KH -n p10logs rollout status sts/p10logs-hub --timeout=180s >/dev/null
for i in $(seq 1 60); do curl -fs "$URL/readyz" >/dev/null 2>&1 && break; sleep 1; done
ok=0; for i in $(seq 1 120); do $KS -n p10logs logs ds/p10logs-agent --since=180s | grep -q 'spool drained' && ok=1 && break; sleep 1; done
[ $ok = 1 ] && pass "spoke spool drained after hub returned" || fail "spool not drained"
sleep 15
q "cluster=spoke&namespace=shop&pod=longline-*&start=-20m&limit=1000" | python3 -c '
import json,sys,re
ls=json.load(sys.stdin)["lines"]; seq=[int(re.search(r"seq=(\d+)",l["m"]).group(1)) for l in ls]
assert len(seq)==len(set(seq)), "duplicates: %d lines, %d unique"%(len(seq),len(set(seq)))
assert max(seq)-min(seq)+1==len(seq), "gaps: have %d of %d..%d"%(len(seq),min(seq),max(seq))
print("  · longline seq %d..%d: %d lines, no gaps, no duplicates"%(min(seq),max(seq),len(seq)))' && pass "no loss, no duplicates across the outage" || fail "loss or duplicates after outage"
AGN=$(api status | python3 -c 'import json,sys,time;d=json.load(sys.stdin);now=time.time_ns();print(sum(1 for a in d["agents"] if now-a["last_seen"]<60e9))')
[ "$AGN" = 4 ] && pass "all 4 agents back" || fail "agents after outage: $AGN"

echo "14. hub integrity"
$KH -n p10logs logs sts/p10logs-hub --since=1h | grep -q '"level":"ERROR"' && fail "hub logged errors" || pass "no hub errors"
curl -fs -u "admin:$PW" "$URL/" | grep -q '<title>p10logs' && pass "UI served" || fail "UI"

echo
echo "ALL MULTI-CLUSTER E2E PASSED in $(( $(date +%s) - t0 ))s"
echo
echo "  UI        : $URL"
echo "  user      : admin"
echo "  password  : $PW"
echo "  contexts  : kind-$HUB (hub, 3 nodes), kind-$SPOKE (spoke, 1 node)"
echo "  hub ip    : $HUB_IP:$PORT (what the spoke pushes to)"
echo "  demo apps : namespaces shop, payments on both clusters (hack/demo/)"
echo "  tear down : make kind-down"
