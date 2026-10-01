#!/usr/bin/env bash
# Real-Kubernetes end-to-end on kind: build images, load them, install the chart,
# generate logs from a pod, verify the hub has them. Needs docker + kind + helm + kubectl.
set -euo pipefail
cd "$(dirname "$0")/.."
CLUSTER=${CLUSTER:-p10logs-e2e}
VER=$(awk '/^version:/{print $2}' charts/p10logs/Chart.yaml)
pass(){ echo "  ✔ $1"; }; fail(){ echo "  ✘ $1"; kubectl -n p10logs get pods -o wide || true; kubectl -n p10logs logs ds/p10logs-agent --tail=50 || true; exit 1; }

echo "1. images"
docker build -q --build-arg VERSION=$VER -f Dockerfile.agent -t ghcr.io/p10node/p10logs-agent:$VER . >/dev/null
docker build -q --build-arg VERSION=$VER -f Dockerfile.hub   -t ghcr.io/p10node/p10logs-hub:$VER   . >/dev/null
pass "built"

echo "2. kind cluster"
kind get clusters 2>/dev/null | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER" --wait 120s >/dev/null
kind load docker-image --name "$CLUSTER" ghcr.io/p10node/p10logs-agent:$VER ghcr.io/p10node/p10logs-hub:$VER >/dev/null
pass "images loaded"

echo "3. helm install (clean)"
helm uninstall p10logs -n p10logs --ignore-not-found >/dev/null 2>&1 || true
kubectl delete secret p10logs-ingest p10logs-ui -n p10logs --ignore-not-found >/dev/null 2>&1 || true
kubectl delete pod e2e-logger -n default --ignore-not-found --now >/dev/null 2>&1 || true
kubectl create ns p10logs --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl label ns p10logs pod-security.kubernetes.io/enforce=privileged --overwrite >/dev/null
helm install p10logs charts/p10logs -n p10logs \
  --set global.clusterName=kind --set agent.image.pullPolicy=IfNotPresent --set hub.image.pullPolicy=IfNotPresent \
  --set hub.storage.persistence.size=1Gi --set hub.auth.ui.mode=none \
  --set agent.enrich.enabled=true --set 'agent.enrich.labels={run,app}' --wait --timeout 180s >/dev/null
# both sides must hold the same token (regression: template evaluated randAlphaNum twice)
t1=$(kubectl -n p10logs get secret p10logs-ingest -o jsonpath='{.data.token}'); t2=$(kubectl -n p10logs get secret p10logs-ingest -o jsonpath='{.data.ingest-token}')
[ "$t1" = "$t2" ] && pass "ingest token consistent" || fail "token mismatch in secret"
kubectl -n p10logs rollout status ds/p10logs-agent --timeout=120s >/dev/null
pass "installed"

echo "4. generate logs"
kubectl -n default run e2e-logger --image=busybox:1.36 --restart=Never -- sh -c 'for i in $(seq 1 200); do echo "e2e line $i level=info"; done; echo "e2e ERROR needle"; sleep 300' >/dev/null
kubectl -n default wait --for=condition=Ready pod/e2e-logger --timeout=120s >/dev/null
PUID=$(kubectl -n default get pod e2e-logger -o jsonpath='{.metadata.uid}')
pass "logger pod running (uid $PUID)"

echo "5. query through the hub"
kubectl -n p10logs port-forward svc/p10logs-hub 28080:8080 >/dev/null 2>&1 & PF=$!
trap 'kill $PF 2>/dev/null || true' EXIT
sleep 2
ok=0
for i in $(seq 1 60); do
  # select by uid: a previous run's pod with the same name may still have log files on the node
  n=$(curl -s "http://127.0.0.1:28080/api/v1/query?pod=e2e-logger&uid=$PUID&start=-1h&limit=1000" | python3 -c 'import json,sys;print(len(json.load(sys.stdin).get("lines",[])))' 2>/dev/null || echo 0)
  [ "$n" = 201 ] && ok=1 && break; sleep 1
done
[ $ok = 1 ] && pass "201 lines from e2e-logger" || fail "expected 201 lines, got $n"
curl -s "http://127.0.0.1:28080/api/v1/query?pod=e2e-logger&uid=$PUID&start=-1h&q=needle" | grep -q 'ERROR needle' && pass "filter works" || fail "filter"
n2=$(curl -s "http://127.0.0.1:28080/api/v1/query?pod=e2e-logger&uid=$PUID&start=-1h&q=needle&limit=10" | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["lines"]))')
[ "$n2" = 1 ] && pass "no duplicate lines" || fail "needle appears $n2 times"
curl -s "http://127.0.0.1:28080/api/v1/status" | grep -q '"cluster":"kind"' && pass "agent heartbeat from kind node" || fail "status"
curl -s "http://127.0.0.1:28080/api/v1/streams" | grep -q 'kube-system' && pass "system pods collected too" || fail "streams"
ok=0; for i in $(seq 1 30); do curl -s "http://127.0.0.1:28080/api/v1/query?label=run:e2e-logger&start=-1h&q=needle" | grep -q 'ERROR needle' && ok=1 && break; sleep 1; done
[ $ok = 1 ] && pass "enrichment: label=run:e2e-logger selects the pod" || fail "enrichment (labels: $(curl -s 'http://127.0.0.1:28080/api/v1/streams' | grep -o '"labels":{[^}]*}' | head -3))"
kubectl -n p10logs logs ds/p10logs-agent | grep -q '"enrich enabled"' && pass "agent watcher started" || fail "enrich not enabled"

echo "6. helm upgrade keeps data (PVC retained, secrets kept, no duplicates)"
kill $PF 2>/dev/null || true
helm upgrade p10logs charts/p10logs -n p10logs --reuse-values --set hub.storage.retention.maxAge=48h --wait --timeout 180s >/dev/null
kubectl -n p10logs rollout status sts/p10logs-hub --timeout=120s >/dev/null
kubectl -n p10logs rollout status ds/p10logs-agent --timeout=120s >/dev/null
kubectl -n p10logs port-forward svc/p10logs-hub 28080:8080 >/dev/null 2>&1 & PF=$!
sleep 3
ok=0; for i in $(seq 1 30); do n=$(curl -s "http://127.0.0.1:28080/api/v1/query?pod=e2e-logger&uid=$PUID&start=-1h&limit=1000" | python3 -c 'import json,sys;print(len(json.load(sys.stdin).get("lines",[])))' 2>/dev/null || echo 0); [ "$n" = 201 ] && ok=1 && break; sleep 1; done
[ $ok = 1 ] && pass "201 lines still there after upgrade, none duplicated" || fail "after upgrade got $n"
t3=$(kubectl -n p10logs get secret p10logs-ingest -o jsonpath='{.data.token}')
[ "$t3" = "$t1" ] && pass "ingest token unchanged across upgrade" || fail "token rotated on upgrade"
echo "ALL KIND E2E PASSED (cluster '$CLUSTER' left running; delete with: kind delete cluster --name $CLUSTER)"
