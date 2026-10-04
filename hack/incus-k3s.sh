#!/usr/bin/env bash
# Create N Incus virtual machines on this host and turn them into one k3s cluster
# (node 1 = server, the rest = agents). Run ON the Incus host:
#
#   PREFIX=p10-k8s COUNT=3 CPU=4 MEM=8GiB DISK=30GiB STORAGE=fast ./hack/incus-k3s.sh
#   scp hack/incus-k3s.sh myhost:/tmp/ && ssh myhost STORAGE=fast /tmp/incus-k3s.sh   # from a laptop
#   (do not pipe it through `bash -s`: incus reads YAML from stdin and would eat the script)
#
# Uses the default profile (so the VMs get whatever network/storage it defines, e.g. a
# bridged NIC with DHCP). Idempotent: existing VMs are kept, k3s is installed only where
# missing. Prints the kubeconfig (server address rewritten to node 1's IP) at the end.
set -euo pipefail
PREFIX=${PREFIX:-p10-k8s}; COUNT=${COUNT:-3}; CPU=${CPU:-4}; MEM=${MEM:-8GiB}; DISK=${DISK:-30GiB}; STORAGE=${STORAGE:-}
IMAGE=${IMAGE:-images:ubuntu/noble/cloud}
K3S_CHANNEL=${K3S_CHANNEL:-stable}
log(){ echo "[$(date +%H:%M:%S)] $*" >&2; }
ex(){ incus exec "$1" -- bash -c "$2" </dev/null; }

names=(); for i in $(seq 1 "$COUNT"); do names+=("$PREFIX-$i"); done

for n in "${names[@]}"; do
  if incus info "$n" >/dev/null 2>&1; then log "$n exists"; continue; fi
  log "launching $n ($CPU cpu, $MEM, $DISK)"
  incus launch "$IMAGE" "$n" --vm ${STORAGE:+-s "$STORAGE"} -c limits.cpu="$CPU" -c limits.memory="$MEM" -d root,size="$DISK" >/dev/null </dev/null
done

ip_of(){ incus list "$1" -c 4 -f csv | grep -oE '([0-9]{1,3}\.){3}[0-9]{1,3}' | head -1; }
for n in "${names[@]}"; do
  log "waiting for $n (agent + cloud-init + IPv4)"
  for i in $(seq 1 120); do incus exec "$n" -- true >/dev/null 2>&1 && break; sleep 2; done
  ex "$n" "cloud-init status --wait >/dev/null 2>&1 || true"
  for i in $(seq 1 60); do [ -n "$(ip_of "$n")" ] && break; sleep 2; done
  log "$n = $(ip_of "$n")"
done

S=${names[0]}; SIP=$(ip_of "$S")
if ! ex "$S" "test -x /usr/local/bin/k3s"; then
  log "installing k3s server on $S"
  ex "$S" "curl -sfL https://get.k3s.io | INSTALL_K3S_CHANNEL=$K3S_CHANNEL INSTALL_K3S_EXEC='server --write-kubeconfig-mode 644 --node-name $S --tls-san $SIP' sh - >/dev/null"
fi
for i in $(seq 1 60); do ex "$S" "test -f /var/lib/rancher/k3s/server/node-token" && break; sleep 2; done
TOKEN=$(ex "$S" "cat /var/lib/rancher/k3s/server/node-token")
for n in "${names[@]:1}"; do
  if ex "$n" "test -x /usr/local/bin/k3s"; then log "k3s already on $n"; continue; fi
  log "installing k3s agent on $n → https://$SIP:6443"
  ex "$n" "curl -sfL https://get.k3s.io | INSTALL_K3S_CHANNEL=$K3S_CHANNEL K3S_URL=https://$SIP:6443 K3S_TOKEN=$TOKEN INSTALL_K3S_EXEC='agent --node-name $n' sh - >/dev/null"
done
log "waiting for $COUNT Ready nodes"
for i in $(seq 1 90); do
  r=$(ex "$S" "kubectl get nodes --no-headers 2>/dev/null | grep -c ' Ready'" || echo 0)
  [ "$r" = "$COUNT" ] && break; sleep 3
done
ex "$S" "kubectl get nodes -o wide" >&2
log "kubeconfig follows on stdout (server rewritten to $SIP)"
ex "$S" "sed 's#https://127.0.0.1:6443#https://$SIP:6443#; s#name: default#name: $PREFIX#; s#cluster: default#cluster: $PREFIX#; s#user: default#user: $PREFIX#; s#current-context: default#current-context: $PREFIX#' /etc/rancher/k3s/k3s.yaml"
