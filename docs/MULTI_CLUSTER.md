# Multi-cluster setup

p10logs supports three topologies. All use the same chart; only values differ.

## A. Standalone (one cluster)

```
helm install p10logs oci://ghcr.io/p10node/charts/p10logs -n p10logs --create-namespace \
  --set global.clusterName=dev
```

Agent + hub in the same cluster. Agents talk to the hub over the ClusterIP service.

## B. Hub + spokes (recommended for most teams)

```
            ┌──────────── central cluster ────────────┐
            │  p10logs-hub  ◄── ingress (TLS) ◄───────┼──── agents (spoke A, egress only)
            │      ▲                                  │
            │      └── agents (central's own nodes)   ◄──── agents (spoke B, egress only)
            └─────────────────────────────────────────┘
```

One hub stores everything; spoke clusters run **agents only** and push over HTTPS.
Spokes need nothing inbound, which works through NAT, private VPCs, and home labs.

### Central cluster

```
helm install p10logs oci://ghcr.io/p10node/charts/p10logs -n p10logs --create-namespace \
  --set global.clusterName=central \
  --set hub.ingress.enabled=true \
  --set hub.ingress.className=nginx \
  --set 'hub.ingress.hosts[0].host=logs.example.com' \
  --set 'hub.ingress.tls[0].secretName=p10logs-tls' \
  --set 'hub.ingress.tls[0].hosts[0]=logs.example.com' \
  --set hub.storage.persistence.size=200Gi
```

Grab the ingest token:

```
kubectl -n p10logs get secret p10logs-ingest -o jsonpath='{.data.token}' | base64 -d
```

### Each spoke

```
helm install p10logs oci://ghcr.io/p10node/charts/p10logs -n p10logs --create-namespace \
  --set global.clusterName=prod-eu-1 \
  --set hub.enabled=false \
  --set agent.hub.url=https://logs.example.com \
  --set agent.hub.token=<ingest-token>
```

Private CA? Put `ca.crt` in a secret and set `agent.hub.tls.caSecret`.

### Per-cluster tokens (recommended in prod)

Instead of one shared token, map tokens to clusters on the hub so a leaked spoke token
cannot impersonate another cluster:

```yaml
hub:
  auth:
    clusterTokens:
      - token: <secret-1>
        clusters: ["prod-eu-1"]
      - token: <secret-2>
        clusters: ["prod-us-1", "staging-*"]
```

Each token may push only as the clusters it lists (`*` globs allowed). The shared
`hub.auth.ingestToken` stays valid for every cluster; remove it once all spokes use
their own token.

### Bandwidth

Agents compress with zstd before sending. Text logs typically shrink 5–10×, so a spoke
producing 5 GB/day raw sends ~0.5–1 GB/day (~10 KB/s average).

### Hub outage

Spoke agents buffer to local disk (`agent.buffer.maxBytes`, default 256 MiB/node) and
replay when the hub returns. Order and dedup are preserved.

## C. Hub per cluster + federation (data residency)

```
   cluster EU                     cluster US                    "console" cluster
   hub-eu (PVC) ◄─ agents         hub-us (PVC) ◄─ agents        hub-console (no agents, or its own)
        ▲                              ▲                              │
        └──────────── query fan-out ───┴──────────────────────────────┘
```

Each cluster keeps its logs locally. One hub (any of them) is configured with peers and
its UI shows all clusters; queries fan out and merge by timestamp.

```yaml
# on each data hub: a read token the console may use
hub:
  auth:
    apiTokens: ["<console-token>"]     # or bind it to a role to scope what the console sees

# on the console hub
hub:
  federation:
    enabled: true
    peers:
      - name: eu
        url: https://logs-eu.example.com
        tokenSecret: p10logs-peer-eu     # secret key `token` = the eu hub's API token
      - name: us
        url: https://logs-us.example.com
        tokenSecret: p10logs-peer-us
```

The console fans `/streams`, `/query`, `/tail`, `/export` and `/status` out to every
peer and merges by timestamp. Peer stream ids are namespaced (`eu/14`), the tree shows
`via eu`, and `X-P10-Hops` stops loops when hubs peer with each other (max 3 hops).
A peer that is down is reported in `peers_failed` and the rest still answer.

Trade-offs vs. B:

|                         | B. Hub + spokes   | C. Federation                   |
|-------------------------|-------------------|---------------------------------|
| Storage                 | one PVC           | one PVC per cluster             |
| Data leaves cluster     | yes               | no (only query results)         |
| Cross-cluster search    | native            | fan-out, slower on wide queries |
| Spoke dependency on WAN | buffer, then drop | none                            |
| Ops                     | 1 hub             | N hubs                          |

## D. Mixed

Any hub can be both a push target for some clusters and a federation peer for others.
Cluster identity is always the `cluster` label stamped by the agent, so the UI tree is
identical regardless of topology.

## E. Try hub + spoke on a laptop (kind)

`make e2e-multi` reproduces topology B with docker only:

```
  kind "p10logs-hub"  (control-plane + 2 workers)        kind "p10logs-spoke" (1 node)
  ┌──────────────────────────────────────────┐           ┌───────────────────────────┐
  │ hub (StatefulSet, PVC) ← NodePort 30080  │ ◄──────── │ agent DaemonSet           │
  │ agents on all 3 nodes                    │  docker   │ token bound to "spoke"    │
  │ demo apps (hack/demo/)                   │  network  │ multiline on, demo apps   │
  └──────────────────────────────────────────┘           └───────────────────────────┘
          ▲ http://localhost:30080  (admin / printed password)
```

It builds the images, creates both clusters (`hack/kind/*.yaml`), installs the chart
twice (hub values: `hub.service.type=NodePort`, `hub.service.nodePort=30080`,
`hub.auth.clusterTokens`; spoke values: `hub.enabled=false`,
`agent.hub.url=http://<hub-control-plane-ip>:30080`, `agent.hub.token`), applies the demo
workloads to both, runs the checks listed in `hack/e2e-multi.sh`, and leaves everything
running. `make kind-down` deletes both clusters. The same values, with an Ingress and
TLS instead of a NodePort, are what a real hub + spoke deployment uses.

## F. A multi-node cluster on one Incus host

`hack/incus-k3s.sh` creates N Incus virtual machines on a host and joins them into one
k3s cluster (node 1 = server). Copy it to the host and run it there (do not pipe it
through `bash -s`: `incus` reads YAML from stdin):

```bash
scp hack/incus-k3s.sh host:/tmp/ && ssh host 'PREFIX=p10-k8s COUNT=3 CPU=4 MEM=8GiB DISK=30GiB STORAGE=fast /tmp/incus-k3s.sh' > p10-k8s.kubeconfig
make images-tar ARCH=amd64                      # dist/p10logs-images-<ver>-amd64.tar
for n in 1 2 3; do ssh host "incus exec p10-k8s-$n -- k3s ctr images import -" < dist/p10logs-images-*-amd64.tar; done
KUBECONFIG=p10-k8s.kubeconfig helm install p10logs charts/p10logs -n p10logs --create-namespace \
  --set global.clusterName=p10-k8s --set hub.service.type=NodePort --set hub.service.nodePort=30080 \
  --set agent.enrich.enabled=true --set 'agent.enrich.labels={app}'
```

The VMs take the host's default profile (bridged NIC + DHCP in the example above), so
the hub is reachable on the LAN at `http://<node-1-ip>:30080`.

## Naming rules

- `global.clusterName` must be unique per hub. Collisions merge streams from two
  clusters into one, which is confusing but not destructive.
- Rename a cluster → old streams remain under the old name until retention removes them.

## Network policy

Agents need egress to the hub only. Example allow-rule for the agent namespace:

```yaml
egress:
  - to: [{ ipBlock: { cidr: 0.0.0.0/0 } }]
    ports: [{ port: 443, protocol: TCP }]
```

The hub needs ingress on its service port from agents (in-cluster) and from the ingress
controller (spokes/UI). Nothing else.
