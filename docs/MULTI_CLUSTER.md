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
