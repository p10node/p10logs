# p10logs Helm chart

Lightweight, persistent, multi-cluster Kubernetes pod-log aggregation. Logs only.

```bash
helm install p10logs oci://ghcr.io/p10node/charts/p10logs -n p10logs --create-namespace \
  --set global.clusterName=dev
kubectl label ns p10logs pod-security.kubernetes.io/enforce=privileged --overwrite
```

Topologies: standalone (default), hub + spokes (`hub.enabled=false` on spokes with
`agent.hub.url`/`agent.hub.token`), federation (`hub.federation.peers`). Object-storage
offload via `hub.storage.objectStore`. Auth: `hub.auth.ui.mode` = `none` | `basic` |
`oidc`; per-cluster ingest tokens `hub.auth.clusterTokens`; viewer roles
`hub.auth.roles`. See the project README and `values.yaml` for every option.
