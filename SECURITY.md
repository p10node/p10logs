# Security

Report vulnerabilities privately to the maintainers via GitHub security advisories on
`p10node/p10logs`. Please do not open public issues for exploitable bugs.

## Model

- Agents run as UID 0 with read-only hostPath mounts, a read-only root filesystem and
  no capabilities. They need no Kubernetes API access unless label enrichment is on
  (then `pods` get/list/watch, scoped by field selector to their own node).
- Agent → hub is a bearer token over whatever transport you put in front of the hub;
  use TLS (Ingress) for anything that leaves a cluster. Per-cluster tokens limit the
  blast radius of a leaked spoke token.
- The hub runs as a non-root user with a read-only root filesystem. UI/API auth is
  basic, OIDC (go-oidc, authorization code, HMAC-signed session cookie) or none.
  Viewer roles scope reads by cluster and namespace.
- Log lines are stored as received; the hub does not redact. Treat the PVC and any
  object-storage bucket as containing secrets.
- No component executes anything from log content. The UI escapes all log text.
