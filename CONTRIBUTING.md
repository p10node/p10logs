# Contributing

- `make test` (unit), `make e2e` (local end-to-end, ~40 s), `make e2e-kind` (needs Docker).
- Keep the agent free of client-go and the hub free of CGO; both are deliberate.
- Formats are versioned in `docs/FORMATS.md`; a change there needs a reader for the old
  version and a note in `CHANGELOG.md`.
- Run `gofmt`, `go vet` and `helm lint charts/p10logs` before opening a PR.
