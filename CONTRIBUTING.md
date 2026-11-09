# Contributing

- `make test` (unit), `make e2e` (local end-to-end, ~40 s), `make e2e-kind` (needs Docker).
- Keep the agent free of client-go and the hub free of CGO; both are deliberate.
- Formats are versioned in `docs/FORMATS.md`; a change there needs a reader for the old
  version and a note in `CHANGELOG.md`.
- Run `gofmt`, `go vet`, `make golangci` (golangci-lint v2, config in `.golangci.yml`) and
  `helm lint charts/p10logs` before opening a PR. CI runs the same plus `go test -race` with
  coverage; new code should come with unit tests (see the `*_test.go` next to each package).
