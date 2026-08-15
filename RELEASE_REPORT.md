# QueueForge release report

## Scope

- Language target: Go 1.22.5
- Module: `QueueForge`
- Dependencies: Go standard library only
- Production Go packages: 10
- Effective production Go LOC: **3,124**
- Production source SHA-256: `718b8c130027d8443840029c1a16e07b7b90e5f259789cef7f1290a8cc77db57`

Effective LOC excludes tests, generated files, blank lines, and comment-only lines. No production file is generated. Per-file results after `gofmt`:

| Effective LOC | Production source             |
| ------------: | ----------------------------- |
|           382 | `cmd/queueforge/main.go`      |
|           183 | `internal/audit/audit.go`     |
|           163 | `internal/codec/codec.go`     |
|           155 | `internal/config/config.go`   |
|           504 | `internal/engine/engine.go`   |
|           381 | `internal/model/model.go`     |
|           272 | `internal/planner/planner.go` |
|           354 | `internal/query/query.go`     |
|           129 | `internal/report/report.go`   |
|           601 | `internal/store/store.go`     |

## Source digest framing

Production `.go` files are selected recursively, excluding `*_test.go` and generated files. Slash-normalized repository-relative paths are sorted in ordinal byte order. For each file, the digest receives `UTF-8(path)`, one NUL byte, then the exact raw file bytes. Files have no extra delimiter and the stream has no trailer:

```text
SHA-256(path1 || NUL || bytes1 || path2 || NUL || bytes2 || ...)
```

All current paths are ASCII, so ordinal path-byte and ordinal Unicode ordering coincide.

## Package list

```text
QueueForge/cmd/queueforge
QueueForge/internal/audit
QueueForge/internal/codec
QueueForge/internal/config
QueueForge/internal/engine
QueueForge/internal/model
QueueForge/internal/planner
QueueForge/internal/query
QueueForge/internal/report
QueueForge/internal/store
```

## Validation

All Go cache, temporary, and smoke-test artifacts were redirected beneath the repository's ignored `.cache/` directory.

- `gofmt -w .` — passed; subsequent source hash is over formatted files.
- `go test ./... -count=1` — passed for all 10 packages; focused tests passed in config, engine, model, planner, and store.
- `go build ./...` — passed with no output.
- `go vet ./...` — passed with no output.
- Offline CLI smoke workflow — passed: enqueue created one job, claim leased one job, complete reached `succeeded`, validate returned `config=true`, and report returned `total=1`.

The focused tests cover unknown JSON fields, valid defaults, dependency cycles, illegal state transitions, durable retry and completion, dependency scheduling order, and SHA-256 journal tamper detection.

## Repository assertions

The repository is initialized only after validation. Final checks verify branch `main`, exactly one root commit, local author configuration, no remotes, and a clean working tree.
