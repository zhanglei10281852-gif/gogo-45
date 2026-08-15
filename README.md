# QueueForge

QueueForge is an independent, original project. It is not affiliated with, endorsed by, sponsored by, or associated with any other product, company, or organization.

QueueForge is a standard-library-only Go 1.22.5 command-line background job queue and scheduler for offline and single-host workflows. It persists every mutation before acknowledging it, verifies an SHA-256 audit chain during startup and recovery, periodically writes atomic snapshots, and never performs network calls.

## Capabilities

- Strict JSON configuration and command input (`DisallowUnknownFields`, one document only).
- Explicit blocked, ready, leased, retry-wait, succeeded, dead, and cancelled states with guarded transitions.
- Dependency DAG validation, automatic unblocking, and dependency-failure propagation.
- Priority ordering, delayed availability, deadlines, deterministic tie-breaking, and queue routing.
- Worker labels and CPU, memory, and slot capacity constraints.
- Exclusive leases, lease tokens, heartbeats, expiry recovery, retry limits, fixed/linear/exponential backoff, deterministic jitter, and dead-letter state.
- Durable idempotency keys with configurable retention.
- Append-only JSON Lines journal with sequence and SHA-256 predecessor integrity.
- Atomic snapshots, strict replay, lock-file exclusion, metrics, timelines, query helpers, route diagnostics, planning, and audits.

## Build and test

```text
go test ./... -count=1
go build ./...
go vet ./...
```

No dependency download is needed: `go.mod` contains no third-party modules.

## Configuration

Pass `-config config.json` to any command. Without it, built-in defaults are used and the data directory is `.queueforge`; `QUEUEFORGE_DATA_DIR` may override that directory only when no config file is supplied. Relative `data_dir` values in a config file are resolved relative to that file.

```json
{
  "data_dir": ".queueforge",
  "lease_seconds": 60,
  "heartbeat_grace_seconds": 15,
  "snapshot_every": 100,
  "max_journal_bytes": 67108864,
  "default_max_attempts": 3,
  "default_backoff_seconds": 5,
  "default_backoff_max_seconds": 3600,
  "default_queues": ["default"],
  "clock_skew_tolerance_seconds": 5,
  "idempotency_retention_hours": 168
}
```

## Command workflow

Input defaults to stdin; use `-input path.json` for a file. Output is JSON except the default text report.

```text
queueforge validate -config config.example.json -input examples/enqueue.json
queueforge enqueue -config config.example.json -input examples/enqueue.json
queueforge claim -config config.example.json -input examples/claim.json
```

A claim response contains a job-specific `lease.token`. Use it for subsequent ownership checks:

```json
{
  "job_id": "render-invoice-1001",
  "lease_token": "lease-...",
  "extend_seconds": 90
}
```

```text
queueforge heartbeat -config config.example.json -input heartbeat.json
```

Complete with a JSON result:

```json
{
  "job_id": "render-invoice-1001",
  "lease_token": "lease-...",
  "result": { "file": "invoice-1001.pdf" }
}
```

```text
queueforge complete -config config.example.json -input complete.json
```

Or fail the attempt. A retryable failure enters `retry_wait` until its calculated availability time unless the attempt limit has been reached; other failures enter `dead`:

```json
{
  "job_id": "render-invoice-1001",
  "lease_token": "lease-...",
  "code": "renderer_busy",
  "message": "local renderer unavailable",
  "retryable": true
}
```

```text
queueforge fail -config config.example.json -input fail.json
queueforge recover -config config.example.json -snapshot=true
queueforge report -config config.example.json -format=text
queueforge report -config config.example.json -format=json
queueforge report -config config.example.json -format=jobs
queueforge report -config config.example.json -format=timeline
```

`validate` checks configuration, optional enqueue input, journal integrity, replay, and the dependency graph. `recover` refuses to run while another QueueForge process holds the data-directory lock. Normal commands also recover expired leases at open or claim time.

## Persistence and crash behavior

`journal.jsonl` is append-only. Each event includes a monotonic sequence, the prior event hash, and its own hash over a canonical JSON representation of all non-hash fields. Each append is synchronized before in-memory state changes are acknowledged. Startup verifies the complete chain, loads a valid `snapshot.json` when present, and replays later events. A malformed line, gap, altered hash, unknown event type, invalid job, or graph violation stops recovery rather than silently discarding data.

Snapshots are written to a temporary file, synchronized, and atomically renamed. `queueforge.lock` provides process exclusion. If a process is forcibly terminated, confirm that no QueueForge process is using the data directory before removing a stale lock. Never edit the journal; archive the entire data directory when retaining audit evidence.

Idempotency keys map to the first accepted job. Re-enqueueing a retained key returns that job with `duplicate: true`. A client should treat this as success. Job IDs and lease tokens can be generated locally and are not security credentials; filesystem permissions are the trust boundary.

## Scheduling semantics

Eligible jobs are sorted by descending priority, then earliest availability, creation time, and ID. A worker must subscribe to the queue (or `*`), match every required label, and have enough remaining CPU, memory, and slots. Dependencies must all succeed. A dead or cancelled dependency dead-letters blocked dependents. Delays and retry waits become ready during refresh. Deadlines dead-letter unclaimed jobs after expiry.

A claim increments `attempts` and creates an exclusive token. Heartbeats replace expiry with `now + extension`. Completion and failure require the current token and reject leases outside the configured grace interval. Expired leases retry with backoff when attempts remain, otherwise they become dead. Jitter is deterministic from job ID and attempt so replay does not change scheduling.

## Production-source measurement

Effective production Go LOC excludes `*_test.go`, generated files, blank lines, and comment-only lines (including lines wholly inside block comments). The release measurement is recorded in the final project report after formatting.

The deterministic production-source digest uses all production `.go` files (excluding `*_test.go` and generated files). Relative paths are slash-normalized and sorted by ordinal byte order. For each file in order, the SHA-256 input is exactly:

```text
UTF-8(relative/path) || NUL || raw-file-bytes
```

There is no separator after the raw bytes and no final trailer. Paths are relative to the repository root. This framing is unambiguous because Go source paths cannot contain NUL and each next path is known from the sorted manifest. The final digest and per-file LOC are reported after all source changes.

## Operational limits

QueueForge is intentionally a local offline queue, not a distributed consensus system. Keep the data directory on a reliable local filesystem that honors file synchronization and atomic rename. The journal size limit prevents accidental unbounded growth; archive a closed queue before policy-driven compaction. Payloads and results remain inline JSON, so place large binary artifacts elsewhere and store only local references. Back up the config, snapshot, and journal together while the queue is closed.
