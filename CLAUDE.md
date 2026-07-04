# CLAUDE.md

Working notes for AI-assisted sessions on this repo. Read this first; it is
the source of truth for project rules, workflow, and where the work stands.

## What Nimbus is

A **real container orchestrator built from scratch in Go** — not a demo, not a
wrapper. It began as a scheduling/autoscaling simulator (that engine survives
under `internal/{autoscaler,scheduler,sim}` as the future "brain"); it pivoted
to a from-scratch cluster manager. The goal is to **learn by building the
plumbing**: reliability first, algorithms later.

Rules that are project identity, not preferences:

- **Zero dependencies.** Go standard library only. `go.mod` has no requires
  and that must stay true. The Docker Engine API is spoken as raw HTTP/JSON
  over `/var/run/docker.sock` (`internal/runtime`) — no Docker SDK, ever.
- **Build it, don't buy it.** When a capability is missing (store, API,
  scheduler, networking), the answer is to design and build it here. Don't
  propose off-the-shelf substitutes (K8s, etcd, SDKs) as alternatives; note
  real constraints briefly if they exist, then build.
- **Reliability first.** Every layer is proven against reality (unit tests on
  fakes, integration tests on live Docker, manual end-to-end runs) before it
  ships. Reconcile properties to preserve: stateless (runtime is the source of
  truth), idempotent, convergent, fault-tolerant (one replica's failure never
  aborts a pass).

## Git & PR workflow (hard rules)

- `main` is protected (ruleset "protect-main", id 17515528): PRs only, with
  required status checks **`test` and `integration`**.
- **Never merge a PR. Never enable auto-merge.** Create the PR, report its
  URL, and stop — the user reviews and merges every PR personally. (GitHub
  can't enforce this via required approvals: the CLI's PRs are authored by the
  user's own account, and authors can't self-approve.)
- Branch from `main` per story (e.g. `cp-a2-node-registry`), squash-merge.
- Before pushing: `gofmt -l .` (must be empty), `go vet ./...`,
  `go test -race ./...`, and update `CHANGELOG.md` (Unreleased section).
- Verify nontrivial changes end-to-end by actually running the binary, not
  just the tests.

## Commands

```bash
make test          # hermetic unit tests (no Docker) — CI job `test`
make integration   # end-to-end vs live Docker    — CI job `integration`
go build -o nimbusd ./cmd/nimbusd
./nimbusd serve                          # control plane (127.0.0.1:7440)
./nimbusd apply -spec examples/cluster.json -server http://127.0.0.1:7440
./nimbusd run                            # all-in-one single-node mode
```

## Repo map

- `internal/spec` — declarative desired state (workloads, probes, validation)
- `internal/store` — durable crash-safe store (temp file → fsync → rename →
  fsync dir; monotonic generation)
- `internal/runtime` — hand-written Docker Engine API client (unix socket)
- `internal/agent` — the reconciler: probe → ensure → reclaim phases, each
  fanned out with bounded parallelism; CrashLoopBackOff; exec liveness probes
- `internal/api` — control-plane HTTP API: wire types, server, client
- `cmd/nimbusd` — one binary, modes: `serve` / `run` / operator CLI
  (`apply|get|scale|delete|status|down`); `agent` mode coming (story A4)
- `test/integration` — build tag `integration`; scopes workloads `itest-*`,
  cleans up before/after, skips without a daemon
- `internal/{autoscaler,scheduler,sim}`, `cmd/nimbus` — the simulation engine
  ("brain"), wired in during Epic D
- `docs/design/control-plane-split.md` — the accepted design for the current
  milestone (architecture, API v1, backlog)

Conventions: containers are labeled `nimbus.managed` / `nimbus.workload` /
`nimbus.replica` and named `nimbus-<workload>-<replica>`; control plane
defaults to `127.0.0.1:7440` (loopback until auth lands in C3); state file
defaults to `nimbus-state.json`.

## Current milestone: control-plane / node-agent split

Design: `docs/design/control-plane-split.md` (pull-based agents, one binary).
Backlog status — update this section as stories land:

- [x] A1 — control-plane HTTP server + operator CLI over HTTP (PR #11)
- [ ] A2 — node registry + register/heartbeat endpoints
- [ ] A3 — assignment endpoint (single node: whole spec)
- [ ] A4 — `nimbusd agent`: register, poll assignment, reconcile locally
- [ ] A5 — status reporting; `nimbusd status` via the control plane
- [ ] Epic B — multi-node assignment (spread, node-label scoping, reassignment)
- [ ] Epic C — robustness (agent survives CP outage, CP restart recovery,
      shared-secret auth, observability)
- [ ] Epic D — wire in the bin-packing scheduler as the assigner

Known sharp edge (until A4): while `serve` runs, a local no-`-server` mutation
to the same state file is invisible to it — the control plane owns the store
and doesn't reload. `run` still reloads each pass.
