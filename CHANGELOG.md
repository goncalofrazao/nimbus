# Changelog

## Unreleased — orchestrator

Nimbus turns into a real, self-hosting container orchestrator (it began as a
scheduling/autoscaling *simulator*). First milestone: a single-node, self-
healing reconcile loop that actually runs containers.

- `internal/runtime`: a from-scratch Docker Engine API client speaking
  HTTP/JSON over the `/var/run/docker.sock` Unix socket using only `net/http`
  — no Docker SDK, still zero dependencies. Pull, create, start, stop, remove,
  list, inspect; idempotent start/stop/remove.
- `internal/spec`: declarative desired state (workloads = image + replicas +
  command), loaded and validated from JSON.
- `internal/agent`: the reconcile loop — stateless (the daemon is the source
  of truth), idempotent, convergent (self-heals crashed replicas by restart),
  and fault-tolerant (per-container failures don't abort the pass).
- `cmd/nimbusd`: the node daemon — `run` (control loop with SIGHUP spec reload
  and a graceful shutdown that leaves workloads up), `status`, `down`.
- Restart backoff (CrashLoopBackOff): a crash-looping replica is restarted on
  an exponential delay (1s → 2s → 4s …, capped at 5m) instead of being
  hammered every reconcile; the streak is forgiven once the replica runs
  stably for 30s. First creation and external deletions still heal
  immediately — backoff is only for genuine crash loops. Backoff state is
  ephemeral controller state, never part of desired state.
- Exec liveness probes: a workload can declare a `liveness` exec check (with
  period, timeout, failureThreshold, initialDelaySeconds). The agent runs it
  inside each replica via the Docker exec API; a replica that fails the
  threshold consecutively is killed and restarted through the backoff path.
  Exec probes are namespace-local, so they work without container networking.
  New `runtime.Exec` (create/start/inspect over the Docker exec API).

- Integration test suite (`test/integration`, build tag `integration`): real
  end-to-end tests against a live Docker daemon — runtime lifecycle, exec,
  reconcile converge + self-heal, scale-down, and liveness kill+restart. A new
  CI job runs them on a Docker-equipped runner; `go test ./...` stays hermetic.
  `make integration` runs them locally.

### Simulation engine (the future "brain", retained)

- Multi-tenant heterogeneous workloads: complementary pod shapes (balanced
  web, CPU-heavy api, memory-heavy cache) with per-workload SLO accounting,
  so one tenant starving while another overshoots still counts as a miss.
- Holt-Winters additive seasonality in the predictive autoscaler: recurring
  patterns (the daily flash sale) are anticipated after one sighting instead
  of burning the cluster on every recurrence.
- Spot/preemptible cost tiers: the cluster autoscaler can fill up to a
  configured share of the fleet with spot nodes (~65% cheaper), keeping one
  empty node of spare headroom so a provider preemption's pods have somewhere
  to land while the replacement boots. New `-spot` benchmark row and a
  dollar-cost metric. Spot cuts the bill ~35% at comparable SLO.

## v0.1.0 — 2026-06-10

Initial release: algorithm core + benchmark harness.

- Predictive autoscaler using Holt double-exponential smoothing with
  lead-time forecasting and spike-acceleration over-provisioning.
- Scored bin-packing scheduler (tightest fit + image-locality bonus) with
  active consolidation on scale-down.
- Reference K8s-style baseline (reactive HPA + spread scheduling) and a
  shared cluster autoscaler.
- Deterministic 12-hour traffic simulator (diurnal curve, flash-sale
  spikes, seeded noise) with SLO / node-hour / utilization scoring.
- Stdlib-only SVG chart renderer (`results.svg`).
- Launch invariant test: ≥25% node-hour savings at equal SLO compliance
  across 5 traffic seeds.
