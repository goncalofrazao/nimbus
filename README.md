# Nimbus

[![CI](https://github.com/goncalofrazao/nimbus/actions/workflows/ci.yml/badge.svg)](https://github.com/goncalofrazao/nimbus/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**A container orchestrator built from scratch in Go — the plumbing included.**

Nimbus runs your containers and keeps them running. You declare what should be
up; Nimbus continuously drives reality toward that declaration, restarting
whatever drifts. It talks to the container runtime itself, with **zero
dependencies** — the whole thing is the Go standard library.

The runtime client is hand-written: the Docker Engine API is just HTTP/JSON
over a Unix socket, so `internal/runtime` speaks it directly with `net/http`,
no Docker SDK. On a real Linux node this layer can later descend to
containerd/runc/namespaces behind the same interface.

> Heads-up: this is an early, single-node orchestrator under active
> construction. The reconcile core — declare, converge, self-heal — works
> today (see the demo below). The control-plane/agent split, persistence,
> multi-node scheduling, health checks and networking are on the roadmap.

## Quick start

Requires a reachable Docker daemon (`/var/run/docker.sock`).

```bash
go build -o nimbusd ./cmd/nimbusd

# Declare a cluster (2× nginx, 3× busybox worker) — see examples/cluster.json
./nimbusd apply -spec examples/cluster.json   # store the desired state
./nimbusd run                                 # the control loop (reads the store)
./nimbusd status                              # what's actually running
./nimbusd scale web 4                         # mutate desired state live
./nimbusd delete worker                       # the daemon reconciles it away
./nimbusd get                                 # print the stored desired state
./nimbusd down                                # tear down all containers
```

Desired state lives in a **persistent, crash-safe store** (a JSON file written
atomically). `apply`/`scale`/`delete` mutate it; `run` reconciles from it and
reloads each pass, so those commands take effect on a running daemon. For a
one-shot start you can still seed straight from a file: `nimbusd run -spec
examples/cluster.json`.

It self-heals. Kill a container behind its back and the next reconcile brings
it back:

```bash
docker rm -f nimbus-worker-1     # simulate a crash
./nimbusd status                 # ... worker-1 is recreated within one interval
```

`SIGHUP` reloads the spec from disk and reconciles immediately; `SIGINT`/
`SIGTERM` stop the daemon cleanly and **leave the workloads running** — the
daemon going down must never take your containers with it.

## How it works

```
   apply/scale/delete        reconcile (every interval)
  cluster.json ──▶ Store ──▶ Reconciler ──────▶ Docker Engine API
   (durable desired state)      │  list → diff → act      (real containers)
                                └─ create / restart / remove
```

- **`internal/spec`** — the declarative desired state (workloads = image +
  replica count + command), loaded and validated from JSON.
- **`internal/store`** — the durable, crash-safe desired-state store. Every
  mutation is written to a temp file, fsync'd, and atomically renamed over the
  real file (the directory is fsync'd too), so a crash leaves the last
  committed state or the previous one — never a torn write. A monotonic
  generation counter tracks changes.
- **`internal/runtime`** — the from-scratch Docker Engine API client over the
  Unix socket: pull, create, start, stop, remove, list, inspect.
- **`internal/agent`** — the reconcile loop, the reliability core. It is
  **stateless** (every pass rebuilds its picture of the world by asking the
  runtime — the daemon is the source of truth, never our memory),
  **idempotent** (a converged cluster yields no actions), **convergent**
  (each pass self-heals crashed replicas), and **fault-tolerant** (one
  container's failure is recorded and skipped, never aborting the pass).
  Crashing replicas restart on **exponential backoff** (CrashLoopBackOff), and
  **exec liveness probes** catch replicas that are up but wedged and restart
  them. Each pass runs its per-replica work (probes, creates, restarts, removes)
  with **bounded concurrency**, so converging or tearing down many replicas is
  fast instead of serial — the shared controller state (backoff and probe
  tables) is mutex-guarded.
- **`cmd/nimbusd`** — the node daemon: control loop, `status`, `down`,
  signal-driven reload and graceful shutdown.

Every container Nimbus owns is stamped with `nimbus.managed`, `nimbus.workload`
and `nimbus.replica` labels and given a deterministic name
(`nimbus-<workload>-<replica>`), so replicas have a durable identity and the
agent can always find — or rebuild — replica *N*.

## The brain (in simulation)

Alongside the orchestrator lives a dependency-free **scheduling & autoscaling
research engine** with a reproducible benchmark (`internal/{autoscaler,
scheduler,sim}`, `cmd/nimbus`): predictive (Holt / Holt-Winters) autoscaling
and scored bin-packing, measured head-to-head against a K8s-style baseline.
These are the algorithms that will eventually drive Nimbus's real scheduler;
for now they prove out in simulation while the orchestrator's plumbing is
built. Run `make run` / `make chart`.

## Layout

```
cmd/nimbusd/           the node daemon (run / apply / scale / delete / get / status / down)
internal/runtime/      hand-written Docker Engine API client (stdlib only)
internal/spec/         declarative desired state + validation
internal/store/        durable, crash-safe desired-state store
internal/agent/        the reconcile loop (declare → converge → self-heal)
examples/              sample cluster specs

cmd/nimbus/            simulation benchmark CLI (the "brain", below)
internal/{autoscaler,scheduler,sim,cluster,report}/   scheduling research engine
test/integration/      end-to-end tests against a real Docker daemon
```

## Testing

```bash
make test          # hermetic unit tests — no Docker needed (also runs in CI)
make integration   # end-to-end tests against a real Docker daemon
```

Unit tests cover the reconcile/backoff/probe logic against an in-memory fake
runtime, so they're fast and need no daemon. The integration suite
(`test/integration`, behind the `integration` build tag) drives the real
runtime client and reconcile loop against live containers — converge,
self-heal, scale-down, liveness kill+restart. Both run in CI on every push.

## Roadmap

Toward a reliable, real cluster manager:

- [x] Hand-written container runtime client (Docker Engine API, stdlib only)
- [x] Declarative desired state + self-healing reconcile loop (single node)
- [x] Restart backoff (CrashLoopBackOff) for crashing replicas
- [x] Exec liveness probes (restart wedged-but-running replicas)
- [x] Persistent, crash-safe desired-state store (survives restarts)
- [x] Concurrent reconcile (bounded parallelism; fast teardown of many replicas)
- [ ] Readiness probes + service routing
- [ ] Control-plane / node-agent split over a real API
- [ ] Multi-node scheduling (place replicas across hosts)
- [ ] Pod networking & service discovery
- [ ] Wire in the predictive autoscaler + bin-packing scheduler as the brain

## License

[MIT](LICENSE)
