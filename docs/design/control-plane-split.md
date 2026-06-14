# Design: Control-plane / node-agent split → multi-node

Status: accepted · Date: 2026-06-15

Nimbus today is a single process that holds desired state and reconciles
containers on one host. This milestone splits it into a **control plane** (holds
cluster-wide desired state, tracks nodes, assigns work) and **node agents**
(run containers on one host each), so Nimbus can manage more than one machine.

## Goals

- One control plane coordinating many node agents over a network.
- Reuse the existing reconcile loop, backoff, probes and runtime client on the
  agent unchanged — the split is about *coordination*, not re-litigating
  single-node reliability.
- Stay zero-dependency: HTTP/JSON over the standard library.
- Reliability first: an agent that can't reach the control plane keeps its
  workloads running; a control-plane restart recovers from the durable store.

## Non-goals (handled later)

- Smart scheduling (bin-packing) — Epic D wires in the existing scheduler.
- Pod networking / service discovery / readiness routing.
- Multi-control-plane HA / consensus. One control plane for now (its state is
  durable; restart-recovery is in scope, leader election is not).

## Decisions

1. **Pull-based assignment.** Agents poll the control plane for their assigned
   desired state and POST heartbeats/status; the control plane holds no
   long-lived connections. This is essentially how kubelet works, it's the
   simplest thing to build correctly on `net/http`, and it degrades gracefully
   — an agent just retries. Cost: change propagation is bounded by the poll
   interval (acceptable).

2. **One binary, multiple modes.** A single `nimbusd`:
   - `nimbusd serve` — control plane (HTTP API + store + node registry + assigner)
   - `nimbusd agent` — node agent (registers, polls, reconciles, reports)
   - `nimbusd apply|get|scale|delete|status` — operator CLI, talking to the
     control plane over HTTP
   - `nimbusd run` — the existing all-in-one single-node mode, kept for local use

## Architecture

```
              HTTP / JSON  (net/http, stdlib only)
  operator ──▶  CONTROL PLANE  ◀── register / poll / report ──  AGENT  ──▶ Docker
  apply/get/      durable store (desired state)                reconciles its
  scale/delete    node registry (liveness)                     assigned replicas
  status          assigner (replicas → nodes)                  (reuses Reconciler)
```

- **Control plane** owns the persistent `internal/store` (cluster desired
  state), an in-memory **node registry** (which agents are alive + their last
  reported status), and an **assigner** that maps desired replicas to nodes.
  It is otherwise stateless toward agents.
- **Agent** is a thin client wrapping the existing `agent.Reconciler`: register
  once, then loop { heartbeat+report, pull assignment, reconcile locally }.

## HTTP API (v1)

Operator surface (used by the CLI):

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/workloads` | apply (upsert) one or more workloads |
| `GET` | `/v1/workloads` | list desired state |
| `GET` | `/v1/workloads/{name}` | get one workload |
| `POST` | `/v1/workloads/{name}/scale` | set replica count |
| `DELETE` | `/v1/workloads/{name}` | remove a workload |
| `GET` | `/v1/status` | cluster status: nodes + workloads + observed replicas |

Agent surface:

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/agents/register` | register a node, returns a node id |
| `POST` | `/v1/agents/{id}/heartbeat` | liveness + observed-status report |
| `GET` | `/v1/agents/{id}/assignment` | the desired-state slice this node should run |

Health: `GET /healthz`.

All bodies are JSON. The desired-state slice reuses `spec.Spec`. Auth is a
shared-secret bearer token (Epic C).

## Data model

- **Desired state** — `internal/store` (already durable + crash-safe), now owned
  by the control plane.
- **Node registry** — `nodeID → { addr, lastHeartbeat, status, observed[] }`,
  in memory (rebuilt as agents re-register after a control-plane restart).
- **Assignment** — `nodeID → spec.Spec`. Epic A: the whole spec goes to the one
  node. Epic B: the assigner spreads replicas across live nodes.

## Multi-node on a single Docker host (for testing)

Real nodes have their own Docker daemon, so container names never collide. To
test multiple agents against one daemon, each agent scopes its world by a
**node id**: a `nimbus.node=<id>` label plus node-prefixed container names, and
the agent's `List` filters on that label so it only manages its own replicas.
Designed in from Epic B (B2); Epic A's single agent needs no scoping.

## Backlog (epics → stories)

**Epic A — Control plane + single agent (vertical slice, no scheduling)**
- A1 — HTTP API server + operator desired-state endpoints over the store;
  `nimbusd serve`; CLI mutators talk HTTP.
- A2 — Node registry + register/heartbeat endpoints; liveness tracking.
- A3 — Assignment endpoint (single node: whole spec).
- A4 — `nimbusd agent`: register, poll assignment, reconcile locally.
- A5 — Status reporting: agent reports observed replicas; `nimbusd status`
  shows nodes + workloads via the control plane.

**Epic B — Multi-node assignment**
- B1 — Assigner interface + trivial spread across live nodes.
- B2 — Per-node reconcile scoping (node-id label + name scoping).
- B3 — Reassignment on node loss (missed heartbeats → move replicas).
- B4 — Multi-agent integration test (2+ agents, one Docker host).

**Epic C — Robustness & durability**
- C1 — Agent survives control-plane outage (keep last assignment, retry).
- C2 — Control-plane restart recovery (store + re-registration).
- C3 — Shared-secret auth token + request validation.
- C4 — Observability (`/healthz`, structured logs, placement in status).

**Epic D — Wire in the brain**
- D1 — Resource model (cpu/mem requests) + node capacity reporting.
- D2 — Replace the trivial assigner with the scored bin-packing scheduler.
- D3 — Consolidation / rebalance pass.

## Testing strategy

- Unit: handlers against the store with `httptest`; assigner logic; agent
  client against a fake control plane.
- Integration (`integration` build tag, real Docker): bring up a control plane
  + agent(s) in-process, apply a spec, assert containers converge; kill an
  agent's container, assert self-heal; (Epic B) two agents split replicas.
