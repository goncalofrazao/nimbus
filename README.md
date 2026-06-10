# Nimbus

[![CI](https://github.com/goncalofrazao/nimbus/actions/workflows/ci.yml/badge.svg)](https://github.com/goncalofrazao/nimbus/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**An intelligent scheduling & autoscaling engine — what the Kubernetes
control plane should have been.**

Nimbus replaces the two weakest muscles of today's orchestrators:

1. **Predictive autoscaling.** The Kubernetes HPA is reactive: it sees high
   utilization *now* and scales *now* — but new capacity takes minutes (node
   boot, image pull), so every spike burns you. Nimbus runs Holt
   double-exponential smoothing over the demand signal, tracking level *and*
   trend, and provisions for the forecast `lead` ticks ahead, where `lead`
   covers node boot time. Capacity arrives **before** demand does. A
   spike-acceleration term over-provisions when demand rises steeply.

2. **Scored bin-packing with active consolidation.** The K8s default spreads
   pods across nodes, keeping every node a little busy — so the cluster
   autoscaler can never reclaim anything. Nimbus scores nodes for tightest
   fit (plus an image-locality bonus) and, on scale-down, drains the
   *emptiest* node first so empties appear and get reclaimed.

## Benchmark

Both control planes replay an **identical** 12-hour traffic trace
(diurnal curve + two flash-sale spikes + noise):

| control plane | SLO violation min | node-hours | avg utilization |
|---|---|---|---|
| K8s-style (reactive HPA + spread) | 5 | 102–165 | 37–59% |
| **Nimbus** (predictive + binpack) | **5** | **~73** | **~79%** |

**≈ 28–55% infrastructure cost saved at identical SLO compliance**, enforced
as a test invariant across 5 traffic seeds (`internal/sim/sim_test.go`).

## Quick start

```bash
make run            # head-to-head comparison
make chart          # + results.svg / results.json
go test ./...       # unit + invariant tests
./nimbus -seed 7 -ticks 1440   # different noise, 24h window
```

Zero dependencies — pure Go standard library, single static binary.

## Layout

```
cmd/nimbus/            CLI entrypoint
internal/cluster/      Pod, Node, State
internal/scheduler/    Spread (k8s-style) vs BinPack (nimbus) — Scheduler interface
internal/autoscaler/   Reactive (HPA-style) vs Predictive — Autoscaler interface
internal/sim/          traffic generator, cluster autoscaler, tick loop, metrics
internal/report/       stdlib-only SVG chart renderer
```

The `Scheduler` and `Autoscaler` interfaces are the extension points: the
simulation engine is the same harness you'd use to regression-test any new
policy before it touches a real cluster.

## Roadmap

- [ ] Replay real Prometheus/Cloud Monitoring traces instead of synthetic traffic
- [ ] Multi-tenant workloads with heterogeneous pod shapes (bin-packing gains grow)
- [ ] Spot/preemptible cost tiers in the cluster autoscaler
- [ ] Holt-Winters seasonality (daily/weekly periodicity)
- [ ] PodDisruptionBudget-aware consolidation
- [ ] Kubernetes scheduler-plugin adapter (run Nimbus scoring inside a real cluster)

## Status

v0.1 — algorithm core + benchmark harness. This is a research-grade engine
with a reproducible benchmark, not yet a production cluster manager.

## License

[MIT](LICENSE)
