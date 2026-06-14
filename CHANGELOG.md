# Changelog

## Unreleased — toward v1.0

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
