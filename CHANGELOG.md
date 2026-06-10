# Changelog

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
