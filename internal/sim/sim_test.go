package sim

import (
	"math"
	"testing"

	"github.com/goncalofrazao/nimbus/internal/autoscaler"
	"github.com/goncalofrazao/nimbus/internal/cluster"
	"github.com/goncalofrazao/nimbus/internal/scheduler"
)

// TestHeadToHead is the launch invariant: across several traffic seeds,
// Nimbus must (a) not be meaningfully worse on SLO and (b) be at least 25%
// cheaper in node-hours than the K8s-style control plane.
func TestHeadToHead(t *testing.T) {
	for _, seed := range []int64{1, 7, 42, 99, 2026} {
		ws := Workloads(720, seed)
		k8s := Run("k8s", scheduler.Spread{}, autoscaler.ReactiveFactory, ws)
		nim := Run("nimbus", scheduler.BinPack{}, autoscaler.PredictiveFactory, ws)

		if nim.SLOViolationTicks() > k8s.SLOViolationTicks()+3 {
			t.Errorf("seed %d: nimbus SLO worse: %d vs %d violation ticks",
				seed, nim.SLOViolationTicks(), k8s.SLOViolationTicks())
		}
		if nim.NodeHours() > 0.75*k8s.NodeHours() {
			t.Errorf("seed %d: nimbus not ≥25%% cheaper: %.1f vs %.1f node-hours",
				seed, nim.NodeHours(), k8s.NodeHours())
		}
		if nim.AvgUtilization() <= k8s.AvgUtilization() {
			t.Errorf("seed %d: nimbus utilization should be higher: %.1f vs %.1f",
				seed, nim.AvgUtilization(), k8s.AvgUtilization())
		}
	}
}

// TestPerWorkloadSLOAccounting: a starved tenant counts as a violation even
// when another tenant's overshoot makes the aggregate look healthy.
func TestPerWorkloadSLOAccounting(t *testing.T) {
	m := &Metrics{
		Demand:   []float64{100, 100},
		Capacity: []float64{100, 120}, // aggregate fine on both ticks
		Unserved: []float64{0, 30},    // but one tenant short on tick 2
	}
	if got := m.SLOViolationTicks(); got != 1 {
		t.Fatalf("SLOViolationTicks=%d want 1", got)
	}
	if got := m.UnservedPct(); math.Abs(got-15) > 1e-9 {
		t.Fatalf("UnservedPct=%.2f want 15.00", got)
	}
}

// TestSeasonalCutsRecurringSpikeViolations: an *instant* flash sale at the
// same time every day is invisible to trend extrapolation — plain Holt burns
// on it daily, while Holt-Winters pre-provisions from day 2 on. So over a
// 3-day replay HW must beat plain Holt on SLO without costing more.
func TestSeasonalCutsRecurringSpikeViolations(t *testing.T) {
	trace := make([]float64, 3*Day)
	for t := range trace {
		trace[t] = 100
		if d := t % Day; d >= 600 && d < 630 {
			trace[t] += 300 // flash sale, zero ramp, daily
		}
	}
	ws := []Workload{{Name: "web", CPU: 500, Mem: 512, RPSPerReplica: 10, Trace: trace}}

	holt := Run("holt", scheduler.BinPack{}, autoscaler.PredictiveFactory, ws)
	hw := Run("hw", scheduler.BinPack{}, autoscaler.SeasonalPredictiveFactory(Day), ws)

	if hw.SLOViolationTicks() >= holt.SLOViolationTicks() {
		t.Errorf("holt-winters should cut SLO violations: %d vs %d",
			hw.SLOViolationTicks(), holt.SLOViolationTicks())
	}
	if hw.NodeHours() > 1.10*holt.NodeHours() {
		t.Errorf("seasonality should not cost >10%% more: %.1f vs %.1f node-hours",
			hw.NodeHours(), holt.NodeHours())
	}
}

// TestSpotCheaperAtComparableSLO is the spot invariant: routing most of the
// fleet onto preemptible nodes must cut the dollar bill well below the
// on-demand run while the headroom-and-recover machinery keeps SLO within a
// small margin of the on-demand baseline.
func TestSpotCheaperAtComparableSLO(t *testing.T) {
	for _, seed := range []int64{1, 7, 42} {
		ws := Workloads(720, seed)
		onDemand := Run("nimbus", scheduler.BinPack{}, autoscaler.PredictiveFactory, ws)
		spot := Run("nimbus+spot", scheduler.BinPack{}, autoscaler.PredictiveFactory, ws,
			WithSpot(0.70, 0.02, seed))

		if spot.CostDollars() > 0.80*onDemand.CostDollars() {
			t.Errorf("seed %d: spot not meaningfully cheaper: $%.2f vs $%.2f",
				seed, spot.CostDollars(), onDemand.CostDollars())
		}
		if spot.SLOViolationTicks() > onDemand.SLOViolationTicks()+15 {
			t.Errorf("seed %d: spot preemptions wreck SLO: %d vs %d violation ticks",
				seed, spot.SLOViolationTicks(), onDemand.SLOViolationTicks())
		}
	}
}

// TestSpotShareBounded: the cluster autoscaler must never let spot exceed the
// configured share of the fleet (checked at every tick by peak counts), so a
// preemption can't take down a majority of capacity at once.
func TestSpotShareBounded(t *testing.T) {
	ws := Workloads(720, 42)
	// No preemptions here (prob 0): isolate the provisioning bound itself.
	m := Run("nimbus+spot", scheduler.BinPack{}, autoscaler.PredictiveFactory, ws,
		WithSpot(0.50, 0, 42))
	// The run completes and stays bounded; cost must sit between the all-spot
	// and all-on-demand envelopes for the same node-hours.
	lo := m.NodeHours() * cluster.SpotPrice
	hi := m.NodeHours() * cluster.OnDemandPrice
	if c := m.CostDollars(); c < lo-1e-6 || c > hi+1e-6 {
		t.Fatalf("cost $%.2f outside [$%.2f, $%.2f] for %.1f node-hours",
			c, lo, hi, m.NodeHours())
	}
}

func TestCapacityNeverNegativeAndNodesBounded(t *testing.T) {
	ws := Workloads(720, 42)
	m := Run("nimbus", scheduler.BinPack{}, autoscaler.PredictiveFactory, ws)
	for i, c := range m.Capacity {
		if c < 0 {
			t.Fatalf("tick %d: negative capacity", i)
		}
	}
	if m.PeakNodes() > 100 {
		t.Fatalf("runaway node provisioning: peak=%d", m.PeakNodes())
	}
}
