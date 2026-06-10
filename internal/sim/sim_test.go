package sim

import (
	"testing"

	"github.com/goncalofrazao/nimbus/internal/autoscaler"
	"github.com/goncalofrazao/nimbus/internal/scheduler"
)

// TestHeadToHead is the launch invariant: across several traffic seeds,
// Nimbus must (a) not be meaningfully worse on SLO and (b) be at least 25%
// cheaper in node-hours than the K8s-style control plane.
func TestHeadToHead(t *testing.T) {
	for _, seed := range []int64{1, 7, 42, 99, 2026} {
		trace := TrafficTrace(720, seed)
		k8s := Run("k8s", scheduler.Spread{}, autoscaler.NewReactive(RPSPerReplica), trace)
		nim := Run("nimbus", scheduler.BinPack{}, autoscaler.NewPredictive(RPSPerReplica), trace)

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

func TestCapacityNeverNegativeAndNodesBounded(t *testing.T) {
	trace := TrafficTrace(720, 42)
	m := Run("nimbus", scheduler.BinPack{}, autoscaler.NewPredictive(RPSPerReplica), trace)
	for i, c := range m.Capacity {
		if c < 0 {
			t.Fatalf("tick %d: negative capacity", i)
		}
	}
	if m.PeakNodes() > 100 {
		t.Fatalf("runaway node provisioning: peak=%d", m.PeakNodes())
	}
}
