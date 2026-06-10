package scheduler

import (
	"testing"

	"github.com/goncalofrazao/nimbus/internal/cluster"
)

func nodeWith(tick int, pods ...*cluster.Pod) *cluster.Node {
	n := cluster.NewNode(tick)
	n.Pods = append(n.Pods, pods...)
	return n
}

func TestSpreadPicksLeastUtilized(t *testing.T) {
	empty := cluster.NewNode(-10)
	busy := nodeWith(-10, cluster.NewPod("web", 500, 512), cluster.NewPod("web", 500, 512))
	got := Spread{}.Pick([]*cluster.Node{busy, empty}, cluster.NewPod("web", 500, 512))
	if got != empty {
		t.Fatalf("spread should pick the emptiest node")
	}
}

func TestBinPackPicksTightestFit(t *testing.T) {
	empty := cluster.NewNode(-10)
	busy := nodeWith(-10, cluster.NewPod("web", 500, 512), cluster.NewPod("web", 500, 512))
	got := BinPack{}.Pick([]*cluster.Node{busy, empty}, cluster.NewPod("web", 500, 512))
	if got != busy {
		t.Fatalf("binpack should pick the fullest node that still fits")
	}
}

func TestBinPackImageLocalityBreaksTies(t *testing.T) {
	a := nodeWith(-10, cluster.NewPod("api", 500, 512))
	b := nodeWith(-10, cluster.NewPod("web", 500, 512))
	got := BinPack{}.Pick([]*cluster.Node{a, b}, cluster.NewPod("web", 500, 512))
	if got != b {
		t.Fatalf("equal utilization: binpack should prefer warm image cache")
	}
}

func TestScheduleRespectsCapacityAndBootDelay(t *testing.T) {
	st := &cluster.State{Nodes: []*cluster.Node{cluster.NewNode(0)}} // boots at 5
	for i := 0; i < 10; i++ {
		st.Pending = append(st.Pending, cluster.NewPod("web", 500, 512))
	}
	if placed := Schedule(BinPack{}, st, 2); placed != 0 {
		t.Fatalf("tick 2: node not Ready, placed=%d want 0", placed)
	}
	// tick 5: node ready; 4000m/8192Mi holds exactly 8 pods of 500m/512Mi.
	if placed := Schedule(BinPack{}, st, 5); placed != 8 {
		t.Fatalf("tick 5: placed=%d want 8 (node capacity)", placed)
	}
	if len(st.Pending) != 2 {
		t.Fatalf("pending=%d want 2", len(st.Pending))
	}
}

func TestVictimPolicies(t *testing.T) {
	low := nodeWith(-10, cluster.NewPod("web", 500, 512))
	high := nodeWith(-10, cluster.NewPod("web", 500, 512), cluster.NewPod("web", 500, 512), cluster.NewPod("web", 500, 512))
	st := &cluster.State{Nodes: []*cluster.Node{low, high}}

	if n, _ := Victim(BinPack{}, st, "web"); n != low {
		t.Fatalf("binpack should drain the emptiest node (consolidation)")
	}
	if n, _ := Victim(Spread{}, st, "web"); n != high {
		t.Fatalf("spread should remove from the most loaded node (rebalance)")
	}
}
