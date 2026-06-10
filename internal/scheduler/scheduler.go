// Package scheduler decides which node each pending pod lands on.
//
// Spread  ~ Kubernetes default (LeastAllocated): balance load across nodes.
//
//	Good for hot-spot avoidance, bad for cost — every node stays a
//	little busy, so the cluster autoscaler can never reclaim them.
//
// BinPack ~ Nimbus: multi-factor scoring that prefers the tightest fit and
//
//	rewards image locality, concentrating headroom on as few nodes
//	as possible so empties appear and can be reclaimed.
package scheduler

import "github.com/goncalofrazao/nimbus/internal/cluster"

// Scheduler picks placement targets and scale-down victims.
type Scheduler interface {
	Name() string
	// Pick chooses a node among candidates that all fit the pod.
	Pick(candidates []*cluster.Node, pod *cluster.Pod) *cluster.Node
	// VictimNode chooses which (non-empty) node loses a pod on scale-down.
	VictimNode(nodes []*cluster.Node) *cluster.Node
}

// Schedule places pending pods onto ready nodes. Returns number placed.
func Schedule(s Scheduler, st *cluster.State, tick int) int {
	placed := 0
	var still []*cluster.Pod
	for _, pod := range st.Pending {
		var candidates []*cluster.Node
		for _, n := range st.ReadyNodes(tick) {
			if n.Fits(pod) {
				candidates = append(candidates, n)
			}
		}
		if len(candidates) == 0 {
			still = append(still, pod)
			continue
		}
		n := s.Pick(candidates, pod)
		n.Pods = append(n.Pods, pod)
		n.EmptySince = -1
		placed++
	}
	st.Pending = still
	return placed
}

// Victim returns the (node, pod) to remove when the app scales down.
// Only nodes hosting at least one pod of the app are candidates.
func Victim(s Scheduler, st *cluster.State, app string) (*cluster.Node, *cluster.Pod) {
	var candidates []*cluster.Node
	for _, n := range st.Nodes {
		for _, p := range n.Pods {
			if p.App == app {
				candidates = append(candidates, n)
				break
			}
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	n := s.VictimNode(candidates)
	for i := len(n.Pods) - 1; i >= 0; i-- {
		if n.Pods[i].App == app {
			return n, n.Pods[i]
		}
	}
	return nil, nil
}

// ---------------------------------------------------------------------------

// Spread is the K8s-style scheduler.
type Spread struct{}

func (Spread) Name() string { return "spread (k8s-style)" }

func (Spread) Pick(c []*cluster.Node, _ *cluster.Pod) *cluster.Node {
	best := c[0]
	for _, n := range c[1:] {
		if n.Utilization() < best.Utilization() {
			best = n
		}
	}
	return best
}

// VictimNode removes from the most loaded node — "rebalances", but keeps
// every node partially occupied, blocking node reclamation.
func (Spread) VictimNode(nodes []*cluster.Node) *cluster.Node {
	best := nodes[0]
	for _, n := range nodes[1:] {
		if n.Utilization() > best.Utilization() {
			best = n
		}
	}
	return best
}

// ---------------------------------------------------------------------------

// BinPack is the Nimbus scheduler: tightest fit + image-locality bonus;
// scale-down drains the emptiest node first (active consolidation).
type BinPack struct{}

func (BinPack) Name() string { return "binpack (nimbus)" }

func (BinPack) Pick(c []*cluster.Node, pod *cluster.Pod) *cluster.Node {
	best, bestScore := c[0], score(c[0], pod)
	for _, n := range c[1:] {
		if sc := score(n, pod); sc > bestScore {
			best, bestScore = n, sc
		}
	}
	return best
}

func score(n *cluster.Node, pod *cluster.Pod) float64 {
	cpu := float64(n.CPUUsed()+pod.CPU) / float64(n.CPUCap)
	mem := float64(n.MemUsed()+pod.Mem) / float64(n.MemCap)
	after := cpu
	if mem > cpu {
		after = mem
	}
	locality := 0.0
	for _, p := range n.Pods {
		if p.App == pod.App {
			locality = 0.05 // warm image cache
			break
		}
	}
	return after + locality
}

func (BinPack) VictimNode(nodes []*cluster.Node) *cluster.Node {
	best := nodes[0]
	for _, n := range nodes[1:] {
		if n.Utilization() < best.Utilization() {
			best = n
		}
	}
	return best
}
