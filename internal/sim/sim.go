// Package sim replays a traffic trace against a full control plane
// (replica autoscaler + cluster autoscaler + scheduler) and scores it.
// One tick = one minute.
package sim

import (
	"math"
	"math/rand"

	"github.com/goncalofrazao/nimbus/internal/autoscaler"
	"github.com/goncalofrazao/nimbus/internal/cluster"
	"github.com/goncalofrazao/nimbus/internal/scheduler"
)

// RPSPerReplica is the throughput one healthy pod can serve.
const RPSPerReplica = 10.0

// TrafficTrace returns requests/sec over a window: diurnal ramp,
// two flash-sale spikes, and gaussian noise.
func TrafficTrace(ticks int, seed int64) []float64 {
	rng := rand.New(rand.NewSource(seed))
	trace := make([]float64, ticks)
	spikes := []struct {
		start, dur int
		height     float64
	}{{180, 30, 160}, {480, 20, 220}}

	for t := 0; t < ticks; t++ {
		diurnal := 120 + 90*math.Sin(math.Pi*float64(t)/float64(ticks))
		spike := 0.0
		for _, s := range spikes {
			if t >= s.start && t < s.start+s.dur {
				ramp := math.Min(1, float64(t-s.start)/6) // sharp ramp
				spike += s.height * ramp
			}
		}
		v := diurnal + spike + rng.NormFloat64()*8
		if v < 5 {
			v = 5
		}
		trace[t] = v
	}
	return trace
}

// clusterAutoscaler adds nodes for unschedulable pods and reclaims nodes
// that sit empty past a grace period. Shared by both control planes.
type clusterAutoscaler struct {
	emptyGrace int
	minNodes   int
}

func (c clusterAutoscaler) step(st *cluster.State, tick int) {
	// Scale UP: provision enough nodes for pending pods that fit nowhere.
	if len(st.Pending) > 0 {
		needCPU := 0
		for _, p := range st.Pending {
			needCPU += p.CPU
		}
		bootingCPU := 0
		for _, n := range st.Nodes {
			if !n.Ready(tick) {
				bootingCPU += n.CPUCap
			}
		}
		perNode := cluster.NewNode(0).CPUCap
		missing := needCPU - bootingCPU
		for i := 0; i < int(math.Ceil(float64(missing)/float64(perNode))); i++ {
			st.Nodes = append(st.Nodes, cluster.NewNode(tick))
		}
	}

	// Scale DOWN: reclaim nodes empty past the grace period.
	for _, n := range st.Nodes {
		if len(n.Pods) == 0 && n.EmptySince < 0 {
			n.EmptySince = tick
		}
	}
	var removable []*cluster.Node
	for _, n := range st.Nodes {
		if len(n.Pods) == 0 && n.Ready(tick) && n.EmptySince >= 0 &&
			tick-n.EmptySince >= c.emptyGrace {
			removable = append(removable, n)
		}
	}
	keep := len(st.Nodes) - len(removable)
	for len(removable) > 0 && keep < c.minNodes {
		removable = removable[:len(removable)-1]
		keep++
	}
	for _, n := range removable {
		st.RemoveNode(n)
	}
}

// Metrics collects the per-tick series and derives the scorecard.
type Metrics struct {
	Name     string
	Demand   []float64
	Capacity []float64
	Nodes    []int
	Replicas []int
	Util     []float64
}

func (m *Metrics) SLOViolationTicks() int {
	c := 0
	for i := range m.Demand {
		if m.Capacity[i] < m.Demand[i] {
			c++
		}
	}
	return c
}

func (m *Metrics) UnservedPct() float64 {
	unserved, total := 0.0, 0.0
	for i := range m.Demand {
		total += m.Demand[i]
		if d := m.Demand[i] - m.Capacity[i]; d > 0 {
			unserved += d
		}
	}
	return 100 * unserved / total
}

func (m *Metrics) NodeHours() float64 {
	s := 0
	for _, n := range m.Nodes {
		s += n
	}
	return float64(s) / 60
}

func (m *Metrics) AvgUtilization() float64 {
	s, c := 0.0, 0
	for _, u := range m.Util {
		if u > 0 {
			s += u
			c++
		}
	}
	if c == 0 {
		return 0
	}
	return 100 * s / float64(c)
}

func (m *Metrics) PeakNodes() int {
	best := 0
	for _, n := range m.Nodes {
		if n > best {
			best = n
		}
	}
	return best
}

// Run replays the trace against one control plane and returns its metrics.
func Run(name string, sched scheduler.Scheduler, as autoscaler.Autoscaler,
	trace []float64) *Metrics {

	st := &cluster.State{Nodes: []*cluster.Node{cluster.NewNode(-10)}} // warm node
	ca := clusterAutoscaler{emptyGrace: 8, minNodes: 1}
	m := &Metrics{Name: name}

	for tick, demand := range trace {
		as.Observe(demand)

		// 1. Replica autoscaling ----------------------------------------
		current := st.AllPods() + len(st.Pending)
		want := as.Desired(tick, demand, current)
		for current < want { // scale up: create pending pods
			st.Pending = append(st.Pending, cluster.NewPod("web"))
			current++
		}
		for current > want { // scale down: drain per scheduler policy
			if n := len(st.Pending); n > 0 {
				st.Pending = st.Pending[:n-1]
			} else {
				node, pod := scheduler.Victim(sched, st)
				if node == nil {
					break
				}
				node.RemovePod(pod)
			}
			current--
		}

		// 2. Node autoscaling + 3. Scheduling ----------------------------
		ca.step(st, tick)
		scheduler.Schedule(sched, st, tick)

		// 4. Serve traffic & record ---------------------------------------
		serving := st.RunningPods(tick)
		ready := st.ReadyNodes(tick)
		util := 0.0
		for _, n := range ready {
			util += n.Utilization()
		}
		if len(ready) > 0 {
			util /= float64(len(ready))
		}

		m.Demand = append(m.Demand, demand)
		m.Capacity = append(m.Capacity, float64(serving)*RPSPerReplica)
		m.Nodes = append(m.Nodes, len(st.Nodes))
		m.Replicas = append(m.Replicas, serving)
		m.Util = append(m.Util, util)
	}
	return m
}
