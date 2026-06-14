// Package sim replays per-workload traffic traces against a full control
// plane (replica autoscalers + cluster autoscaler + scheduler) and scores it.
// One tick = one minute.
package sim

import (
	"math"
	"math/rand"

	"github.com/goncalofrazao/nimbus/internal/autoscaler"
	"github.com/goncalofrazao/nimbus/internal/cluster"
	"github.com/goncalofrazao/nimbus/internal/scheduler"
)

// Workload is one tenant: a pod shape plus its own demand trace.
type Workload struct {
	Name          string
	CPU, Mem      int // pod shape (millicores / MiB)
	RPSPerReplica float64
	Trace         []float64
}

// Workloads returns the default multi-tenant set — a balanced web tier, a
// CPU-heavy api tier and a memory-heavy cache tier. The complementary shapes
// are where bin-packing earns its keep: spread scheduling strands capacity
// in one dimension on every node, tight packing fills both.
func Workloads(ticks int, seed int64) []Workload {
	return []Workload{
		{Name: "web", CPU: 500, Mem: 512, RPSPerReplica: 10,
			Trace: synthTrace(ticks, seed, 120, 90, 0,
				spike{180, 30, 160}, spike{480, 20, 220})},
		{Name: "api", CPU: 1200, Mem: 1024, RPSPerReplica: 20,
			Trace: synthTrace(ticks, seed+101, 70, 50, 0.7,
				spike{320, 25, 130})},
		{Name: "cache", CPU: 300, Mem: 2048, RPSPerReplica: 30,
			Trace: synthTrace(ticks, seed+202, 90, 40, 1.3,
				spike{540, 15, 150})},
	}
}

type spike struct {
	start, dur int
	height     float64
}

// Day is the seasonal period: spikes recur at the same wall-clock offset
// every Day ticks — the recurring flash sale Holt-Winters learns to expect.
const Day = 1440

// synthTrace builds a demand curve in rps: diurnal ramp (phase-shifted per
// tenant), sharp daily-recurring flash-sale spikes, and gaussian noise.
func synthTrace(ticks int, seed int64, base, amp, phase float64, spikes ...spike) []float64 {
	rng := rand.New(rand.NewSource(seed))
	out := make([]float64, ticks)
	for t := 0; t < ticks; t++ {
		v := base + amp*math.Sin(math.Pi*float64(t)/float64(ticks)+phase)
		for _, s := range spikes {
			if d := t % Day; d >= s.start && d < s.start+s.dur {
				v += s.height * math.Min(1, float64(d-s.start)/6) // sharp ramp
			}
		}
		v += rng.NormFloat64() * base / 15
		if v < 5 {
			v = 5
		}
		out[t] = v
	}
	return out
}

// clusterAutoscaler adds nodes for unschedulable pods and reclaims nodes
// that sit empty past a grace period. Shared by both control planes.
// With maxSpotShare > 0 it provisions spot-tier nodes up to that share of
// the fleet and keeps one node of spare headroom so a preemption's pods
// have somewhere to land while the replacement boots.
type clusterAutoscaler struct {
	emptyGrace   int
	minNodes     int
	maxSpotShare float64
}

// newNode picks the tier for the next provisioned node.
func (c clusterAutoscaler) newNode(st *cluster.State, tick int) *cluster.Node {
	if c.maxSpotShare > 0 {
		spot := 0
		for _, n := range st.Nodes {
			if n.Spot {
				spot++
			}
		}
		if float64(spot+1) <= c.maxSpotShare*float64(len(st.Nodes)+1) {
			return cluster.NewSpotNode(tick)
		}
	}
	return cluster.NewNode(tick)
}

func (c clusterAutoscaler) step(st *cluster.State, tick int) {
	// Scale UP: provision enough nodes for pending pods that fit nowhere.
	if len(st.Pending) > 0 {
		needCPU, needMem := 0, 0
		for _, p := range st.Pending {
			needCPU += p.CPU
			needMem += p.Mem
		}
		bootingCPU, bootingMem := 0, 0
		for _, n := range st.Nodes {
			if !n.Ready(tick) {
				bootingCPU += n.CPUCap
				bootingMem += n.MemCap
			}
		}
		missing := math.Ceil(float64(needCPU-bootingCPU) / cluster.NodeCPUCap)
		if m := math.Ceil(float64(needMem-bootingMem) / cluster.NodeMemCap); m > missing {
			missing = m
		}
		for i := 0; i < int(missing); i++ {
			st.Nodes = append(st.Nodes, c.newNode(st, tick))
		}
	}

	// Spot mode: keep one empty node as spare headroom for preemptions.
	if c.maxSpotShare > 0 {
		hasSpare := false
		for _, n := range st.Nodes {
			if len(n.Pods) == 0 {
				hasSpare = true
				break
			}
		}
		if !hasSpare {
			st.Nodes = append(st.Nodes, c.newNode(st, tick))
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
	if c.maxSpotShare > 0 && len(removable) > 0 {
		removable = removable[:len(removable)-1] // the spare survives reclaim
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

// preemptSpot models the provider reclaiming spot capacity: each spot node
// dies with probability prob per tick, its pods thrown back to pending.
func preemptSpot(st *cluster.State, rng *rand.Rand, prob float64) {
	var keep []*cluster.Node
	for _, n := range st.Nodes {
		if n.Spot && rng.Float64() < prob {
			st.Pending = append(st.Pending, n.Pods...)
			continue
		}
		keep = append(keep, n)
	}
	st.Nodes = keep
}

// Metrics collects the per-tick series and derives the scorecard.
// Demand and Capacity are aggregates across workloads; Unserved sums the
// per-workload shortfalls, so one tenant starving while another overshoots
// still counts as an SLO violation.
type Metrics struct {
	Name     string
	Demand   []float64
	Capacity []float64
	Unserved []float64
	Nodes    []int
	Replicas []int
	Util     []float64
	Cost     []float64 // $ accrued per tick across the fleet
}

func (m *Metrics) SLOViolationTicks() int {
	c := 0
	for _, u := range m.Unserved {
		if u > 0 {
			c++
		}
	}
	return c
}

func (m *Metrics) UnservedPct() float64 {
	unserved, total := 0.0, 0.0
	for i := range m.Demand {
		total += m.Demand[i]
		unserved += m.Unserved[i]
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

// CostDollars is the fleet bill: node-hours weighted by tier price.
func (m *Metrics) CostDollars() float64 {
	s := 0.0
	for _, c := range m.Cost {
		s += c
	}
	return s
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

// Option tweaks the control-plane configuration for a Run.
type Option func(*config)

type config struct {
	maxSpotShare float64
	preemptProb  float64
	preemptSeed  int64
}

// WithSpot lets the cluster autoscaler fill up to share of the fleet with
// spot-tier nodes, each preempted with probability prob per tick.
func WithSpot(share, prob float64, seed int64) Option {
	return func(c *config) {
		c.maxSpotShare = share
		c.preemptProb = prob
		c.preemptSeed = seed
	}
}

// Run replays the workload traces against one control plane and returns its
// metrics. newAutoscaler builds one replica autoscaler per workload.
func Run(name string, sched scheduler.Scheduler,
	newAutoscaler func(rpsPerReplica float64) autoscaler.Autoscaler,
	workloads []Workload, opts ...Option) *Metrics {

	var cfg config
	for _, o := range opts {
		o(&cfg)
	}

	st := &cluster.State{Nodes: []*cluster.Node{cluster.NewNode(-10)}} // warm node
	ca := clusterAutoscaler{emptyGrace: 8, minNodes: 1, maxSpotShare: cfg.maxSpotShare}
	var preemptRNG *rand.Rand
	if cfg.preemptProb > 0 {
		preemptRNG = rand.New(rand.NewSource(cfg.preemptSeed))
	}
	m := &Metrics{Name: name}

	as := make([]autoscaler.Autoscaler, len(workloads))
	for i, w := range workloads {
		as[i] = newAutoscaler(w.RPSPerReplica)
	}

	ticks := len(workloads[0].Trace)
	for tick := 0; tick < ticks; tick++ {
		// 0. Provider-side spot preemptions --------------------------------
		if preemptRNG != nil {
			preemptSpot(st, preemptRNG, cfg.preemptProb)
		}

		// 1. Per-workload replica autoscaling -----------------------------
		for i, w := range workloads {
			demand := w.Trace[tick]
			as[i].Observe(demand)
			current := st.AppPods(w.Name) + st.PendingOf(w.Name)
			want := as[i].Desired(tick, demand, current)
			for current < want { // scale up: create pending pods
				st.Pending = append(st.Pending, cluster.NewPod(w.Name, w.CPU, w.Mem))
				current++
			}
			for current > want { // scale down: drain per scheduler policy
				if !st.RemovePending(w.Name) {
					node, pod := scheduler.Victim(sched, st, w.Name)
					if node == nil {
						break
					}
					node.RemovePod(pod)
				}
				current--
			}
		}

		// 2. Node autoscaling + 3. Scheduling ------------------------------
		ca.step(st, tick)
		scheduler.Schedule(sched, st, tick)

		// 4. Serve traffic & record ----------------------------------------
		demand, capacity, unserved, serving := 0.0, 0.0, 0.0, 0
		for _, w := range workloads {
			d := w.Trace[tick]
			pods := st.RunningAppPods(tick, w.Name)
			c := float64(pods) * w.RPSPerReplica
			demand += d
			capacity += c
			serving += pods
			if d > c {
				unserved += d - c
			}
		}
		ready := st.ReadyNodes(tick)
		util := 0.0
		for _, n := range ready {
			util += n.Utilization()
		}
		if len(ready) > 0 {
			util /= float64(len(ready))
		}

		cost := 0.0
		for _, n := range st.Nodes {
			cost += n.Price
		}

		m.Demand = append(m.Demand, demand)
		m.Capacity = append(m.Capacity, capacity)
		m.Unserved = append(m.Unserved, unserved)
		m.Nodes = append(m.Nodes, len(st.Nodes))
		m.Replicas = append(m.Replicas, serving)
		m.Util = append(m.Util, util)
		m.Cost = append(m.Cost, cost/60)
	}
	return m
}
