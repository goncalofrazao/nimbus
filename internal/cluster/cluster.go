// Package cluster holds the core data model: Pods, Nodes, State.
package cluster

import "sync/atomic"

var podSeq, nodeSeq atomic.Int64

// Pod is one replica of a workload. Resources in millicores / MiB.
type Pod struct {
	ID  int64
	CPU int
	Mem int
	App string
}

// NewPod returns a pod of the given shape (millicores / MiB).
func NewPod(app string, cpu, mem int) *Pod {
	return &Pod{ID: podSeq.Add(1), CPU: cpu, Mem: mem, App: app}
}

// Capacity of the standard worker node (millicores / MiB).
const (
	NodeCPUCap = 4000
	NodeMemCap = 8192
)

// Node prices in $ per node-hour. Spot capacity is ~65% cheaper but the
// provider can preempt it at any time.
const (
	OnDemandPrice = 1.00
	SpotPrice     = 0.35
)

// Node is a worker. Nodes take BootDelay ticks to become Ready.
type Node struct {
	ID         int64
	CPUCap     int
	MemCap     int
	BootDelay  int
	CreatedAt  int
	Spot       bool
	Price      float64 // $ per node-hour
	Pods       []*Pod
	EmptySince int // tick when node last became empty; -1 = not empty
}

// NewNode provisions a standard on-demand node (4 cores, 8Gi) at the
// given tick.
func NewNode(tick int) *Node {
	return &Node{
		ID: nodeSeq.Add(1), CPUCap: NodeCPUCap, MemCap: NodeMemCap,
		BootDelay: 5, CreatedAt: tick, EmptySince: -1,
		Price: OnDemandPrice,
	}
}

// NewSpotNode provisions a spot-tier worker: same shape, ~65% cheaper,
// preemptible by the provider at any time.
func NewSpotNode(tick int) *Node {
	n := NewNode(tick)
	n.Spot = true
	n.Price = SpotPrice
	return n
}

func (n *Node) Ready(tick int) bool { return tick-n.CreatedAt >= n.BootDelay }

func (n *Node) CPUUsed() int {
	s := 0
	for _, p := range n.Pods {
		s += p.CPU
	}
	return s
}

func (n *Node) MemUsed() int {
	s := 0
	for _, p := range n.Pods {
		s += p.Mem
	}
	return s
}

func (n *Node) Fits(p *Pod) bool {
	return n.CPUUsed()+p.CPU <= n.CPUCap && n.MemUsed()+p.Mem <= n.MemCap
}

// Utilization is dominant-resource utilization in [0,1].
func (n *Node) Utilization() float64 {
	cpu := float64(n.CPUUsed()) / float64(n.CPUCap)
	mem := float64(n.MemUsed()) / float64(n.MemCap)
	if cpu > mem {
		return cpu
	}
	return mem
}

// RemovePod deletes pod by identity; returns true if found.
func (n *Node) RemovePod(p *Pod) bool {
	for i, q := range n.Pods {
		if q == p {
			n.Pods = append(n.Pods[:i], n.Pods[i+1:]...)
			return true
		}
	}
	return false
}

// State is the whole cluster: nodes plus unscheduled (pending) pods.
type State struct {
	Nodes   []*Node
	Pending []*Pod
}

func (s *State) ReadyNodes(tick int) []*Node {
	var out []*Node
	for _, n := range s.Nodes {
		if n.Ready(tick) {
			out = append(out, n)
		}
	}
	return out
}

// RunningPods are pods on Ready nodes — only these serve traffic.
func (s *State) RunningPods(tick int) int {
	c := 0
	for _, n := range s.Nodes {
		if n.Ready(tick) {
			c += len(n.Pods)
		}
	}
	return c
}

// AllPods counts scheduled pods on all nodes, ready or booting.
func (s *State) AllPods() int {
	c := 0
	for _, n := range s.Nodes {
		c += len(n.Pods)
	}
	return c
}

// AppPods counts scheduled pods of one app on all nodes, ready or booting.
func (s *State) AppPods(app string) int {
	c := 0
	for _, n := range s.Nodes {
		for _, p := range n.Pods {
			if p.App == app {
				c++
			}
		}
	}
	return c
}

// RunningAppPods counts app pods on Ready nodes — only these serve traffic.
func (s *State) RunningAppPods(tick int, app string) int {
	c := 0
	for _, n := range s.Nodes {
		if !n.Ready(tick) {
			continue
		}
		for _, p := range n.Pods {
			if p.App == app {
				c++
			}
		}
	}
	return c
}

// PendingOf counts unscheduled pods of one app.
func (s *State) PendingOf(app string) int {
	c := 0
	for _, p := range s.Pending {
		if p.App == app {
			c++
		}
	}
	return c
}

// RemovePending drops the most recently added pending pod of one app.
func (s *State) RemovePending(app string) bool {
	for i := len(s.Pending) - 1; i >= 0; i-- {
		if s.Pending[i].App == app {
			s.Pending = append(s.Pending[:i], s.Pending[i+1:]...)
			return true
		}
	}
	return false
}

// RemoveNode deletes node by identity.
func (s *State) RemoveNode(n *Node) {
	for i, m := range s.Nodes {
		if m == n {
			s.Nodes = append(s.Nodes[:i], s.Nodes[i+1:]...)
			return
		}
	}
}
