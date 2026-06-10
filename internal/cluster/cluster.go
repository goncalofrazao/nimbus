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

// NewPod returns a standard web pod (500m CPU, 512Mi).
func NewPod(app string) *Pod {
	return &Pod{ID: podSeq.Add(1), CPU: 500, Mem: 512, App: app}
}

// Node is a worker. Nodes take BootDelay ticks to become Ready.
type Node struct {
	ID         int64
	CPUCap     int
	MemCap     int
	BootDelay  int
	CreatedAt  int
	Pods       []*Pod
	EmptySince int // tick when node last became empty; -1 = not empty
}

// NewNode provisions a standard node (4 cores, 8Gi) at the given tick.
func NewNode(tick int) *Node {
	return &Node{
		ID: nodeSeq.Add(1), CPUCap: 4000, MemCap: 8192,
		BootDelay: 5, CreatedAt: tick, EmptySince: -1,
	}
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

// RemoveNode deletes node by identity.
func (s *State) RemoveNode(n *Node) {
	for i, m := range s.Nodes {
		if m == n {
			s.Nodes = append(s.Nodes[:i], s.Nodes[i+1:]...)
			return
		}
	}
}
