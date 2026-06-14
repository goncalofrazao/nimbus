package agent

import (
	"context"
	"sync"
	"time"

	"github.com/goncalofrazao/nimbus/internal/runtime"
	"github.com/goncalofrazao/nimbus/internal/spec"
)

// replicaProbe is the liveness history of one running replica.
type replicaProbe struct {
	containerID string    // resets the rest when the container is replaced
	seenAt      time.Time // first time we saw this container running (initial delay)
	lastProbe   time.Time // when we last executed the probe
	failures    int       // consecutive failures
}

// probeTable holds per-replica liveness state, keyed by container name. Like
// the backoff table it is ephemeral controller state, never desired state, and
// mutex-guarded so probes can run concurrently.
type probeTable struct {
	mu sync.Mutex
	m  map[string]*replicaProbe
}

func newProbeTable() *probeTable { return &probeTable{m: map[string]*replicaProbe{}} }

func (t *probeTable) forget(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.m, key)
}

// due decides, under lock, whether a probe should run for this replica now. It
// (re)initializes state when the underlying container changes (a restart
// resets the history), honors the initial delay and period, and stamps
// lastProbe when it returns true — so the blocking probe itself runs outside
// the lock and two passes never double-fire.
func (t *probeTable) due(key, containerID string, now time.Time, initialDelay, period time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.m[key]
	if p == nil || p.containerID != containerID {
		p = &replicaProbe{containerID: containerID, seenAt: now}
		t.m[key] = p
	}
	if now.Sub(p.seenAt) < initialDelay {
		return false
	}
	if !p.lastProbe.IsZero() && now.Sub(p.lastProbe) < period {
		return false
	}
	p.lastProbe = now
	return true
}

// record folds a probe result into the streak and reports whether the replica
// should be killed (threshold consecutive failures reached).
func (t *probeTable) record(key string, healthy bool, threshold int) (failures int, kill bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.m[key]
	if p == nil {
		return 0, false
	}
	if healthy {
		p.failures = 0
		return 0, false
	}
	p.failures++
	return p.failures, p.failures >= threshold
}

// probeResult is what one replica's liveness check produced this pass.
type probeResult struct {
	action Action
	hasAct bool
	killed bool // if set, the caller marks the replica exited so ensure restarts it
	key    replicaKey
}

// probeAll runs due liveness probes against the running replicas concurrently
// and reports which ones must be killed. Killing (the runtime Stop) happens in
// the parallel task, but mutating the shared `have` map is left to the caller,
// which does it sequentially after this returns.
func (r *Reconciler) probeAll(ctx context.Context, desired map[string]spec.Workload, have map[replicaKey]runtime.Container) ([]Action, []replicaKey) {
	now := r.now()

	type job struct {
		key   replicaKey
		c     runtime.Container
		probe spec.Probe
	}
	var jobs []job
	for k, c := range have {
		w, ok := desired[k.workload]
		if !ok || w.Liveness == nil || !c.Running() {
			continue
		}
		jobs = append(jobs, job{key: k, c: c, probe: *w.Liveness})
	}

	tasks := make([]func() probeResult, len(jobs))
	for i := range jobs {
		j := jobs[i]
		name := containerName(j.key.workload, j.key.replica)
		tasks[i] = func() probeResult {
			if !r.probes.due(name, j.c.ID, now, j.probe.InitialDelay(), j.probe.Period()) {
				return probeResult{}
			}
			healthy := r.runProbe(ctx, j.c.ID, j.probe)
			failures, kill := r.probes.record(name, healthy, j.probe.Threshold())
			if healthy {
				return probeResult{}
			}
			if !kill {
				return probeResult{hasAct: true, action: Action{Verb: "unhealthy",
					Workload: j.key.workload, Replica: j.key.replica, ID: j.c.ID, Failures: failures}}
			}
			// Threshold reached: kill it (the ensure phase restarts it under
			// backoff). Best-effort stop; force-remove is not used so the
			// container stays as an exited replica to restart in place.
			_ = r.rt.Stop(ctx, j.c.ID, stopGrace)
			r.probes.forget(name)
			return probeResult{hasAct: true, killed: true, key: j.key,
				action: Action{Verb: "killed", Workload: j.key.workload,
					Replica: j.key.replica, ID: j.c.ID, Failures: failures}}
		}
	}

	results := runParallel(r.parallelism, tasks)
	var actions []Action
	var killed []replicaKey
	for _, res := range results {
		if res.hasAct {
			actions = append(actions, res.action)
		}
		if res.killed {
			killed = append(killed, res.key)
		}
	}
	return actions, killed
}

// runProbe executes one exec probe under its timeout; healthy iff exit 0.
func (r *Reconciler) runProbe(ctx context.Context, id string, p spec.Probe) bool {
	pctx, cancel := context.WithTimeout(ctx, p.Timeout())
	defer cancel()
	code, err := r.rt.Exec(pctx, id, p.Exec)
	return err == nil && code == 0
}
