package agent

import (
	"context"
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
// the backoff table it is ephemeral controller state, never desired state.
type probeTable struct {
	m map[string]*replicaProbe
}

func newProbeTable() *probeTable { return &probeTable{m: map[string]*replicaProbe{}} }

func (t *probeTable) forget(key string) { delete(t.m, key) }

// track returns the probe state for a replica, (re)initializing it whenever
// the underlying container changes identity (a restart resets the history).
func (t *probeTable) track(key, containerID string, now time.Time) *replicaProbe {
	p := t.m[key]
	if p == nil || p.containerID != containerID {
		p = &replicaProbe{containerID: containerID, seenAt: now}
		t.m[key] = p
	}
	return p
}

// probeAll runs due liveness probes against the running replicas and kills any
// that have failed FailureThreshold times in a row. A killed container is set
// to "exited" in have so the caller's ensure loop restarts it under backoff.
func (r *Reconciler) probeAll(ctx context.Context, desired map[string]spec.Workload, have map[replicaKey]runtime.Container) []Action {
	now := r.now()
	var actions []Action

	for k, c := range have {
		w, ok := desired[k.workload]
		if !ok || w.Liveness == nil || !c.Running() {
			continue
		}
		probe := *w.Liveness
		st := r.probes.track(containerName(k.workload, k.replica), c.ID, now)

		// Respect the initial delay and the probe period.
		if now.Sub(st.seenAt) < probe.InitialDelay() {
			continue
		}
		if !st.lastProbe.IsZero() && now.Sub(st.lastProbe) < probe.Period() {
			continue
		}
		st.lastProbe = now

		healthy := r.runProbe(ctx, c.ID, probe)
		if healthy {
			st.failures = 0
			continue
		}
		st.failures++
		if st.failures < probe.Threshold() {
			actions = append(actions, Action{Verb: "unhealthy", Workload: k.workload,
				Replica: k.replica, ID: c.ID, Failures: st.failures})
			continue
		}

		// Threshold reached: kill it. Best-effort stop; the ensure loop will
		// restart it (subject to crash-loop backoff). Mark it exited locally so
		// that restart actually happens this pass.
		_ = r.rt.Stop(ctx, c.ID, stopGrace)
		c.State = "exited"
		have[k] = c
		r.probes.forget(containerName(k.workload, k.replica))
		actions = append(actions, Action{Verb: "killed", Workload: k.workload,
			Replica: k.replica, ID: c.ID, Failures: st.failures})
	}
	return actions
}

// runProbe executes one exec probe under its timeout; healthy iff exit 0.
func (r *Reconciler) runProbe(ctx context.Context, id string, p spec.Probe) bool {
	pctx, cancel := context.WithTimeout(ctx, p.Timeout())
	defer cancel()
	code, err := r.rt.Exec(pctx, id, p.Exec)
	return err == nil && code == 0
}
