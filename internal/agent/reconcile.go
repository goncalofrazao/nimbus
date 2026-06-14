// Package agent is Nimbus's node-level reconciler — the kubelet-equivalent.
// It drives the actual set of running containers toward a desired spec.Spec,
// and it does so statelessly: every pass rebuilds its picture of the world by
// asking the runtime what is really there, so the agent can crash, restart,
// and converge again with no memory of its own.
//
// Reconciliation is the reliability core. It is:
//   - idempotent  — running it twice on a converged cluster does nothing;
//   - convergent  — each pass moves reality closer to desired and self-heals
//     crashed containers by restarting them;
//   - fault-tolerant — one container's failure is recorded and skipped, never
//     aborting the rest of the pass.
package agent

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/goncalofrazao/nimbus/internal/runtime"
	"github.com/goncalofrazao/nimbus/internal/spec"
)

// stopGrace is how long a container gets to exit cleanly before it is killed.
const stopGrace = 5 * time.Second

// Runtime is the subset of the container runtime the agent depends on. Keeping
// it an interface lets the reconcile logic be unit-tested against a fake and
// lets the Docker backend be swapped for containerd/runc later.
type Runtime interface {
	Pull(ctx context.Context, image string) error
	Create(ctx context.Context, spec runtime.ContainerSpec) (string, error)
	Start(ctx context.Context, id string) error
	Stop(ctx context.Context, id string, timeout time.Duration) error
	Remove(ctx context.Context, id string, force bool) error
	List(ctx context.Context, labels map[string]string) ([]runtime.Container, error)
}

// Reconciler converges one node's containers toward the desired spec.
type Reconciler struct {
	rt      Runtime
	backoff *backoffTable
	now     func() time.Time // injectable clock for deterministic tests
}

// New returns a Reconciler backed by rt.
func New(rt Runtime) *Reconciler {
	return &Reconciler{rt: rt, backoff: newBackoffTable(), now: time.Now}
}

// Action records one thing the reconciler did (or tried to do) this pass.
type Action struct {
	Verb     string // create, restart, remove, backoff, none, error
	Workload string
	Replica  int
	ID       string
	Err      error
	Failures int           // crash-loop streak length (restart/backoff)
	Wait     time.Duration // remaining backoff wait (backoff)
}

func (a Action) String() string {
	id := ""
	if a.ID != "" {
		id = " " + shortID(a.ID)
	}
	s := fmt.Sprintf("%-7s %s/%d%s", a.Verb, a.Workload, a.Replica, id)
	if a.Verb == "backoff" {
		s += fmt.Sprintf(" (crash #%d, wait %s)", a.Failures, a.Wait.Round(time.Second))
	}
	if a.Err != nil {
		s += " — " + a.Err.Error()
	}
	return s
}

// Report is the outcome of a single reconcile pass.
type Report struct {
	Actions []Action
}

// Changed reports whether the pass mutated the cluster at all. A converged
// cluster — and one merely waiting out a backoff — yields Changed()==false.
func (r Report) Changed() bool {
	for _, a := range r.Actions {
		if a.Verb != "none" && a.Verb != "backoff" {
			return true
		}
	}
	return false
}

// Errs returns the actions that failed.
func (r Report) Errs() []Action {
	var out []Action
	for _, a := range r.Actions {
		if a.Err != nil {
			out = append(out, a)
		}
	}
	return out
}

// containerName is the deterministic name for a workload replica. Stable
// names give every replica a durable identity, so reconcile needs no state of
// its own — it can always find (or recreate) replica N by name/label.
func containerName(workload string, replica int) string {
	return fmt.Sprintf("nimbus-%s-%d", workload, replica)
}

// Reconcile performs one full convergence pass and returns what it did. It
// never returns an error for individual container failures (those are captured
// as Actions with Err set); it returns an error only if it cannot see the
// world at all (the initial List failed), since acting blind could be unsafe.
func (r *Reconciler) Reconcile(ctx context.Context, s *spec.Spec) (Report, error) {
	actual, err := r.rt.List(ctx, nil)
	if err != nil {
		return Report{}, fmt.Errorf("list world: %w", err)
	}

	// Index the world by (workload, replica). Anything managed by Nimbus but
	// not mappable to a desired replica is an orphan to be reclaimed.
	type key struct {
		workload string
		replica  int
	}
	have := make(map[key]runtime.Container, len(actual))
	var orphans []runtime.Container
	for _, c := range actual {
		w := c.Labels[runtime.LabelWorkload]
		rep, err := strconv.Atoi(c.Labels[runtime.LabelReplica])
		if w == "" || err != nil {
			orphans = append(orphans, c)
			continue
		}
		have[key{w, rep}] = c
	}

	desired := make(map[string]spec.Workload, len(s.Workloads))
	for _, w := range s.Workloads {
		desired[w.Name] = w
	}

	var rep Report

	// 1. Ensure every desired replica exists and is running.
	for _, name := range s.Names() {
		w := desired[name]
		for i := 0; i < w.Replicas; i++ {
			k := key{w.Name, i}
			cur, ok := have[k]
			delete(have, k) // consume; whatever remains is excess
			rep.Actions = append(rep.Actions, r.ensureReplica(ctx, w, i, cur, ok))
		}
	}

	// 2. Anything left in `have` is an excess replica (count scaled down or
	//    workload removed). Reclaim it.
	for k, c := range have {
		rep.Actions = append(rep.Actions, r.removeContainer(ctx, k.workload, k.replica, c))
	}

	// 3. Reclaim orphans (managed containers with no usable identity).
	for _, c := range orphans {
		rep.Actions = append(rep.Actions, r.removeContainer(ctx, c.Workload(), -1, c))
	}

	return rep, nil
}

// ensureReplica guarantees replica i of w is present and running, pacing
// restarts of a crash-looping replica with exponential backoff.
func (r *Reconciler) ensureReplica(ctx context.Context, w spec.Workload, i int, cur runtime.Container, exists bool) Action {
	key := containerName(w.Name, i)
	now := r.now()

	// Healthy: nothing to do, and forgive the crash streak once it has been
	// up long enough.
	if exists && cur.Running() {
		r.backoff.observeRunning(key, now)
		return Action{Verb: "none", Workload: w.Name, Replica: i, ID: cur.ID}
	}

	// A brand-new replica we've never seen heals immediately — only a replica
	// that has died on us is subject to backoff. (An exited container, or one
	// removed behind our back after a prior restart, counts as a death.)
	firstCreate := !exists && !r.backoff.known(key)
	if !firstCreate {
		if ok, wait := r.backoff.ready(key, now); !ok {
			b := r.backoff.m[key]
			return Action{Verb: "backoff", Workload: w.Name, Replica: i, ID: cur.ID,
				Failures: b.failures, Wait: wait}
		}
	}

	// Eligible to act.
	if exists {
		if err := r.rt.Start(ctx, cur.ID); err != nil {
			// Vanished underneath us — recreate from scratch.
			id, cerr := r.createReplica(ctx, w, i)
			if cerr != nil {
				return Action{Verb: "error", Workload: w.Name, Replica: i, ID: cur.ID, Err: err}
			}
			n := r.backoff.recordRestart(key, now)
			return Action{Verb: "create", Workload: w.Name, Replica: i, ID: id, Failures: n}
		}
		n := r.backoff.recordRestart(key, now)
		return Action{Verb: "restart", Workload: w.Name, Replica: i, ID: cur.ID, Failures: n}
	}

	id, err := r.createReplica(ctx, w, i)
	if err != nil {
		return Action{Verb: "error", Workload: w.Name, Replica: i, Err: err}
	}
	a := Action{Verb: "create", Workload: w.Name, Replica: i, ID: id}
	if !firstCreate {
		a.Failures = r.backoff.recordRestart(key, now)
	}
	return a
}

// createReplica pulls the image (cheap if cached), creates the container with
// its deterministic name and ownership labels, and starts it.
func (r *Reconciler) createReplica(ctx context.Context, w spec.Workload, i int) (string, error) {
	if err := r.rt.Pull(ctx, w.Image); err != nil {
		return "", err
	}
	id, err := r.rt.Create(ctx, runtime.ContainerSpec{
		Name:  containerName(w.Name, i),
		Image: w.Image,
		Cmd:   w.Cmd,
		Env:   w.Env,
		Labels: map[string]string{
			runtime.LabelWorkload: w.Name,
			runtime.LabelReplica:  strconv.Itoa(i),
		},
	})
	if err != nil {
		return "", err
	}
	if err := r.rt.Start(ctx, id); err != nil {
		return id, err
	}
	return id, nil
}

// removeContainer stops then force-removes a container, returning the action.
func (r *Reconciler) removeContainer(ctx context.Context, workload string, replica int, c runtime.Container) Action {
	if replica >= 0 {
		r.backoff.forget(containerName(workload, replica)) // no longer desired
	}
	_ = r.rt.Stop(ctx, c.ID, stopGrace) // best-effort; Remove(force) finishes it
	if err := r.rt.Remove(ctx, c.ID, true); err != nil {
		return Action{Verb: "error", Workload: workload, Replica: replica, ID: c.ID, Err: err}
	}
	return Action{Verb: "remove", Workload: workload, Replica: replica, ID: c.ID}
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}
