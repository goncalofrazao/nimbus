package agent

import (
	"context"
	"testing"
	"time"

	"github.com/goncalofrazao/nimbus/internal/spec"
)

func TestBackoffDelaySchedule(t *testing.T) {
	cases := map[int]time.Duration{
		1:  1 * time.Second,
		2:  2 * time.Second,
		3:  4 * time.Second,
		4:  8 * time.Second,
		20: backoffCap, // far past the cap
	}
	for failures, want := range cases {
		if got := backoffDelay(failures); got != want {
			t.Errorf("backoffDelay(%d)=%s want %s", failures, got, want)
		}
	}
}

// clocked builds a reconciler whose clock the test controls.
func clocked(f *fakeRuntime) (*Reconciler, *time.Time) {
	r := New(f)
	clk := time.Now()
	r.now = func() time.Time { return clk }
	return r, &clk
}

// TestCrashLoopBacksOff: a container that keeps dying is restarted on an
// exponential delay, not hammered every pass.
func TestCrashLoopBacksOff(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	r, clk := clocked(f)
	s := specOf(spec.Workload{Name: "web", Image: "nginx", Replicas: 1})

	// Initial create heals immediately (no backoff for a never-seen replica).
	rep, _ := r.Reconcile(ctx, s)
	if v := verbOf(rep, "web", 0); v != "create" {
		t.Fatalf("initial action=%q want create", v)
	}

	// 1st crash → restart, streak 1, next eligible in 1s.
	f.crashOne("web")
	rep, _ = r.Reconcile(ctx, s)
	if a := actionOf(rep, "web", 0); a.Verb != "restart" || a.Failures != 1 {
		t.Fatalf("1st crash action=%v want restart/1", a)
	}

	// Crash again immediately: still inside the 1s window → backoff, no restart.
	f.crashOne("web")
	rep, _ = r.Reconcile(ctx, s)
	if a := actionOf(rep, "web", 0); a.Verb != "backoff" {
		t.Fatalf("within window action=%v want backoff", a)
	}
	if f.running("web") != 0 {
		t.Fatal("backoff must NOT restart the container")
	}
	if rep.Changed() {
		t.Fatal("a pass that only backs off is not a change")
	}

	// Advance past the window → eligible, restart, streak grows to 2.
	*clk = clk.Add(1100 * time.Millisecond)
	rep, _ = r.Reconcile(ctx, s)
	if a := actionOf(rep, "web", 0); a.Verb != "restart" || a.Failures != 2 {
		t.Fatalf("after window action=%v want restart/2", a)
	}
	if f.running("web") != 1 {
		t.Fatal("should be running again after the wait")
	}
}

// TestBackoffResetsAfterStable: a replica that stays up long enough has its
// crash streak forgiven, so the next crash starts the backoff from scratch.
func TestBackoffResetsAfterStable(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	r, clk := clocked(f)
	s := specOf(spec.Workload{Name: "web", Image: "nginx", Replicas: 1})

	r.Reconcile(ctx, s)
	f.crashOne("web")
	rep, _ := r.Reconcile(ctx, s) // restart, streak 1
	if actionOf(rep, "web", 0).Failures != 1 {
		t.Fatal("setup: want streak 1")
	}

	// Runs uninterrupted past the stable window → streak forgiven.
	*clk = clk.Add(backoffStable + time.Second)
	r.Reconcile(ctx, s) // observes running, resets

	// Next crash starts the streak over at 1, not 2.
	f.crashOne("web")
	rep, _ = r.Reconcile(ctx, s)
	if a := actionOf(rep, "web", 0); a.Verb != "restart" || a.Failures != 1 {
		t.Fatalf("post-reset action=%v want restart/1", a)
	}
}

// TestExternalDeleteHealsImmediately: a replica removed behind our back with
// no crash history is recreated at once — backoff is for crash loops, not for
// operator/external deletions.
func TestExternalDeleteHealsImmediately(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	r, _ := clocked(f)
	s := specOf(spec.Workload{Name: "web", Image: "nginx", Replicas: 1})

	r.Reconcile(ctx, s)
	// Wipe the container entirely (external delete).
	for id := range f.byID {
		delete(f.byID, id)
	}
	rep, _ := r.Reconcile(ctx, s)
	if a := actionOf(rep, "web", 0); a.Verb != "create" || a.Failures != 0 {
		t.Fatalf("external-delete action=%v want create/0 (immediate heal)", a)
	}
	if f.running("web") != 1 {
		t.Fatal("should have healed immediately")
	}
}

// helpers ------------------------------------------------------------------

func actionOf(r Report, workload string, replica int) Action {
	for _, a := range r.Actions {
		if a.Workload == workload && a.Replica == replica {
			return a
		}
	}
	return Action{Verb: "<none>"}
}

func verbOf(r Report, workload string, replica int) string {
	return actionOf(r, workload, replica).Verb
}
