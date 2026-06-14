package agent

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/goncalofrazao/nimbus/internal/spec"
)

func (f *fakeRuntime) idOf(workload string, replica int) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := fmt.Sprintf("nimbus-%s-%d", workload, replica)
	for id, c := range f.byID {
		if c.Name == name {
			return id
		}
	}
	return ""
}

func liveSpec(exec []string, threshold, initialDelay int) *spec.Spec {
	return specOf(spec.Workload{
		Name: "web", Image: "nginx", Replicas: 1,
		Liveness: &spec.Probe{
			Exec: exec, FailureThreshold: threshold, PeriodSeconds: 10,
			InitialDelaySec: initialDelay,
		},
	})
}

// TestHealthyProbeNoAction: a replica passing its probe is never disturbed,
// and the probe actually runs (at most once per period).
func TestHealthyProbeNoAction(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	r, clk := clocked(f)
	s := liveSpec([]string{"true"}, 3, 0)

	r.Reconcile(ctx, s) // create web-0 (healthy: exitFor unset -> 0)
	for i := 0; i < 3; i++ {
		*clk = clk.Add(11 * time.Second) // past the period each time
		rep, _ := r.Reconcile(ctx, s)
		if rep.Changed() {
			t.Fatalf("healthy probe should not change anything: %v", rep.Actions)
		}
	}
	if f.running("web") != 1 {
		t.Fatal("healthy replica should still be running")
	}
	if f.execs == 0 {
		t.Fatal("probe was never executed")
	}
}

// TestUnhealthyProbeKillsAndRestarts: a replica that fails its probe
// FailureThreshold times in a row is killed and restarted (under backoff).
func TestUnhealthyProbeKillsAndRestarts(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	r, clk := clocked(f)
	s := liveSpec([]string{"check"}, 3, 0)

	r.Reconcile(ctx, s) // create web-0
	id := f.idOf("web", 0)
	f.exitFor[id] = 1 // probe now fails

	// First two due probes: below threshold -> "unhealthy", no kill.
	for want := 1; want <= 2; want++ {
		*clk = clk.Add(11 * time.Second)
		rep, _ := r.Reconcile(ctx, s)
		a := actionOf(rep, "web", 0)
		if a.Verb != "unhealthy" || a.Failures != want {
			t.Fatalf("probe %d: action=%v want unhealthy/%d", want, a, want)
		}
		if !f.byID[id].Running() {
			t.Fatalf("probe %d should not have killed the container yet", want)
		}
	}

	// Third failure hits the threshold: kill + restart in the same pass.
	*clk = clk.Add(11 * time.Second)
	rep, _ := r.Reconcile(ctx, s)
	if killed := actionVerbCount(rep, "killed"); killed != 1 {
		t.Fatalf("want 1 kill at threshold, got %d (%v)", killed, rep.Actions)
	}
	if restarts := actionVerbCount(rep, "restart"); restarts != 1 {
		t.Fatalf("want 1 restart after kill, got %d (%v)", restarts, rep.Actions)
	}
	if f.running("web") != 1 {
		t.Fatal("replica should be running again after restart")
	}
}

// TestProbeRecoversResetsFailures: an intermittent failure that recovers
// before the threshold does not accumulate toward a kill.
func TestProbeRecoversResetsFailures(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	r, clk := clocked(f)
	s := liveSpec([]string{"check"}, 3, 0)

	r.Reconcile(ctx, s)
	id := f.idOf("web", 0)

	f.exitFor[id] = 1 // fail twice
	for i := 0; i < 2; i++ {
		*clk = clk.Add(11 * time.Second)
		r.Reconcile(ctx, s)
	}
	f.exitFor[id] = 0 // recover
	*clk = clk.Add(11 * time.Second)
	r.Reconcile(ctx, s)

	// Fail twice more — streak restarted from zero, so still no kill.
	f.exitFor[id] = 1
	for i := 0; i < 2; i++ {
		*clk = clk.Add(11 * time.Second)
		rep, _ := r.Reconcile(ctx, s)
		if actionVerbCount(rep, "killed") != 0 {
			t.Fatalf("recovery should have reset the streak; got a kill: %v", rep.Actions)
		}
	}
	if f.running("web") != 1 {
		t.Fatal("replica should still be running")
	}
}

// TestInitialDelaySuppressesProbe: no probe runs until the initial delay has
// elapsed (gives slow-starting apps time to come up).
func TestInitialDelaySuppressesProbe(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	r, clk := clocked(f)
	s := liveSpec([]string{"check"}, 1, 30) // 30s initial delay, fail-fast threshold

	r.Reconcile(ctx, s)
	id := f.idOf("web", 0)
	f.exitFor[id] = 1 // would fail immediately if probed

	// This pass first sees the replica running, anchoring the initial delay.
	rep, _ := r.Reconcile(ctx, s)
	if f.execs != 0 || rep.Changed() {
		t.Fatalf("probe must not run at first sighting (execs=%d, actions=%v)", f.execs, rep.Actions)
	}

	*clk = clk.Add(20 * time.Second) // still within the 30s delay
	if _, _ = r.Reconcile(ctx, s); f.execs != 0 {
		t.Fatalf("probe ran during initial delay (execs=%d)", f.execs)
	}

	*clk = clk.Add(15 * time.Second) // now 35s past first sighting
	r.Reconcile(ctx, s)
	if f.execs == 0 {
		t.Fatal("probe should run after the initial delay")
	}
}

func actionVerbCount(r Report, verb string) int {
	n := 0
	for _, a := range r.Actions {
		if a.Verb == verb {
			n++
		}
	}
	return n
}
