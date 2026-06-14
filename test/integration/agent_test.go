//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/goncalofrazao/nimbus/internal/agent"
	"github.com/goncalofrazao/nimbus/internal/runtime"
	"github.com/goncalofrazao/nimbus/internal/spec"
)

// TestReconcileConvergesAndSelfHeals: the reconcile loop brings a workload to
// its desired replica count against real Docker, and recreates a replica
// deleted behind its back.
func TestReconcileConvergesAndSelfHeals(t *testing.T) {
	c := newClient(t)
	const wl = "itest-heal"
	cleanup(t, c, wl)
	defer cleanup(t, c, wl)
	ctx := context.Background()

	rec := agent.New(c)
	s := &spec.Spec{Workloads: []spec.Workload{{
		Name: wl, Image: testImage, Replicas: 2, Cmd: []string{"sleep", "300"},
	}}}

	if _, err := rec.Reconcile(ctx, s); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	eventually(t, 15*time.Second, "2 replicas running", func() bool {
		return runningCount(t, c, wl) == 2
	})

	// Kill one replica behind the agent's back.
	cs, err := c.List(ctx, map[string]string{runtime.LabelWorkload: wl})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Remove(ctx, cs[0].ID, true); err != nil {
		t.Fatalf("remove: %v", err)
	}
	eventually(t, 5*time.Second, "down to 1 after external delete", func() bool {
		return runningCount(t, c, wl) == 1
	})

	// Next reconcile must heal it back to 2.
	if _, err := rec.Reconcile(ctx, s); err != nil {
		t.Fatalf("reconcile (heal): %v", err)
	}
	eventually(t, 15*time.Second, "healed back to 2", func() bool {
		return runningCount(t, c, wl) == 2
	})
}

// TestReconcileScaleDown: lowering the replica count reclaims the excess.
func TestReconcileScaleDown(t *testing.T) {
	c := newClient(t)
	const wl = "itest-scale"
	cleanup(t, c, wl)
	defer cleanup(t, c, wl)
	ctx := context.Background()

	rec := agent.New(c)
	mk := func(n int) *spec.Spec {
		return &spec.Spec{Workloads: []spec.Workload{{
			Name: wl, Image: testImage, Replicas: n, Cmd: []string{"sleep", "300"},
		}}}
	}

	if _, err := rec.Reconcile(ctx, mk(3)); err != nil {
		t.Fatal(err)
	}
	eventually(t, 15*time.Second, "3 running", func() bool {
		return runningCount(t, c, wl) == 3
	})

	if _, err := rec.Reconcile(ctx, mk(1)); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "scaled down to 1", func() bool {
		return runningCount(t, c, wl) == 1
	})
}

// TestLivenessKillsAndRestarts: a replica that is up but fails its liveness
// probe is killed and restarted. With threshold 1, the first due probe (the
// reconcile after the container is seen running) triggers it.
func TestLivenessKillsAndRestarts(t *testing.T) {
	c := newClient(t)
	const wl = "itest-live"
	cleanup(t, c, wl)
	defer cleanup(t, c, wl)
	ctx := context.Background()

	rec := agent.New(c)
	s := &spec.Spec{Workloads: []spec.Workload{{
		Name: wl, Image: testImage, Replicas: 1, Cmd: []string{"sleep", "300"},
		Liveness: &spec.Probe{Exec: []string{"false"}, PeriodSeconds: 1, FailureThreshold: 1},
	}}}

	if _, err := rec.Reconcile(ctx, s); err != nil { // creates the replica
		t.Fatal(err)
	}
	eventually(t, 15*time.Second, "replica running", func() bool {
		return runningCount(t, c, wl) == 1
	})

	// The next reconcile sees it running and runs the (failing) probe, which
	// at threshold 1 kills and restarts it in the same pass.
	rep, err := rec.Reconcile(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	killed, restarted := false, false
	for _, a := range rep.Actions {
		switch a.Verb {
		case "killed":
			killed = true
		case "restart":
			restarted = true
		}
	}
	if !killed || !restarted {
		t.Fatalf("liveness should kill+restart; actions=%v", rep.Actions)
	}
	eventually(t, 15*time.Second, "running again after restart", func() bool {
		return runningCount(t, c, wl) == 1
	})
}
