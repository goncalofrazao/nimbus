//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/goncalofrazao/nimbus/internal/runtime"
)

// TestRuntimeLifecycle drives a container through its whole life against the
// real daemon: pull, create, start, observe running, stop, observe exited,
// remove (twice — removal is idempotent).
func TestRuntimeLifecycle(t *testing.T) {
	c := newClient(t)
	const wl = "itest-lifecycle"
	cleanup(t, c, wl)
	defer cleanup(t, c, wl)
	ctx := context.Background()

	if err := c.Pull(ctx, testImage); err != nil {
		t.Fatalf("pull: %v", err)
	}
	id, err := c.Create(ctx, runtime.ContainerSpec{
		Name:  wl + "-0",
		Image: testImage,
		Cmd:   []string{"sleep", "60"},
		Labels: map[string]string{
			runtime.LabelWorkload: wl,
			runtime.LabelReplica:  "0",
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := c.Start(ctx, id); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := c.Start(ctx, id); err != nil {
		t.Fatalf("start must be idempotent: %v", err)
	}

	got, err := c.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if !got.Running() {
		t.Fatalf("state = %q, want running", got.State)
	}
	if got.Workload() != wl {
		t.Fatalf("workload label = %q, want %q", got.Workload(), wl)
	}

	if err := c.Stop(ctx, id, 2*time.Second); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if got, _ = c.Inspect(ctx, id); got.Running() {
		t.Fatal("still running after stop")
	}

	if err := c.Remove(ctx, id, true); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if err := c.Remove(ctx, id, true); err != nil {
		t.Fatalf("remove must be idempotent: %v", err)
	}
}

// TestRuntimeExec verifies the exec API (the basis for liveness probes) returns
// a command's exit code faithfully.
func TestRuntimeExec(t *testing.T) {
	c := newClient(t)
	const wl = "itest-exec"
	cleanup(t, c, wl)
	defer cleanup(t, c, wl)
	ctx := context.Background()

	if err := c.Pull(ctx, testImage); err != nil {
		t.Fatalf("pull: %v", err)
	}
	id, err := c.Create(ctx, runtime.ContainerSpec{
		Name:   wl + "-0",
		Image:  testImage,
		Cmd:    []string{"sleep", "60"},
		Labels: map[string]string{runtime.LabelWorkload: wl, runtime.LabelReplica: "0"},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := c.Start(ctx, id); err != nil {
		t.Fatalf("start: %v", err)
	}

	for _, tc := range []struct {
		cmd  []string
		want int
	}{
		{[]string{"true"}, 0},
		{[]string{"false"}, 1},
		{[]string{"sh", "-c", "exit 7"}, 7},
	} {
		got, err := c.Exec(ctx, id, tc.cmd)
		if err != nil {
			t.Fatalf("exec %v: %v", tc.cmd, err)
		}
		if got != tc.want {
			t.Errorf("exec %v = %d, want %d", tc.cmd, got, tc.want)
		}
	}
}
