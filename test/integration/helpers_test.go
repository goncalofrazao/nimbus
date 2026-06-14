//go:build integration

// Package integration holds Nimbus's end-to-end tests: they drive the real
// runtime client and reconcile loop against a live Docker daemon, starting and
// stopping actual containers.
//
// They are isolated behind the `integration` build tag so a plain
// `go test ./...` stays hermetic and needs no daemon. Run them with:
//
//	go test -tags=integration ./test/integration/...
//
// or `make integration`. CI runs them on a runner that has Docker. Every test
// scopes its containers to an `itest-*` workload name and cleans up before and
// after, so a stray run can't leak or clobber unrelated containers.
package integration

import (
	"context"
	"testing"
	"time"

	"github.com/goncalofrazao/nimbus/internal/runtime"
)

// testImage is intentionally tiny to keep pulls fast and rate-limit-friendly.
const testImage = "busybox:latest"

// newClient returns a runtime client, skipping the test if no daemon is
// reachable (so the suite degrades gracefully off-CI).
func newClient(t *testing.T) *runtime.Client {
	t.Helper()
	c := runtime.New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Ping(ctx); err != nil {
		t.Skipf("no reachable docker daemon: %v", err)
	}
	return c
}

// cleanup force-removes every container belonging to a test workload.
func cleanup(t *testing.T, c *runtime.Client, workload string) {
	t.Helper()
	ctx := context.Background()
	cs, err := c.List(ctx, map[string]string{runtime.LabelWorkload: workload})
	if err != nil {
		return
	}
	for _, ct := range cs {
		_ = c.Remove(ctx, ct.ID, true)
	}
}

// runningCount returns how many of a workload's containers are running.
func runningCount(t *testing.T, c *runtime.Client, workload string) int {
	t.Helper()
	cs, err := c.List(context.Background(), map[string]string{runtime.LabelWorkload: workload})
	if err != nil {
		t.Fatalf("list %s: %v", workload, err)
	}
	n := 0
	for _, ct := range cs {
		if ct.Running() {
			n++
		}
	}
	return n
}

// eventually polls cond until it holds or the timeout elapses.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}
