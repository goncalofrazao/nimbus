package agent

import (
	"context"
	"testing"
	"time"

	"github.com/goncalofrazao/nimbus/internal/spec"
)

// TestParallelTeardownBeatsSerial: with a per-op latency injected into the
// runtime, tearing down many replicas concurrently must be dramatically faster
// than doing it one at a time — this is the whole point of concurrent reconcile.
func TestParallelTeardownBeatsSerial(t *testing.T) {
	const replicas = 16
	const opDelay = 20 * time.Millisecond
	ctx := context.Background()

	// Each removal does Stop+Remove = 2 ops. Serial lower bound ≈
	// replicas*2*opDelay = 640ms; with parallelism 8 it should be ~4*opDelay.
	teardown := func(parallelism int) time.Duration {
		f := newFake()
		r := New(f)
		r.SetParallelism(parallelism)
		if _, err := r.Reconcile(ctx, specOf(spec.Workload{Name: "web", Image: "x", Replicas: replicas})); err != nil {
			t.Fatal(err)
		}
		if f.running("web") != replicas {
			t.Fatalf("setup: %d running, want %d", f.running("web"), replicas)
		}
		f.opDelay = opDelay // only time the teardown
		start := time.Now()
		if _, err := r.Reconcile(ctx, &spec.Spec{}); err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(start)
		if f.running("web") != 0 {
			t.Fatalf("teardown incomplete: %d still running", f.running("web"))
		}
		return elapsed
	}

	par := teardown(8)
	serial := teardown(1)
	t.Logf("teardown of %d replicas: parallel(8)=%s serial(1)=%s", replicas, par, serial)

	// Parallel must be well under serial. Generous bound to stay CI-stable.
	if par > serial/2 {
		t.Errorf("parallel teardown not meaningfully faster: parallel=%s serial=%s", par, serial)
	}
}

// TestConcurrentConvergeCorrectAtScale: converging and then scaling a larger
// fleet must land on the exact desired counts despite concurrency. Run with
// -race to catch shared-state races in the reconcile phases.
func TestConcurrentConvergeCorrectAtScale(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	r := New(f) // default parallelism

	if _, err := r.Reconcile(ctx, specOf(
		spec.Workload{Name: "web", Image: "x", Replicas: 12},
		spec.Workload{Name: "api", Image: "y", Replicas: 9},
	)); err != nil {
		t.Fatal(err)
	}
	if f.running("web") != 12 || f.running("api") != 9 {
		t.Fatalf("converge: web=%d api=%d want 12/9", f.running("web"), f.running("api"))
	}

	// Scale web down, api up, in one pass.
	if _, err := r.Reconcile(ctx, specOf(
		spec.Workload{Name: "web", Image: "x", Replicas: 3},
		spec.Workload{Name: "api", Image: "y", Replicas: 15},
	)); err != nil {
		t.Fatal(err)
	}
	if f.running("web") != 3 || f.running("api") != 15 {
		t.Fatalf("rescale: web=%d api=%d want 3/15", f.running("web"), f.running("api"))
	}
}
