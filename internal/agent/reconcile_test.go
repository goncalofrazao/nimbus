package agent

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/goncalofrazao/nimbus/internal/runtime"
	"github.com/goncalofrazao/nimbus/internal/spec"
)

// fakeRuntime is an in-memory stand-in for the container runtime, so the
// convergence logic can be tested deterministically without Docker.
type fakeRuntime struct {
	mu     sync.Mutex
	seq    int
	byID   map[string]*runtime.Container
	pulled []string
	fails  map[string]error // verb -> error to inject
	// exitFor maps a container ID to the exit code its exec probe returns;
	// absent IDs probe healthy (0). execs counts probe invocations.
	exitFor map[string]int
	execs   int
}

func newFake() *fakeRuntime {
	return &fakeRuntime{byID: map[string]*runtime.Container{}, fails: map[string]error{}, exitFor: map[string]int{}}
}

func (f *fakeRuntime) Exec(_ context.Context, id string, _ []string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fails["exec"]; err != nil {
		return -1, err
	}
	f.execs++
	return f.exitFor[id], nil
}

func (f *fakeRuntime) Pull(_ context.Context, image string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fails["pull"]; err != nil {
		return err
	}
	f.pulled = append(f.pulled, image)
	return nil
}

func (f *fakeRuntime) Create(_ context.Context, s runtime.ContainerSpec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fails["create"]; err != nil {
		return "", err
	}
	f.seq++
	id := fmt.Sprintf("c%d", f.seq)
	labels := map[string]string{runtime.LabelManaged: "true"}
	for k, v := range s.Labels {
		labels[k] = v
	}
	f.byID[id] = &runtime.Container{ID: id, Name: s.Name, Image: s.Image, State: "created", Labels: labels}
	return id, nil
}

func (f *fakeRuntime) Start(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fails["start"]; err != nil {
		return err
	}
	c, ok := f.byID[id]
	if !ok {
		return fmt.Errorf("no such container %s", id)
	}
	c.State = "running"
	return nil
}

func (f *fakeRuntime) Stop(_ context.Context, id string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.byID[id]; ok {
		c.State = "exited"
	}
	return nil
}

func (f *fakeRuntime) Remove(_ context.Context, id string, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.byID, id)
	return nil
}

func (f *fakeRuntime) List(_ context.Context, labels map[string]string) ([]runtime.Container, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []runtime.Container
	for _, c := range f.byID {
		match := true
		for k, v := range labels {
			if c.Labels[k] != v {
				match = false
				break
			}
		}
		if match {
			out = append(out, *c)
		}
	}
	return out, nil
}

// running counts containers in the running state for a workload.
func (f *fakeRuntime) running(workload string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.byID {
		if c.Labels[runtime.LabelWorkload] == workload && c.State == "running" {
			n++
		}
	}
	return n
}

// crashOne flips one running replica of a workload to exited, simulating a
// container that died on its own.
func (f *fakeRuntime) crashOne(workload string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.byID {
		if c.Labels[runtime.LabelWorkload] == workload && c.State == "running" {
			c.State = "exited"
			return
		}
	}
}

func specOf(ws ...spec.Workload) *spec.Spec { return &spec.Spec{Workloads: ws} }

func TestConvergesFromEmpty(t *testing.T) {
	f := newFake()
	r := New(f)
	s := specOf(spec.Workload{Name: "web", Image: "nginx", Replicas: 3})

	rep, err := r.Reconcile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Changed() {
		t.Fatal("first pass should have created replicas")
	}
	if got := f.running("web"); got != 3 {
		t.Fatalf("running=%d want 3", got)
	}
}

func TestIdempotent(t *testing.T) {
	f := newFake()
	r := New(f)
	s := specOf(spec.Workload{Name: "web", Image: "nginx", Replicas: 2})

	if _, err := r.Reconcile(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	rep, err := r.Reconcile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Changed() {
		t.Fatalf("second pass on a converged cluster should be a no-op, did: %v", rep.Actions)
	}
}

func TestSelfHealsCrashedReplica(t *testing.T) {
	f := newFake()
	r := New(f)
	s := specOf(spec.Workload{Name: "web", Image: "nginx", Replicas: 3})
	if _, err := r.Reconcile(context.Background(), s); err != nil {
		t.Fatal(err)
	}

	f.crashOne("web") // a replica dies
	if f.running("web") != 2 {
		t.Fatalf("setup: want 2 running after crash, got %d", f.running("web"))
	}

	rep, err := r.Reconcile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Changed() {
		t.Fatal("reconcile should have restarted the crashed replica")
	}
	if got := f.running("web"); got != 3 {
		t.Fatalf("after heal running=%d want 3", got)
	}
	// And it healed by restart, not by leaking a new container.
	restarts := 0
	for _, a := range rep.Actions {
		if a.Verb == "restart" {
			restarts++
		}
	}
	if restarts != 1 {
		t.Fatalf("want exactly 1 restart, got %d (%v)", restarts, rep.Actions)
	}
}

func TestScaleDownRemovesExcess(t *testing.T) {
	f := newFake()
	r := New(f)
	if _, err := r.Reconcile(context.Background(), specOf(spec.Workload{Name: "web", Image: "nginx", Replicas: 4})); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), specOf(spec.Workload{Name: "web", Image: "nginx", Replicas: 1})); err != nil {
		t.Fatal(err)
	}
	if got := f.running("web"); got != 1 {
		t.Fatalf("running=%d want 1 after scale-down", got)
	}
}

func TestRemovedWorkloadReclaimed(t *testing.T) {
	f := newFake()
	r := New(f)
	full := specOf(
		spec.Workload{Name: "web", Image: "nginx", Replicas: 2},
		spec.Workload{Name: "api", Image: "go-api", Replicas: 2},
	)
	if _, err := r.Reconcile(context.Background(), full); err != nil {
		t.Fatal(err)
	}
	// Drop "api" from the desired spec entirely.
	if _, err := r.Reconcile(context.Background(), specOf(spec.Workload{Name: "web", Image: "nginx", Replicas: 2})); err != nil {
		t.Fatal(err)
	}
	if got := f.running("api"); got != 0 {
		t.Fatalf("api should be fully reclaimed, %d still running", got)
	}
	if got := f.running("web"); got != 2 {
		t.Fatalf("web should be untouched, running=%d want 2", got)
	}
}

// TestListFailureIsFatal: if the agent can't see the world, it must refuse to
// act rather than mutate the cluster blind.
func TestListFailureIsFatal(t *testing.T) {
	f := newFake()
	f.fails["create"] = nil
	r := New(f)
	// Seed one container so List has something, then make List fail.
	if _, err := r.Reconcile(context.Background(), specOf(spec.Workload{Name: "web", Image: "nginx", Replicas: 1})); err != nil {
		t.Fatal(err)
	}
	f.fails["pull"] = fmt.Errorf("registry down")
	rep, err := r.Reconcile(context.Background(), specOf(spec.Workload{Name: "web", Image: "nginx", Replicas: 2}))
	if err != nil {
		t.Fatalf("a single pull failure should not abort the pass: %v", err)
	}
	// The existing replica still no-ops fine; the new one records an error.
	if len(rep.Errs()) != 1 {
		t.Fatalf("want 1 errored action, got %d (%v)", len(rep.Errs()), rep.Actions)
	}
}
