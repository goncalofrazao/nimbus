package store

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/goncalofrazao/nimbus/internal/spec"
)

func tmpStore(t *testing.T) (string, *Store) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return path, s
}

func wl(name string, replicas int) spec.Workload {
	return spec.Workload{Name: name, Image: "busybox", Replicas: replicas}
}

func TestOpenEmpty(t *testing.T) {
	_, s := tmpStore(t)
	if g := s.Generation(); g != 0 {
		t.Fatalf("fresh store generation=%d want 0", g)
	}
	if len(s.Spec().Workloads) != 0 {
		t.Fatal("fresh store should be empty")
	}
}

// TestDurableAcrossReopen is the core property: a committed apply survives a
// process restart (a fresh Store on the same path recovers it).
func TestDurableAcrossReopen(t *testing.T) {
	path, s := tmpStore(t)
	if _, err := s.Apply(wl("web", 3)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(wl("api", 1)); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if g := reopened.Generation(); g != 2 {
		t.Fatalf("recovered generation=%d want 2", g)
	}
	got, ok := reopened.Get("web")
	if !ok || got.Replicas != 3 {
		t.Fatalf("web not recovered: %+v ok=%v", got, ok)
	}
}

func TestApplyIdempotentAndUpdate(t *testing.T) {
	_, s := tmpStore(t)
	changed, _ := s.Apply(wl("web", 2))
	if !changed || s.Generation() != 1 {
		t.Fatalf("first apply: changed=%v gen=%d", changed, s.Generation())
	}
	// Identical apply: durable no-op.
	changed, _ = s.Apply(wl("web", 2))
	if changed || s.Generation() != 1 {
		t.Fatalf("identical apply should be a no-op: changed=%v gen=%d", changed, s.Generation())
	}
	// Changed apply: commits.
	changed, _ = s.Apply(wl("web", 5))
	if !changed || s.Generation() != 2 {
		t.Fatalf("changed apply: changed=%v gen=%d", changed, s.Generation())
	}
	if got, _ := s.Get("web"); got.Replicas != 5 {
		t.Fatalf("replicas=%d want 5", got.Replicas)
	}
}

func TestDelete(t *testing.T) {
	_, s := tmpStore(t)
	s.Apply(wl("web", 1))
	existed, _ := s.Delete("web")
	if !existed || s.Generation() != 2 {
		t.Fatalf("delete existing: existed=%v gen=%d", existed, s.Generation())
	}
	existed, _ = s.Delete("ghost")
	if existed || s.Generation() != 2 {
		t.Fatalf("delete missing should be a no-op: existed=%v gen=%d", existed, s.Generation())
	}
}

func TestScale(t *testing.T) {
	_, s := tmpStore(t)
	s.Apply(wl("web", 1))
	if err := s.Scale("web", 4); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Get("web"); got.Replicas != 4 {
		t.Fatalf("replicas=%d want 4", got.Replicas)
	}
	if err := s.Scale("ghost", 1); err == nil {
		t.Fatal("scaling a missing workload should error")
	}
}

// TestApplyAllAtomic: a batch containing an invalid workload commits nothing.
func TestApplyAllAtomic(t *testing.T) {
	path, s := tmpStore(t)
	s.Apply(wl("web", 1))
	genBefore := s.Generation()

	_, err := s.ApplyAll([]spec.Workload{wl("api", 2), {Name: "BAD NAME", Image: "x", Replicas: 1}})
	if err == nil {
		t.Fatal("ApplyAll with an invalid workload should fail")
	}
	if s.Generation() != genBefore {
		t.Fatalf("failed batch must not bump generation: %d", s.Generation())
	}
	if _, ok := s.Get("api"); ok {
		t.Fatal("failed batch must not partially apply")
	}
	// The on-disk file must be unchanged too.
	reopened, _ := Open(path)
	if _, ok := reopened.Get("api"); ok {
		t.Fatal("failed batch leaked to disk")
	}
}

func TestValidationRejectsBadWorkload(t *testing.T) {
	_, s := tmpStore(t)
	if _, err := s.Apply(spec.Workload{Name: "x", Replicas: 1}); err == nil {
		t.Fatal("workload with empty image should be rejected")
	}
	if s.Generation() != 0 {
		t.Fatal("rejected apply must not commit")
	}
}

func TestRejectsCorruptFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("opening a corrupt store should error, not silently reset")
	}
}

// TestReloadPicksUpExternalWrite simulates the running daemon (handle b)
// noticing a change made by a separate `apply` process (handle a).
func TestReloadPicksUpExternalWrite(t *testing.T) {
	path, a := tmpStore(t)
	b, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Apply(wl("web", 2)); err != nil {
		t.Fatal(err)
	}
	changed, err := b.Reload()
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("reload should report the external change")
	}
	if got, ok := b.Get("web"); !ok || got.Replicas != 2 {
		t.Fatalf("reloaded handle missing the change: %+v ok=%v", got, ok)
	}
}

// TestNoTempLeftover: atomic writes must not leave temp files behind.
func TestNoTempLeftover(t *testing.T) {
	path, s := tmpStore(t)
	for i := 0; i < 5; i++ {
		s.Apply(wl("web", i))
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("leftover temp file: %s", e.Name())
		}
	}
}

// TestConcurrentApply: concurrent mutations are serialized and all land.
// Run with -race.
func TestConcurrentApply(t *testing.T) {
	path, s := tmpStore(t)
	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := s.Apply(wl("w"+string(rune('a'+i)), i)); err != nil {
				t.Errorf("apply %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	if got := len(s.Spec().Workloads); got != n {
		t.Fatalf("got %d workloads want %d", got, n)
	}
	if g := s.Generation(); g != n {
		t.Fatalf("generation=%d want %d", g, n)
	}
	// And it's all durable.
	reopened, _ := Open(path)
	if got := len(reopened.Spec().Workloads); got != n {
		t.Fatalf("durable count=%d want %d", got, n)
	}
}
