// Package store is Nimbus's persistent desired-state store — the control
// plane's memory. It holds the declared set of workloads and survives daemon
// restarts, so "what the cluster should be running" outlives the process that
// declared it.
//
// Durability is hand-rolled and crash-safe: every mutation is written to a
// temporary file, fsync'd, and atomically renamed over the real file (the
// directory is fsync'd too). A crash can therefore leave the store holding the
// last fully-committed state or the previous one — never a torn half-write.
//
// This is the seam the control-plane/agent split will form along: today the
// store is a shared file that the `run` loop reloads and the `apply`/`delete`
// commands mutate; tomorrow it sits behind a control-plane API, unchanged.
package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"

	"github.com/goncalofrazao/nimbus/internal/spec"
)

// State is the persisted desired state: every workload plus a monotonic
// generation that bumps on each committed change (useful for "did anything
// change since I last looked").
type State struct {
	Generation int64           `json:"generation"`
	Workloads  []spec.Workload `json:"workloads"`
}

// Store is a durable, concurrency-safe holder of desired state.
type Store struct {
	mu   sync.RWMutex
	path string
	st   State
}

// Open loads the store at path, or starts an empty one if the file does not
// yet exist. A corrupt or invalid file is a hard error — we will not silently
// reconcile against garbage.
func Open(path string) (*Store, error) {
	s := &Store{path: path}
	if _, err := s.Reload(); err != nil {
		return nil, err
	}
	return s, nil
}

// Reload re-reads the state from disk, replacing the in-memory copy. The
// running daemon calls this each reconcile so it picks up mutations made by
// separate `apply`/`delete` invocations. It reports whether the committed
// generation changed. A missing file means an empty cluster.
func (s *Store) Reload() (changed bool, err error) {
	b, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		s.mu.Lock()
		defer s.mu.Unlock()
		was := s.st.Generation
		s.st = State{}
		return was != 0, nil
	}
	if err != nil {
		return false, fmt.Errorf("read store %s: %w", s.path, err)
	}
	var loaded State
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&loaded); err != nil {
		return false, fmt.Errorf("parse store %s: %w", s.path, err)
	}
	if err := validateState(loaded); err != nil {
		return false, fmt.Errorf("invalid store %s: %w", s.path, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	changed = loaded.Generation != s.st.Generation
	s.st = loaded
	return changed, nil
}

// Spec returns a snapshot of the desired state as a *spec.Spec, safe to hand
// to the reconciler. The slice is copied so the caller can't mutate the store.
func (s *Store) Spec() *spec.Spec {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]spec.Workload, len(s.st.Workloads))
	copy(out, s.st.Workloads)
	return &spec.Spec{Workloads: out}
}

// State returns a consistent snapshot of the committed state — generation and
// workloads taken under one lock, so they always correspond.
func (s *Store) State() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]spec.Workload, len(s.st.Workloads))
	copy(out, s.st.Workloads)
	return State{Generation: s.st.Generation, Workloads: out}
}

// Generation returns the current committed generation.
func (s *Store) Generation() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.st.Generation
}

// Get returns a workload by name.
func (s *Store) Get(name string) (spec.Workload, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, w := range s.st.Workloads {
		if w.Name == name {
			return w, true
		}
	}
	return spec.Workload{}, false
}

// Apply upserts one workload (validated) and commits. It returns whether the
// stored state actually changed (an identical apply is a durable no-op).
func (s *Store) Apply(w spec.Workload) (changed bool, err error) {
	if err := w.Validate(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upsertLocked(w)
}

// ApplyAll upserts several workloads atomically: either every one validates
// and the batch commits once, or nothing changes. Returns the number of
// workloads that differed from what was stored.
func (s *Store) ApplyAll(ws []spec.Workload) (changed int, err error) {
	for i := range ws {
		if err := ws[i].Validate(); err != nil {
			return 0, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.st // copy header; Workloads slice shared until we rebuild
	next.Workloads = append([]spec.Workload(nil), s.st.Workloads...)
	for _, w := range ws {
		if upsertInto(&next.Workloads, w) {
			changed++
		}
	}
	if changed == 0 {
		return 0, nil
	}
	next.Generation = s.st.Generation + 1
	if err := s.commitLocked(next); err != nil {
		return 0, err
	}
	return changed, nil
}

// Delete removes a workload by name and commits. Reports whether it existed.
func (s *Store) Delete(name string) (existed bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := -1
	for i, w := range s.st.Workloads {
		if w.Name == name {
			idx = i
			break
		}
	}
	if idx < 0 {
		return false, nil
	}
	next := s.st
	next.Workloads = append(append([]spec.Workload(nil), s.st.Workloads[:idx]...), s.st.Workloads[idx+1:]...)
	next.Generation = s.st.Generation + 1
	if err := s.commitLocked(next); err != nil {
		return false, err
	}
	return true, nil
}

// Scale sets a workload's replica count and commits. It reports whether the
// workload existed — a missing workload is not an error, so callers (the CLI,
// the HTTP API) decide how to surface it.
func (s *Store) Scale(name string, replicas int) (existed bool, err error) {
	if replicas < 0 {
		return false, fmt.Errorf("negative replicas")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.getLocked(name)
	if !ok {
		return false, nil
	}
	if w.Replicas == replicas {
		return true, nil
	}
	w.Replicas = replicas
	_, err = s.upsertLocked(w)
	return true, err
}

// --- locked helpers -------------------------------------------------------

func (s *Store) getLocked(name string) (spec.Workload, bool) {
	for _, w := range s.st.Workloads {
		if w.Name == name {
			return w, true
		}
	}
	return spec.Workload{}, false
}

func (s *Store) upsertLocked(w spec.Workload) (bool, error) {
	next := s.st
	next.Workloads = append([]spec.Workload(nil), s.st.Workloads...)
	if !upsertInto(&next.Workloads, w) {
		return false, nil // identical; nothing to commit
	}
	next.Generation = s.st.Generation + 1
	if err := s.commitLocked(next); err != nil {
		return false, err
	}
	return true, nil
}

// upsertInto replaces or appends w in ws (kept sorted by name), returning
// whether the contents changed.
func upsertInto(ws *[]spec.Workload, w spec.Workload) bool {
	for i := range *ws {
		if (*ws)[i].Name == w.Name {
			if equalWorkload((*ws)[i], w) {
				return false
			}
			(*ws)[i] = w
			return true
		}
	}
	*ws = append(*ws, w)
	sort.Slice(*ws, func(i, j int) bool { return (*ws)[i].Name < (*ws)[j].Name })
	return true
}

// equalWorkload reports whether two workloads are byte-for-byte equivalent,
// so an identical apply commits nothing.
func equalWorkload(a, b spec.Workload) bool { return reflect.DeepEqual(a, b) }

// commitLocked persists next durably, then adopts it in memory. If the write
// fails the in-memory state is left untouched, so a failed commit is a no-op.
func (s *Store) commitLocked(next State) error {
	if err := writeAtomic(s.path, next); err != nil {
		return err
	}
	s.st = next
	return nil
}

// writeAtomic serializes st and replaces path crash-safely: write a temp file
// in the same directory, fsync it, rename it over path, then fsync the dir.
func writeAtomic(path string, st State) error {
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("encode store: %w", err)
	}
	b = append(b, '\n')

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("ensure store dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".nimbus-store-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op once the rename succeeds

	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil { // durable on disk before rename
		tmp.Close()
		return fmt.Errorf("fsync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil { // atomic replace
		return fmt.Errorf("rename into place: %w", err)
	}
	return fsyncDir(dir) // make the rename itself durable
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir for fsync: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsync dir: %w", err)
	}
	return nil
}

// validateState rejects a state whose workloads are individually invalid or
// have duplicate names.
func validateState(st State) error {
	seen := make(map[string]bool, len(st.Workloads))
	for i := range st.Workloads {
		w := &st.Workloads[i]
		if err := w.Validate(); err != nil {
			return err
		}
		if seen[w.Name] {
			return fmt.Errorf("duplicate workload name %q", w.Name)
		}
		seen[w.Name] = true
	}
	return nil
}
