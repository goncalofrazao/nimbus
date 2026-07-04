package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/goncalofrazao/nimbus/internal/spec"
	"github.com/goncalofrazao/nimbus/internal/store"
)

// newTestServer stands up a control plane over a temp-dir store and returns a
// client pointed at it, plus the store path so durability can be checked.
func newTestServer(t *testing.T) (*Client, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(NewServer(st, nil))
	t.Cleanup(ts.Close)
	return NewClient(ts.URL), path
}

func wl(name string, replicas int) spec.Workload {
	return spec.Workload{Name: name, Image: "busybox", Replicas: replicas}
}

func TestHealthz(t *testing.T) {
	cl, _ := newTestServer(t)
	if err := cl.Health(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestApplyAndGetRoundtrip(t *testing.T) {
	cl, _ := newTestServer(t)
	ctx := context.Background()

	res, err := cl.Apply(ctx, []spec.Workload{wl("web", 2), wl("api", 1)})
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied != 2 || res.Generation != 1 {
		t.Fatalf("apply: %+v want applied=2 gen=1", res)
	}

	st, err := cl.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Generation != 1 || len(st.Workloads) != 2 {
		t.Fatalf("state: gen=%d workloads=%d want 1/2", st.Generation, len(st.Workloads))
	}
	// The store keeps workloads sorted by name.
	if st.Workloads[0].Name != "api" || st.Workloads[1].Name != "web" {
		t.Fatalf("workloads out of order: %v", st.Workloads)
	}

	w, err := cl.Get(ctx, "web")
	if err != nil {
		t.Fatal(err)
	}
	if w.Replicas != 2 || w.Image != "busybox" {
		t.Fatalf("get web: %+v", w)
	}

	// An identical apply is a durable no-op: nothing applied, generation held.
	res, err = cl.Apply(ctx, []spec.Workload{wl("web", 2)})
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied != 0 || res.Generation != 1 {
		t.Fatalf("identical apply: %+v want applied=0 gen=1", res)
	}
}

func TestGetMissingWorkload(t *testing.T) {
	cl, _ := newTestServer(t)
	_, err := cl.Get(context.Background(), "ghost")
	if err == nil || !strings.Contains(err.Error(), `no such workload "ghost"`) {
		t.Fatalf("want not-found error, got %v", err)
	}
}

func TestApplyValidationRejected(t *testing.T) {
	cl, _ := newTestServer(t)
	ctx := context.Background()

	// One bad workload poisons the whole batch — nothing may commit.
	_, err := cl.Apply(ctx, []spec.Workload{wl("web", 1), {Name: "bad", Replicas: 1}})
	if err == nil || !strings.Contains(err.Error(), "empty image") {
		t.Fatalf("want validation error, got %v", err)
	}
	st, err := cl.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Generation != 0 || len(st.Workloads) != 0 {
		t.Fatalf("failed apply must not commit: gen=%d workloads=%d", st.Generation, len(st.Workloads))
	}

	if _, err := cl.Apply(ctx, nil); err == nil {
		t.Fatal("empty apply should be rejected")
	}
}

func TestScale(t *testing.T) {
	cl, _ := newTestServer(t)
	ctx := context.Background()
	if _, err := cl.Apply(ctx, []spec.Workload{wl("web", 1)}); err != nil {
		t.Fatal(err)
	}

	res, err := cl.Scale(ctx, "web", 5)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Existed || res.Generation != 2 {
		t.Fatalf("scale: %+v want existed gen=2", res)
	}
	w, err := cl.Get(ctx, "web")
	if err != nil {
		t.Fatal(err)
	}
	if w.Replicas != 5 {
		t.Fatalf("replicas=%d want 5", w.Replicas)
	}

	if _, err := cl.Scale(ctx, "ghost", 1); err == nil ||
		!strings.Contains(err.Error(), `no such workload "ghost"`) {
		t.Fatalf("want not-found error, got %v", err)
	}
	if _, err := cl.Scale(ctx, "web", -1); err == nil {
		t.Fatal("negative replicas should be rejected")
	}
}

func TestDeleteIsIdempotent(t *testing.T) {
	cl, _ := newTestServer(t)
	ctx := context.Background()
	if _, err := cl.Apply(ctx, []spec.Workload{wl("web", 1)}); err != nil {
		t.Fatal(err)
	}

	res, err := cl.Delete(ctx, "web")
	if err != nil {
		t.Fatal(err)
	}
	if !res.Existed || res.Generation != 2 {
		t.Fatalf("delete: %+v want existed gen=2", res)
	}

	res, err = cl.Delete(ctx, "web")
	if err != nil {
		t.Fatal(err)
	}
	if res.Existed || res.Generation != 2 {
		t.Fatalf("second delete: %+v want existed=false gen=2", res)
	}
}

// TestMutationsAreDurable: what the API commits must survive the control
// plane — reopening the store file sees exactly what was applied.
func TestMutationsAreDurable(t *testing.T) {
	cl, path := newTestServer(t)
	ctx := context.Background()
	if _, err := cl.Apply(ctx, []spec.Workload{wl("web", 3)}); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Scale(ctx, "web", 7); err != nil {
		t.Fatal(err)
	}

	reopened, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if g := reopened.Generation(); g != 2 {
		t.Fatalf("reopened generation=%d want 2", g)
	}
	w, ok := reopened.Get("web")
	if !ok || w.Replicas != 7 {
		t.Fatalf("reopened web: ok=%v %+v", ok, w)
	}
}

// Malformed requests are exercised over raw HTTP, below the Client.
func TestBadRequests(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(NewServer(st, nil))
	t.Cleanup(ts.Close)

	post := func(url, body string) int {
		t.Helper()
		resp, err := http.Post(url, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	if code := post(ts.URL+"/v1/workloads", "{not json"); code != http.StatusBadRequest {
		t.Fatalf("garbage body: %d want 400", code)
	}
	if code := post(ts.URL+"/v1/workloads", `{"workloadz":[]}`); code != http.StatusBadRequest {
		t.Fatalf("unknown field: %d want 400", code)
	}
	if code := post(ts.URL+"/v1/workloads", `{"workloads":[]}{}`); code != http.StatusBadRequest {
		t.Fatalf("trailing data: %d want 400", code)
	}

	// Wrong method on a known path.
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/v1/workloads", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("PUT: %d want 405", resp.StatusCode)
	}
}
