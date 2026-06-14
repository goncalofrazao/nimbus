package runtime

import (
	"context"
	"os"
	"testing"
	"time"
)

// dial returns a Client if a Docker daemon is reachable, else skips the test.
// These are real integration tests: they start and stop actual containers.
func dial(t *testing.T) *Client {
	t.Helper()
	socket := DefaultSocket
	if s := os.Getenv("DOCKER_SOCKET"); s != "" {
		socket = s
	}
	c := NewWithSocket(socket)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Ping(ctx); err != nil {
		t.Skipf("no reachable docker daemon at %s: %v", socket, err)
	}
	return c
}

// TestLifecycle drives a container through its whole life: pull, create,
// start, observe it running, stop, observe it exited, remove.
func TestLifecycle(t *testing.T) {
	c := dial(t)
	ctx := context.Background()

	const image = "busybox:latest"
	if err := c.Pull(ctx, image); err != nil {
		t.Fatalf("pull: %v", err)
	}

	name := "nimbus-test-" + time.Now().Format("150405.000000")
	id, err := c.Create(ctx, ContainerSpec{
		Name:   name,
		Image:  image,
		Cmd:    []string{"sleep", "60"},
		Labels: map[string]string{LabelWorkload: "lifecycle", LabelReplica: "0"},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Always clean up, even on later failure.
	defer func() { _ = c.Remove(context.Background(), id, true) }()

	if err := c.Start(ctx, id); err != nil {
		t.Fatalf("start: %v", err)
	}
	// Start must be idempotent.
	if err := c.Start(ctx, id); err != nil {
		t.Fatalf("second start should be a no-op: %v", err)
	}

	got, err := c.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if !got.Running() {
		t.Fatalf("container state = %q, want running", got.State)
	}
	if got.Workload() != "lifecycle" {
		t.Fatalf("workload label = %q, want lifecycle", got.Workload())
	}

	// It must show up in a label-filtered list.
	list, err := c.List(ctx, map[string]string{LabelWorkload: "lifecycle"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	found := false
	for _, ct := range list {
		if ct.ID == id {
			found = true
		}
	}
	if !found {
		t.Fatalf("container %s not in filtered list", short(id))
	}

	if err := c.Stop(ctx, id, 2*time.Second); err != nil {
		t.Fatalf("stop: %v", err)
	}
	got, err = c.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("inspect after stop: %v", err)
	}
	if got.Running() {
		t.Fatalf("container still running after stop")
	}

	if err := c.Remove(ctx, id, true); err != nil {
		t.Fatalf("remove: %v", err)
	}
	// Remove must be idempotent (gone == success).
	if err := c.Remove(ctx, id, true); err != nil {
		t.Fatalf("second remove should be a no-op: %v", err)
	}
}

func TestSplitImage(t *testing.T) {
	cases := map[string][2]string{
		"busybox":              {"busybox", "latest"},
		"busybox:1.36":         {"busybox", "1.36"},
		"library/nginx:alpine": {"library/nginx", "alpine"},
		"registry:5000/app":    {"registry:5000/app", "latest"},
	}
	for in, want := range cases {
		repo, tag := splitImage(in)
		if repo != want[0] || tag != want[1] {
			t.Errorf("splitImage(%q) = (%q,%q), want (%q,%q)", in, repo, tag, want[0], want[1])
		}
	}
}
