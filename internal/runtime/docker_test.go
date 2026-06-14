package runtime

import "testing"

// Unit tests only. The Docker-hitting integration tests for this client live
// in ./test/integration (build tag `integration`), so `go test ./...` stays
// hermetic and needs no daemon.

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
