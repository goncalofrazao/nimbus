// Package spec defines Nimbus's desired-state model: the declarative
// description of what should be running. It is deliberately small and
// serializable — the control plane stores it, the node agent reconciles
// reality toward it. The running daemon, never this struct, is the source of
// truth for what is *actually* running; this is only what the operator *wants*.
package spec

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// Workload is one declared service: an image to run at a target replica count.
type Workload struct {
	Name     string   `json:"name"`
	Image    string   `json:"image"`
	Replicas int      `json:"replicas"`
	Cmd      []string `json:"cmd,omitempty"`
	Env      []string `json:"env,omitempty"`
	// Liveness, if set, is run periodically inside each replica; a replica
	// that fails it FailureThreshold times in a row is killed and restarted.
	Liveness *Probe `json:"liveness,omitempty"`
}

// Probe is an exec health check: Cmd is run inside the container and the
// replica is considered healthy iff it exits 0. (Exec probes are namespace-
// local, so they work without container networking — unlike HTTP/TCP probes.)
type Probe struct {
	Exec             []string `json:"exec"`
	PeriodSeconds    int      `json:"periodSeconds,omitempty"`
	TimeoutSeconds   int      `json:"timeoutSeconds,omitempty"`
	FailureThreshold int      `json:"failureThreshold,omitempty"`
	InitialDelaySec  int      `json:"initialDelaySeconds,omitempty"`
}

// Probe defaults, applied where the spec leaves a field zero.
const (
	defaultPeriodSeconds    = 10
	defaultTimeoutSeconds   = 2
	defaultFailureThreshold = 3
)

// withDefaults returns a copy of the probe with zero fields filled in.
func (p Probe) withDefaults() Probe {
	if p.PeriodSeconds == 0 {
		p.PeriodSeconds = defaultPeriodSeconds
	}
	if p.TimeoutSeconds == 0 {
		p.TimeoutSeconds = defaultTimeoutSeconds
	}
	if p.FailureThreshold == 0 {
		p.FailureThreshold = defaultFailureThreshold
	}
	return p
}

// Period, Timeout, Threshold and InitialDelay return the effective values
// (defaults applied) as their natural types.
func (p Probe) Period() time.Duration {
	return time.Duration(p.withDefaults().PeriodSeconds) * time.Second
}
func (p Probe) Timeout() time.Duration {
	return time.Duration(p.withDefaults().TimeoutSeconds) * time.Second
}
func (p Probe) Threshold() int { return p.withDefaults().FailureThreshold }
func (p Probe) InitialDelay() time.Duration {
	return time.Duration(p.InitialDelaySec) * time.Second
}

// Spec is a full cluster declaration: a set of workloads keyed by name.
type Spec struct {
	Workloads []Workload `json:"workloads"`
}

// Validate checks one workload is internally consistent. Bad input should be
// rejected at the door, not midway through a reconcile that has already
// mutated the cluster.
func (w *Workload) Validate() error {
	switch {
	case strings.TrimSpace(w.Name) == "":
		return fmt.Errorf("empty workload name")
	case !validName(w.Name):
		return fmt.Errorf("workload %q: name must be lowercase alphanumeric or '-'", w.Name)
	case strings.TrimSpace(w.Image) == "":
		return fmt.Errorf("workload %q: empty image", w.Name)
	case w.Replicas < 0:
		return fmt.Errorf("workload %q: negative replicas", w.Name)
	}
	if w.Liveness != nil {
		if len(w.Liveness.Exec) == 0 {
			return fmt.Errorf("workload %q: liveness probe needs a non-empty exec command", w.Name)
		}
		if w.Liveness.FailureThreshold < 0 || w.Liveness.PeriodSeconds < 0 ||
			w.Liveness.TimeoutSeconds < 0 || w.Liveness.InitialDelaySec < 0 {
			return fmt.Errorf("workload %q: liveness probe fields must be non-negative", w.Name)
		}
	}
	return nil
}

// Validate checks the spec is internally consistent: at least one workload,
// each individually valid, with unique names.
func (s *Spec) Validate() error {
	if len(s.Workloads) == 0 {
		return fmt.Errorf("spec has no workloads")
	}
	seen := make(map[string]bool, len(s.Workloads))
	for i := range s.Workloads {
		w := &s.Workloads[i]
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

// Names returns the workload names in sorted order, for stable iteration.
func (s *Spec) Names() []string {
	out := make([]string, len(s.Workloads))
	for i, w := range s.Workloads {
		out[i] = w.Name
	}
	sort.Strings(out)
	return out
}

// Load reads and validates a spec from a JSON file.
func Load(path string) (*Spec, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s Spec
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields() // typos in a cluster spec should be loud
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", path, err)
	}
	return &s, nil
}

// validName allows DNS-label-ish names: lowercase letters, digits, hyphens.
func validName(s string) bool {
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
		default:
			return false
		}
	}
	return s[0] != '-' && s[len(s)-1] != '-'
}
