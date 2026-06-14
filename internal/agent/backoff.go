package agent

import (
	"sync"
	"time"
)

// CrashLoopBackOff schedule. A replica that keeps dying is restarted on an
// exponential delay so a crash loop can't hammer the host; a replica that runs
// uninterrupted long enough has its failure streak forgiven.
const (
	backoffBase   = 1 * time.Second
	backoffFactor = 2
	backoffCap    = 5 * time.Minute
	backoffStable = 30 * time.Second
)

// replicaBackoff is one replica's crash history.
type replicaBackoff struct {
	failures     int       // consecutive restarts not yet forgiven
	lastRestart  time.Time // when we last (re)started it
	nextEligible time.Time // earliest time it may be restarted again
}

// backoffTable keys backoff state by container name — the stable replica
// identity. This is ephemeral controller state: it lives only in the running
// agent, is lost on restart (as kubelet's is), and is never part of the
// desired state, which remains derived solely from the runtime.
//
// It is mutex-guarded because a reconcile pass acts on replicas concurrently;
// each replica is a distinct key handled by one goroutine, so the lock only
// protects the map itself, never blocks across a runtime call.
type backoffTable struct {
	mu sync.Mutex
	m  map[string]*replicaBackoff
}

func newBackoffTable() *backoffTable { return &backoffTable{m: map[string]*replicaBackoff{}} }

// known reports whether we have ever recorded a restart for key.
func (t *backoffTable) known(key string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.m[key]
	return ok
}

// ready reports whether a restart is permitted for key at time now; if not, it
// returns the remaining wait. It also returns the current streak length.
func (t *backoffTable) ready(key string, now time.Time) (ok bool, wait time.Duration, failures int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	b := t.m[key]
	if b == nil {
		return true, 0, 0
	}
	if !now.Before(b.nextEligible) {
		return true, 0, b.failures
	}
	return false, b.nextEligible.Sub(now), b.failures
}

// recordRestart bumps the failure streak and schedules the next eligible time,
// returning the new streak length.
func (t *backoffTable) recordRestart(key string, now time.Time) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	b := t.m[key]
	if b == nil {
		b = &replicaBackoff{}
		t.m[key] = b
	}
	b.failures++
	b.lastRestart = now
	b.nextEligible = now.Add(backoffDelay(b.failures))
	return b.failures
}

// observeRunning forgives the streak once a replica has stayed up for
// backoffStable since its last restart.
func (t *backoffTable) observeRunning(key string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if b := t.m[key]; b != nil && now.Sub(b.lastRestart) >= backoffStable {
		delete(t.m, key)
	}
}

// forget drops state for a replica that is no longer desired.
func (t *backoffTable) forget(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.m, key)
}

// backoffDelay returns base * factor^(failures-1), capped at backoffCap.
func backoffDelay(failures int) time.Duration {
	d := backoffBase
	for i := 1; i < failures; i++ {
		d *= backoffFactor
		if d >= backoffCap {
			return backoffCap
		}
	}
	return d
}
