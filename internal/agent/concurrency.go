package agent

import "sync"

// defaultParallelism caps how many container operations a reconcile pass runs
// at once. Reconcile work is I/O-bound on the container runtime (pulls, stops
// with a grace period, exec probes), so a handful of concurrent operations
// turns an O(replicas) serial pass into a roughly constant-time one without
// hammering the daemon.
const defaultParallelism = 8

// runParallel executes each task with at most `limit` running concurrently and
// returns their results positionally (out[i] is tasks[i]()'s result). A limit
// of 1 (or a single task) runs sequentially, which keeps tests deterministic
// and avoids goroutine overhead for trivial passes.
func runParallel[T any](limit int, tasks []func() T) []T {
	out := make([]T, len(tasks))
	if limit < 1 {
		limit = 1
	}
	if limit == 1 || len(tasks) <= 1 {
		for i, t := range tasks {
			out[i] = t()
		}
		return out
	}
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	for i := range tasks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			out[i] = tasks[i]()
		}(i)
	}
	wg.Wait()
	return out
}
