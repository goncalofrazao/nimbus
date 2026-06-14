// Command nimbusd is the Nimbus node daemon. It continuously reconciles the
// containers on this host toward a declared spec, restarting whatever drifts.
//
//	nimbusd run    -spec cluster.json [-interval 5s] [-once]
//	nimbusd status
//	nimbusd down   [-spec cluster.json]
//
// `run` is the control loop: reconcile on start, then every interval, plus an
// immediate pass on SIGHUP (reload the spec from disk). SIGINT/SIGTERM stop
// the loop cleanly and leave the containers running — like a kubelet, the
// daemon going down must not take the workloads with it.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/goncalofrazao/nimbus/internal/agent"
	"github.com/goncalofrazao/nimbus/internal/runtime"
	"github.com/goncalofrazao/nimbus/internal/spec"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{}))

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]

	var err error
	switch cmd {
	case "run":
		err = runCmd(log, args)
	case "status":
		err = statusCmd(args)
	case "down":
		err = downCmd(log, args)
	case "-h", "--help", "help":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `nimbusd — Nimbus node daemon

usage:
  nimbusd run    -spec <file> [-interval 5s] [-once]   reconcile loop
  nimbusd status                                        show managed containers
  nimbusd down   [-spec <file>]                         stop & remove workloads
`)
}

// connect returns a runtime client after confirming the daemon is reachable —
// fail fast with a clear message rather than deep inside the first reconcile.
func connect(ctx context.Context) (*runtime.Client, error) {
	rt := runtime.New()
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rt.Ping(cctx); err != nil {
		return nil, fmt.Errorf("cannot reach docker at %s: %w", runtime.DefaultSocket, err)
	}
	return rt, nil
}

func runCmd(log *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	specPath := fs.String("spec", "", "path to the cluster spec (JSON)")
	interval := fs.Duration("interval", 5*time.Second, "reconcile interval")
	once := fs.Bool("once", false, "reconcile a single time and exit")
	fs.Parse(args)
	if *specPath == "" {
		return fmt.Errorf("run: -spec is required")
	}

	ctx := context.Background()
	rt, err := connect(ctx)
	if err != nil {
		return err
	}
	rec := agent.New(rt)

	load := func() (*spec.Spec, error) { return spec.Load(*specPath) }
	s, err := load()
	if err != nil {
		return err
	}

	reconcile := func(s *spec.Spec) {
		rctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		rep, err := rec.Reconcile(rctx, s)
		if err != nil {
			log.Error("reconcile failed", "err", err)
			return
		}
		for _, a := range rep.Actions {
			if a.Verb == "none" {
				continue
			}
			lvl := slog.LevelInfo
			switch {
			case a.Err != nil:
				lvl = slog.LevelError
			case a.Verb == "backoff":
				lvl = slog.LevelWarn
			}
			attrs := []any{"action", a.Verb, "workload", a.Workload, "replica", a.Replica, "id", a.ID}
			if a.Failures > 0 {
				attrs = append(attrs, "crashes", a.Failures)
			}
			if a.Verb == "backoff" {
				attrs = append(attrs, "wait", a.Wait.Round(time.Second).String())
			}
			if a.Err != nil {
				attrs = append(attrs, "err", a.Err)
			}
			log.Log(ctx, lvl, "reconcile", attrs...)
		}
		if rep.Changed() {
			log.Info("converged", "changes", len(rep.Actions), "errors", len(rep.Errs()))
		}
	}

	log.Info("nimbusd starting", "spec", *specPath, "interval", interval.String(),
		"workloads", len(s.Workloads))
	reconcile(s)
	if *once {
		return nil
	}

	// Signals: HUP reloads the spec and reconciles now; INT/TERM shut down.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			reconcile(s)
		case sig := <-sigs:
			switch sig {
			case syscall.SIGHUP:
				ns, err := load()
				if err != nil {
					log.Error("spec reload failed, keeping previous", "err", err)
					continue
				}
				s = ns
				log.Info("spec reloaded", "workloads", len(s.Workloads))
				reconcile(s)
			default:
				log.Info("shutting down; workloads left running", "signal", sig.String())
				return nil
			}
		}
	}
}

func statusCmd(args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	fs.Parse(args)

	ctx := context.Background()
	rt, err := connect(ctx)
	if err != nil {
		return err
	}
	cs, err := rt.List(ctx, nil)
	if err != nil {
		return err
	}
	if len(cs) == 0 {
		fmt.Println("no nimbus-managed containers")
		return nil
	}
	sort.Slice(cs, func(i, j int) bool {
		if cs[i].Workload() != cs[j].Workload() {
			return cs[i].Workload() < cs[j].Workload()
		}
		return cs[i].Labels[runtime.LabelReplica] < cs[j].Labels[runtime.LabelReplica]
	})
	fmt.Printf("%-16s %-8s %-10s %-14s %s\n", "WORKLOAD", "REPLICA", "STATE", "ID", "IMAGE")
	for _, c := range cs {
		fmt.Printf("%-16s %-8s %-10s %-14s %s\n",
			c.Workload(), c.Labels[runtime.LabelReplica], c.State, shortID(c.ID), c.Image)
	}
	return nil
}

func downCmd(log *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("down", flag.ExitOnError)
	specPath := fs.String("spec", "", "only tear down workloads named in this spec (default: all)")
	fs.Parse(args)

	ctx := context.Background()
	rt, err := connect(ctx)
	if err != nil {
		return err
	}

	var only map[string]bool
	if *specPath != "" {
		s, err := spec.Load(*specPath)
		if err != nil {
			return err
		}
		only = make(map[string]bool, len(s.Workloads))
		for _, w := range s.Workloads {
			only[w.Name] = true
		}
	}

	cs, err := rt.List(ctx, nil)
	if err != nil {
		return err
	}
	removed := 0
	for _, c := range cs {
		if only != nil && !only[c.Workload()] {
			continue
		}
		_ = rt.Stop(ctx, c.ID, 5*time.Second)
		if err := rt.Remove(ctx, c.ID, true); err != nil {
			log.Error("remove failed", "id", shortID(c.ID), "err", err)
			continue
		}
		log.Info("removed", "workload", c.Workload(), "replica", c.Labels[runtime.LabelReplica], "id", shortID(c.ID))
		removed++
	}
	fmt.Printf("removed %d container(s)\n", removed)
	return nil
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}
