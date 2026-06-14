// Command nimbusd is the Nimbus node daemon. It reconciles the containers on
// this host toward a persistent, declared desired state.
//
//	nimbusd run    [-spec cluster.json] [-state f] [-interval 5s] [-once]
//	nimbusd apply  -spec cluster.json   [-state f]
//	nimbusd scale  <workload> <n>       [-state f]
//	nimbusd delete <workload>           [-state f]
//	nimbusd get                         [-state f]
//	nimbusd status
//	nimbusd down   [-spec cluster.json]
//
// Desired state lives in a durable store (a crash-safe JSON file). `apply`,
// `scale` and `delete` mutate it; `run` is the control loop, reconciling from
// the store and reloading it each pass so changes from those commands take
// effect live. `run -spec` seeds the store from a file for convenience.
//
// SIGINT/SIGTERM stop the loop cleanly and leave the containers running — like
// a kubelet, the daemon going down must not take the workloads with it.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"syscall"
	"time"

	"github.com/goncalofrazao/nimbus/internal/agent"
	"github.com/goncalofrazao/nimbus/internal/runtime"
	"github.com/goncalofrazao/nimbus/internal/spec"
	"github.com/goncalofrazao/nimbus/internal/store"
)

// defaultStatePath is where desired state is persisted unless -state overrides.
const defaultStatePath = "nimbus-state.json"

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
	case "apply":
		err = applyCmd(args)
	case "scale":
		err = scaleCmd(args)
	case "delete":
		err = deleteCmd(args)
	case "get":
		err = getCmd(args)
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
  nimbusd run    [-spec <file>] [-state <file>] [-interval 5s] [-once]
  nimbusd apply  -spec <file>   [-state <file>]   upsert workloads into the store
  nimbusd scale  <workload> <n> [-state <file>]   set a workload's replica count
  nimbusd delete <workload>     [-state <file>]   remove a workload
  nimbusd get                   [-state <file>]   print desired state
  nimbusd status                                  show managed containers
  nimbusd down   [-spec <file>]                   stop & remove containers
`)
}

// stateFlag registers and returns the shared -state flag.
func stateFlag(fs *flag.FlagSet) *string {
	return fs.String("state", defaultStatePath, "path to the persistent desired-state store")
}

// parseArgs parses flags that may appear before, after, or between positional
// arguments (Go's flag package otherwise stops at the first positional), and
// returns the positionals. So `scale web 4 -state s` works as readily as
// `scale -state s web 4`.
func parseArgs(fs *flag.FlagSet, args []string) ([]string, error) {
	var positionals []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if fs.NArg() == 0 {
			return positionals, nil
		}
		positionals = append(positionals, fs.Arg(0))
		args = fs.Args()[1:]
	}
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
	specPath := fs.String("spec", "", "seed the store from this spec file before running (optional)")
	statePath := stateFlag(fs)
	interval := fs.Duration("interval", 5*time.Second, "reconcile interval")
	once := fs.Bool("once", false, "reconcile a single time and exit")
	fs.Parse(args)

	ctx := context.Background()
	rt, err := connect(ctx)
	if err != nil {
		return err
	}
	rec := agent.New(rt)

	st, err := store.Open(*statePath)
	if err != nil {
		return err
	}
	// Optionally seed desired state from a spec file (the newcomer path).
	seed := func() error {
		if *specPath == "" {
			return nil
		}
		sp, err := spec.Load(*specPath)
		if err != nil {
			return err
		}
		n, err := st.ApplyAll(sp.Workloads)
		if err != nil {
			return err
		}
		if n > 0 {
			log.Info("seeded store from spec", "spec", *specPath, "changed", n)
		}
		return nil
	}
	if err := seed(); err != nil {
		return err
	}

	reconcile := func() {
		if _, err := st.Reload(); err != nil {
			log.Error("store reload failed, keeping previous desired state", "err", err)
		}
		s := st.Spec()
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
			case a.Verb == "backoff", a.Verb == "unhealthy", a.Verb == "killed":
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
			log.Info("converged", "generation", st.Generation(),
				"changes", len(rep.Actions), "errors", len(rep.Errs()))
		}
	}

	log.Info("nimbusd starting", "state", *statePath, "interval", interval.String(),
		"generation", st.Generation(), "workloads", len(st.Spec().Workloads))
	reconcile()
	if *once {
		return nil
	}

	// Signals: HUP re-seeds from -spec and reconciles now; INT/TERM shut down.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			reconcile()
		case sig := <-sigs:
			switch sig {
			case syscall.SIGHUP:
				if err := seed(); err != nil {
					log.Error("spec re-seed failed, keeping store", "err", err)
				}
				reconcile()
			default:
				log.Info("shutting down; workloads left running", "signal", sig.String())
				return nil
			}
		}
	}
}

func applyCmd(args []string) error {
	fs := flag.NewFlagSet("apply", flag.ExitOnError)
	specPath := fs.String("spec", "", "path to the spec file to apply (JSON)")
	statePath := stateFlag(fs)
	fs.Parse(args)
	if *specPath == "" {
		return fmt.Errorf("apply: -spec is required")
	}
	sp, err := spec.Load(*specPath)
	if err != nil {
		return err
	}
	st, err := store.Open(*statePath)
	if err != nil {
		return err
	}
	n, err := st.ApplyAll(sp.Workloads)
	if err != nil {
		return err
	}
	fmt.Printf("applied %d workload(s) from %s (%d unchanged); generation %d\n",
		n, *specPath, len(sp.Workloads)-n, st.Generation())
	return nil
}

func scaleCmd(args []string) error {
	fs := flag.NewFlagSet("scale", flag.ExitOnError)
	statePath := stateFlag(fs)
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("usage: nimbusd scale <workload> <replicas>")
	}
	name := pos[0]
	n, err := strconv.Atoi(pos[1])
	if err != nil {
		return fmt.Errorf("replicas must be an integer: %q", pos[1])
	}
	st, err := store.Open(*statePath)
	if err != nil {
		return err
	}
	if err := st.Scale(name, n); err != nil {
		return err
	}
	fmt.Printf("scaled %s to %d; generation %d\n", name, n, st.Generation())
	return nil
}

func deleteCmd(args []string) error {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	statePath := stateFlag(fs)
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: nimbusd delete <workload>")
	}
	name := pos[0]
	st, err := store.Open(*statePath)
	if err != nil {
		return err
	}
	existed, err := st.Delete(name)
	if err != nil {
		return err
	}
	if !existed {
		fmt.Printf("no such workload %q\n", name)
		return nil
	}
	fmt.Printf("deleted %s; generation %d\n", name, st.Generation())
	return nil
}

func getCmd(args []string) error {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	statePath := stateFlag(fs)
	fs.Parse(args)
	st, err := store.Open(*statePath)
	if err != nil {
		return err
	}
	s := st.Spec()
	fmt.Printf("desired state (generation %d):\n", st.Generation())
	if len(s.Workloads) == 0 {
		fmt.Println("  (empty)")
		return nil
	}
	fmt.Printf("  %-16s %-24s %s\n", "WORKLOAD", "IMAGE", "REPLICAS")
	for _, w := range s.Workloads {
		live := ""
		if w.Liveness != nil {
			live = "  (liveness)"
		}
		fmt.Printf("  %-16s %-24s %d%s\n", w.Name, w.Image, w.Replicas, live)
	}
	return nil
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
