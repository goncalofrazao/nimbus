// Command nimbusd is Nimbus. One binary, several modes:
//
//	nimbusd serve  [-listen addr]       [-state f]            control plane (HTTP API)
//	nimbusd run    [-spec cluster.json] [-state f] [-interval 5s] [-once]
//	nimbusd apply  -spec cluster.json   [-state f] [-server url]
//	nimbusd scale  <workload> <n>       [-state f] [-server url]
//	nimbusd delete <workload>           [-state f] [-server url]
//	nimbusd get                         [-state f] [-server url]
//	nimbusd status
//	nimbusd down   [-spec cluster.json]
//
// Desired state lives in a durable store (a crash-safe JSON file). `serve`
// exposes it over the control-plane HTTP API; with `-server` the operator
// commands (`apply`, `get`, `scale`, `delete`) talk to that API instead of
// opening the store file directly, so they work from anywhere the control
// plane is reachable. Without `-server` they mutate the local store, which
// pairs with `run` — the all-in-one single-node mode, whose control loop
// reloads the store each pass so changes take effect live.
//
// SIGINT/SIGTERM stop the daemons cleanly and leave the containers running —
// like a kubelet, the daemon going down must not take the workloads with it.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"syscall"
	"time"

	"github.com/goncalofrazao/nimbus/internal/agent"
	"github.com/goncalofrazao/nimbus/internal/api"
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
	case "serve":
		err = serveCmd(log, args)
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
	fmt.Fprint(os.Stderr, `nimbusd — Nimbus, a container orchestrator

usage:
  nimbusd serve  [-listen `+api.DefaultAddr+`] [-state <file>]   control plane: HTTP API over the store
  nimbusd run    [-spec <file>] [-state <file>] [-interval 5s] [-once]
  nimbusd apply  -spec <file>   [-state <file>] [-server <url>]   upsert workloads
  nimbusd scale  <workload> <n> [-state <file>] [-server <url>]   set a workload's replica count
  nimbusd delete <workload>     [-state <file>] [-server <url>]   remove a workload
  nimbusd get                   [-state <file>] [-server <url>]   print desired state
  nimbusd status                                                  show managed containers
  nimbusd down   [-spec <file>]                                   stop & remove containers

Operator commands mutate the local -state file, or a running control plane
when -server (e.g. -server http://`+api.DefaultAddr+`) is given.
`)
}

// stateFlag registers and returns the shared -state flag.
func stateFlag(fs *flag.FlagSet) *string {
	return fs.String("state", defaultStatePath, "path to the persistent desired-state store")
}

// serverFlag registers and returns the shared -server flag. When set, an
// operator command talks HTTP to the control plane instead of opening the
// local store file.
func serverFlag(fs *flag.FlagSet) *string {
	return fs.String("server", "", "control-plane URL (e.g. http://"+api.DefaultAddr+"); empty = use the local -state file")
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

// serveCmd runs the control plane: the HTTP API over the durable store. It
// touches no containers — agents (and, for now, `run` daemons) do that.
func serveCmd(log *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listen := fs.String("listen", api.DefaultAddr, "address to listen on (loopback by default; the API has no auth yet)")
	statePath := stateFlag(fs)
	fs.Parse(args)

	st, err := store.Open(*statePath)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              *listen,
		Handler:           api.NewServer(st, log),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("control plane listening", "addr", *listen, "state", *statePath,
		"generation", st.Generation(), "workloads", len(st.Spec().Workloads))

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errc:
		return err // e.g. the listen address is taken
	case sig := <-sigs:
		log.Info("shutting down", "signal", sig.String())
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(ctx)
	}
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
	server := serverFlag(fs)
	fs.Parse(args)
	if *specPath == "" {
		return fmt.Errorf("apply: -spec is required")
	}
	sp, err := spec.Load(*specPath)
	if err != nil {
		return err
	}

	var n int
	var gen int64
	if *server != "" {
		res, err := api.NewClient(*server).Apply(context.Background(), sp.Workloads)
		if err != nil {
			return err
		}
		n, gen = res.Applied, res.Generation
	} else {
		st, err := store.Open(*statePath)
		if err != nil {
			return err
		}
		if n, err = st.ApplyAll(sp.Workloads); err != nil {
			return err
		}
		gen = st.Generation()
	}
	fmt.Printf("applied %d workload(s) from %s (%d unchanged); generation %d\n",
		n, *specPath, len(sp.Workloads)-n, gen)
	return nil
}

func scaleCmd(args []string) error {
	fs := flag.NewFlagSet("scale", flag.ExitOnError)
	statePath := stateFlag(fs)
	server := serverFlag(fs)
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

	var gen int64
	if *server != "" {
		res, err := api.NewClient(*server).Scale(context.Background(), name, n)
		if err != nil {
			return err
		}
		gen = res.Generation
	} else {
		st, err := store.Open(*statePath)
		if err != nil {
			return err
		}
		existed, err := st.Scale(name, n)
		if err != nil {
			return err
		}
		if !existed {
			return fmt.Errorf("no such workload %q", name)
		}
		gen = st.Generation()
	}
	fmt.Printf("scaled %s to %d; generation %d\n", name, n, gen)
	return nil
}

func deleteCmd(args []string) error {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	statePath := stateFlag(fs)
	server := serverFlag(fs)
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: nimbusd delete <workload>")
	}
	name := pos[0]

	var existed bool
	var gen int64
	if *server != "" {
		res, err := api.NewClient(*server).Delete(context.Background(), name)
		if err != nil {
			return err
		}
		existed, gen = res.Existed, res.Generation
	} else {
		st, err := store.Open(*statePath)
		if err != nil {
			return err
		}
		if existed, err = st.Delete(name); err != nil {
			return err
		}
		gen = st.Generation()
	}
	if !existed {
		fmt.Printf("no such workload %q\n", name)
		return nil
	}
	fmt.Printf("deleted %s; generation %d\n", name, gen)
	return nil
}

func getCmd(args []string) error {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	statePath := stateFlag(fs)
	server := serverFlag(fs)
	fs.Parse(args)

	var gen int64
	var ws []spec.Workload
	if *server != "" {
		st, err := api.NewClient(*server).State(context.Background())
		if err != nil {
			return err
		}
		gen, ws = st.Generation, st.Workloads
	} else {
		st, err := store.Open(*statePath)
		if err != nil {
			return err
		}
		snap := st.State()
		gen, ws = snap.Generation, snap.Workloads
	}

	fmt.Printf("desired state (generation %d):\n", gen)
	if len(ws) == 0 {
		fmt.Println("  (empty)")
		return nil
	}
	fmt.Printf("  %-16s %-24s %s\n", "WORKLOAD", "IMAGE", "REPLICAS")
	for _, w := range ws {
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
