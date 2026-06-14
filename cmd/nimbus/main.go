// Command nimbus runs the head-to-head: a K8s-style control plane
// (reactive HPA + spread scheduling) vs Nimbus (predictive autoscaling +
// scored bin-packing) on an identical traffic trace.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/goncalofrazao/nimbus/internal/autoscaler"
	"github.com/goncalofrazao/nimbus/internal/report"
	"github.com/goncalofrazao/nimbus/internal/scheduler"
	"github.com/goncalofrazao/nimbus/internal/sim"
)

type summary struct {
	ControlPlane        string  `json:"control_plane"`
	SLOViolationMinutes int     `json:"slo_violation_minutes"`
	UnservedTrafficPct  float64 `json:"unserved_traffic_pct"`
	NodeHours           float64 `json:"node_hours"`
	CostDollars         float64 `json:"cost_dollars"`
	AvgNodeUtilPct      float64 `json:"avg_node_utilization_pct"`
	PeakNodes           int     `json:"peak_nodes"`
}

func summarize(m *sim.Metrics) summary {
	return summary{
		ControlPlane:        m.Name,
		SLOViolationMinutes: m.SLOViolationTicks(),
		UnservedTrafficPct:  round2(m.UnservedPct()),
		NodeHours:           round1(m.NodeHours()),
		CostDollars:         round2(m.CostDollars()),
		AvgNodeUtilPct:      round1(m.AvgUtilization()),
		PeakNodes:           m.PeakNodes(),
	}
}

func main() {
	ticks := flag.Int("ticks", 720, "minutes to simulate")
	seed := flag.Int64("seed", 42, "traffic noise seed")
	period := flag.Int("period", 0,
		"Holt-Winters seasonal period in minutes (0 = off; try 1440 with -ticks 4320)")
	spot := flag.Bool("spot", false,
		"add a third run: Nimbus on spot/preemptible nodes (up to 70% of fleet)")
	svgPath := flag.String("svg", "", "write SVG chart to this path")
	jsonPath := flag.String("json", "", "write results JSON to this path")
	flag.Parse()

	ws := sim.Workloads(*ticks, *seed)

	nimAS := autoscaler.PredictiveFactory
	if *period > 0 {
		nimAS = autoscaler.SeasonalPredictiveFactory(*period)
	}
	k8s := sim.Run("k8s-style", scheduler.Spread{}, autoscaler.ReactiveFactory, ws)
	nim := sim.Run("nimbus", scheduler.BinPack{}, nimAS, ws)

	runs := []*sim.Metrics{k8s, nim}
	if *spot {
		nimSpot := sim.Run("nimbus+spot", scheduler.BinPack{}, nimAS, ws,
			sim.WithSpot(0.70, 0.02, *seed))
		runs = append(runs, nimSpot)
	}

	summaries := make([]summary, len(runs))
	for i, m := range runs {
		summaries[i] = summarize(m)
	}
	ks, ns := summaries[0], summaries[1]

	fmt.Println("\n=== multi-tenant traffic replay: identical demand, parallel control planes ===")
	fmt.Println()
	fmt.Printf("%-12s %22s %22s %12s %12s %26s %12s\n", "control_plane",
		"slo_violation_minutes", "unserved_traffic_pct", "node_hours",
		"cost_dollars", "avg_node_utilization_pct", "peak_nodes")
	for _, s := range summaries {
		fmt.Printf("%-12s %22d %22.2f %12.1f %12.2f %26.1f %12d\n", s.ControlPlane,
			s.SLOViolationMinutes, s.UnservedTrafficPct, s.NodeHours,
			s.CostDollars, s.AvgNodeUtilPct, s.PeakNodes)
	}

	saved := 100 * (1 - ns.NodeHours/ks.NodeHours)
	fmt.Println("\n--- Nimbus vs K8s-style ---")
	fmt.Printf("  SLO violation minutes : %d -> %d\n", ks.SLOViolationMinutes, ns.SLOViolationMinutes)
	fmt.Printf("  Unserved traffic      : %.2f%% -> %.2f%%\n", ks.UnservedTrafficPct, ns.UnservedTrafficPct)
	fmt.Printf("  Node-hours (cost)     : %.1f -> %.1f (%.1f%% cost saved)\n",
		ks.NodeHours, ns.NodeHours, saved)
	fmt.Printf("  Avg node utilization  : %.1f%% -> %.1f%%\n",
		ks.AvgNodeUtilPct, ns.AvgNodeUtilPct)
	if *spot {
		sp := summaries[2]
		billSaved := 100 * (1 - sp.CostDollars/ns.CostDollars)
		fmt.Printf("  Spot bill vs on-demand: $%.2f -> $%.2f (%.1f%% bill saved, SLO %d min)\n",
			ns.CostDollars, sp.CostDollars, billSaved, sp.SLOViolationMinutes)
	}
	fmt.Println()

	if *jsonPath != "" {
		data, _ := json.MarshalIndent(summaries, "", "  ")
		if err := os.WriteFile(*jsonPath, data, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "write json:", err)
			os.Exit(1)
		}
	}
	if *svgPath != "" {
		if err := report.WriteSVG(*svgPath, k8s, nim); err != nil {
			fmt.Fprintln(os.Stderr, "write svg:", err)
			os.Exit(1)
		}
		fmt.Println("chart saved to", *svgPath)
	}
}

func round1(v float64) float64 { return float64(int(v*10+0.5)) / 10 }
func round2(v float64) float64 { return float64(int(v*100+0.5)) / 100 }
