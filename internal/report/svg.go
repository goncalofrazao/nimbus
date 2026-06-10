// Package report renders the head-to-head results as an SVG chart using
// only the standard library — no chart dependencies needed.
package report

import (
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/goncalofrazao/nimbus/internal/sim"
)

const (
	width    = 1100
	panelH   = 250
	padL     = 60
	padR     = 20
	padT     = 34
	padB     = 28
	plotW    = width - padL - padR
	plotH    = panelH - padT - padB
	colGrey  = "#999999"
	colRed   = "#d62728"
	colGreen = "#2ca02c"
)

// WriteSVG renders three stacked panels: each control plane's
// demand-vs-capacity, then the node count (cost) comparison.
func WriteSVG(path string, k8s, nim *sim.Metrics) error {
	var b strings.Builder
	total := 3 * panelH
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" font-family="sans-serif">`+"\n", width, total)
	fmt.Fprintf(&b, `<rect width="%d" height="%d" fill="white"/>`+"\n", width, total)

	maxRPS := maxOf(k8s.Demand, k8s.Capacity, nim.Capacity)
	panelSeries(&b, 0, "K8s-style: reactive HPA + spread scheduling",
		k8s, colRed, maxRPS)
	panelSeries(&b, 1, "Nimbus: predictive autoscaling + bin-packing",
		nim, colGreen, maxRPS)
	panelNodes(&b, 2, k8s, nim)

	b.WriteString("</svg>\n")
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func panelSeries(b *strings.Builder, idx int, title string,
	m *sim.Metrics, color string, maxV float64) {

	oy := idx * panelH
	fmt.Fprintf(b, `<text x="%d" y="%d" font-size="14" font-weight="bold">%s</text>`+"\n", padL, oy+20, title)
	axes(b, oy, maxV, "rps")

	// SLO violation shading: thin red columns on ticks where any workload
	// was undersupplied (aggregate capacity can mask per-tenant shortfalls).
	for i := range m.Demand {
		if m.Unserved[i] <= 0 {
			continue
		}
		x := xPos(i, len(m.Demand))
		y1 := yPos(math.Max(m.Demand[i], m.Capacity[i]), maxV, oy)
		y2 := yPos(math.Min(m.Demand[i], m.Capacity[i]), maxV, oy)
		h := y2 - y1
		if h < 2 {
			h = 2
		}
		fmt.Fprintf(b, `<rect x="%.1f" y="%.1f" width="1.6" height="%.1f" fill="%s" opacity="0.35"/>`+"\n",
			x-0.8, y1, h, colRed)
	}
	polyline(b, m.Demand, maxV, oy, colGrey, 1)
	polyline(b, m.Capacity, maxV, oy, color, 1.6)
	legend(b, oy, [][2]string{{colGrey, "demand (rps)"}, {color, "capacity"}, {colRed, "SLO violations"}})
}

func panelNodes(b *strings.Builder, idx int, k8s, nim *sim.Metrics) {
	oy := idx * panelH
	fmt.Fprintf(b, `<text x="%d" y="%d" font-size="14" font-weight="bold">Provisioned nodes (cost)</text>`+"\n", padL, oy+20)

	maxN := 0.0
	kn, nn := toF(k8s.Nodes), toF(nim.Nodes)
	for _, v := range append(append([]float64{}, kn...), nn...) {
		if v > maxN {
			maxN = v
		}
	}
	maxN *= 1.08
	axes(b, oy, maxN, "nodes")
	polyline(b, kn, maxN, oy, colRed, 1.6)
	polyline(b, nn, maxN, oy, colGreen, 1.6)
	legend(b, oy, [][2]string{{colRed, "k8s-style"}, {colGreen, "nimbus"}})
	fmt.Fprintf(b, `<text x="%d" y="%d" font-size="11" fill="#555">minute</text>`+"\n", padL+plotW/2, oy+panelH-6)
}

// --- low-level helpers ------------------------------------------------------

func axes(b *strings.Builder, oy int, maxV float64, unit string) {
	fmt.Fprintf(b, `<rect x="%d" y="%d" width="%d" height="%d" fill="none" stroke="#ccc"/>`+"\n",
		padL, oy+padT, plotW, plotH)
	for _, f := range []float64{0, 0.5, 1.0} {
		y := float64(oy+padT) + (1-f)*float64(plotH)
		fmt.Fprintf(b, `<text x="%d" y="%.1f" font-size="10" fill="#555" text-anchor="end">%.0f</text>`+"\n",
			padL-6, y+3, f*maxV)
	}
	fmt.Fprintf(b, `<text x="%d" y="%d" font-size="10" fill="#555">%s</text>`+"\n", 8, oy+padT+12, unit)
}

func polyline(b *strings.Builder, vals []float64, maxV float64, oy int, color string, w float64) {
	var pts strings.Builder
	for i, v := range vals {
		fmt.Fprintf(&pts, "%.1f,%.1f ", xPos(i, len(vals)), yPos(v, maxV, oy))
	}
	fmt.Fprintf(b, `<polyline points="%s" fill="none" stroke="%s" stroke-width="%.1f"/>`+"\n",
		strings.TrimSpace(pts.String()), color, w)
}

func legend(b *strings.Builder, oy int, items [][2]string) {
	x := padL + 10
	for _, it := range items {
		fmt.Fprintf(b, `<rect x="%d" y="%d" width="12" height="4" fill="%s"/>`+"\n", x, oy+padT+8, it[0])
		fmt.Fprintf(b, `<text x="%d" y="%d" font-size="11" fill="#333">%s</text>`+"\n", x+16, oy+padT+13, it[1])
		x += 16 + 8*len(it[1]) + 24
	}
}

func xPos(i, n int) float64 {
	return float64(padL) + float64(i)/float64(n-1)*float64(plotW)
}

func yPos(v, maxV float64, oy int) float64 {
	return float64(oy+padT) + (1-v/maxV)*float64(plotH)
}

func maxOf(series ...[]float64) float64 {
	m := 0.0
	for _, s := range series {
		for _, v := range s {
			if v > m {
				m = v
			}
		}
	}
	return m * 1.08
}

func toF(in []int) []float64 {
	out := make([]float64, len(in))
	for i, v := range in {
		out[i] = float64(v)
	}
	return out
}
