// Package autoscaler sizes the replica count for a workload.
//
// Reactive   ~ Kubernetes HPA: looks at *current* load and scales when a
//
//	threshold is crossed. By the time new pods need new nodes
//	(5-tick boot), the spike has already burned you.
//
// Predictive ~ Nimbus: Holt double-exponential smoothing tracks both the
//
//	level AND the trend of demand, then provisions for the
//	forecast `lead` ticks ahead — lead is sized to cover node
//	boot time, so capacity arrives *before* demand does.
package autoscaler

import "math"

// Autoscaler decides the desired replica count each tick.
type Autoscaler interface {
	Name() string
	Observe(demand float64)
	Desired(tick int, demandNow float64, currentReplicas int) int
}

// ---------------------------------------------------------------------------

// Reactive mimics the K8s Horizontal Pod Autoscaler.
type Reactive struct {
	RPSPerReplica float64
	TargetUtil    float64 // keep replicas ~70% busy
	DownCooldown  int
	lastDownscale int
}

func NewReactive(rpsPerReplica float64) *Reactive {
	return &Reactive{
		RPSPerReplica: rpsPerReplica,
		TargetUtil:    0.70,
		DownCooldown:  15,
		lastDownscale: math.MinInt32,
	}
}

func (r *Reactive) Name() string    { return "reactive (HPA-style)" }
func (r *Reactive) Observe(float64) {} // reactive: no model to update

func (r *Reactive) Desired(tick int, demandNow float64, current int) int {
	want := int(math.Ceil(demandNow / (r.RPSPerReplica * r.TargetUtil)))
	if want < 1 {
		want = 1
	}
	if want < current {
		if tick-r.lastDownscale < r.DownCooldown {
			return current // cooldown: hold
		}
		r.lastDownscale = tick
		// HPA scales down conservatively (a couple of pods per step).
		if want < current-2 {
			return current - 2
		}
	}
	return want
}

// ---------------------------------------------------------------------------

// Predictive is the Nimbus autoscaler (Holt's linear smoothing).
type Predictive struct {
	RPSPerReplica float64
	Lead          int     // forecast horizon ≥ node boot time
	TargetUtil    float64 // trend-aware ⇒ can run pods hotter
	Alpha, Beta   float64

	level       float64
	trend       float64
	initialized bool
}

func NewPredictive(rpsPerReplica float64) *Predictive {
	return &Predictive{
		RPSPerReplica: rpsPerReplica,
		Lead:          7,
		TargetUtil:    0.75,
		Alpha:         0.55,
		Beta:          0.35,
	}
}

func (p *Predictive) Name() string { return "predictive (nimbus)" }

// Observe runs the Holt double-exponential smoothing update.
func (p *Predictive) Observe(demand float64) {
	if !p.initialized {
		p.level, p.initialized = demand, true
		return
	}
	prev := p.level
	p.level = p.Alpha*demand + (1-p.Alpha)*(p.level+p.trend)
	p.trend = p.Beta*(p.level-prev) + (1-p.Beta)*p.trend
}

// Forecast projects demand k ticks ahead. Falling trends are damped so we
// don't underscale on noise.
func (p *Predictive) Forecast(k int) float64 {
	if !p.initialized {
		return 0
	}
	t := p.trend
	if t < 0 {
		t *= 0.3
	}
	f := p.level + float64(k)*t
	if f < 0 {
		return 0
	}
	return f
}

func (p *Predictive) Desired(tick int, demandNow float64, current int) int {
	peak := demandNow
	for k := 1; k <= p.Lead; k++ {
		if f := p.Forecast(k); f > peak {
			peak = f
		}
	}
	// Spike acceleration: when demand rises steeply, over-provision the
	// forecast so capacity lands before the curve does.
	base := p.level
	if base < 1 {
		base = 1
	}
	if p.trend > 0.04*base {
		peak *= 1.15
	}
	want := int(math.Ceil(peak * 1.08 / (p.RPSPerReplica * p.TargetUtil)))
	if want < 1 {
		want = 1
	}
	if want < current {
		// gentle, continuous downscale — the forecast guards against flap
		return current - 1
	}
	return want
}
