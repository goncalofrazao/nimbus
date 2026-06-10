package autoscaler

import (
	"math"
	"testing"
)

func TestHoltTracksRisingTrend(t *testing.T) {
	p := NewPredictive(10)
	for i := 0; i < 30; i++ {
		p.Observe(100 + 10*float64(i)) // +10 rps per tick
	}
	f := p.Forecast(5)
	if f < 430 { // level ≈390, trend ≈10 ⇒ forecast(5) ≈ 440
		t.Fatalf("forecast(5)=%.1f, want ≥430 (should extrapolate the trend)", f)
	}
}

func TestPredictiveScalesAheadOfReactive(t *testing.T) {
	pr, re := NewPredictive(10), NewReactive(10)
	demand := 100.0
	var pWant, rWant int
	for i := 0; i < 12; i++ {
		pr.Observe(demand)
		re.Observe(demand)
		pWant = pr.Desired(i, demand, pWant)
		rWant = re.Desired(i, demand, rWant)
		demand += 25 // steep ramp
	}
	if pWant <= rWant {
		t.Fatalf("on a steep ramp predictive should request more capacity: predictive=%d reactive=%d", pWant, rWant)
	}
}

func TestFallingTrendIsDamped(t *testing.T) {
	p := NewPredictive(10)
	for i := 0; i < 30; i++ {
		p.Observe(400 - 10*float64(i))
	}
	if f := p.Forecast(10); f < p.level-50 {
		t.Fatalf("falling trend should be damped: forecast(10)=%.1f level=%.1f", f, p.level)
	}
}

// TestSeasonalAnticipatesRecurringSpike: a spike at the same time every
// "day" surprises plain Holt forever, but Holt-Winters sees it coming from
// the second day on.
func TestSeasonalAnticipatesRecurringSpike(t *testing.T) {
	const day, spikeAt, spikeLen = 100, 60, 8
	demand := func(tick int) float64 {
		if i := tick % day; i >= spikeAt && i < spikeAt+spikeLen {
			return 400
		}
		return 100
	}

	hw := NewSeasonalPredictive(10, day)
	holt := NewPredictive(10)
	// Observe two full days plus the run-up to day 3's spike, stopping
	// Lead ticks before it starts: a forecast made here is what decides
	// whether capacity boots in time.
	for tick := 0; tick <= 2*day+spikeAt-hw.Lead; tick++ {
		hw.Observe(demand(tick))
		holt.Observe(demand(tick))
	}
	if f := hw.Forecast(hw.Lead); f < 200 {
		t.Errorf("holt-winters should anticipate the day-3 spike: forecast=%.1f want ≥200", f)
	}
	if f := holt.Forecast(holt.Lead); f > 150 {
		t.Errorf("plain holt should NOT see the spike coming: forecast=%.1f want ≤150", f)
	}
}

// TestSeasonalWarmup: before one full period is observed, the seasonal
// component must stay silent (no garbage forecasts).
func TestSeasonalWarmup(t *testing.T) {
	hw := NewSeasonalPredictive(10, 100)
	for i := 0; i < 50; i++ {
		hw.Observe(200)
	}
	if f := hw.Forecast(5); math.Abs(f-200) > 10 {
		t.Errorf("mid-warmup forecast on flat demand: %.1f want ≈200", f)
	}
}

func TestDesiredNeverBelowOne(t *testing.T) {
	p := NewPredictive(10)
	p.Observe(0)
	if got := p.Desired(0, 0, 5); got < 1 {
		t.Fatalf("desired=%d, want ≥1", got)
	}
	r := NewReactive(10)
	if got := r.Desired(100, 0, 1); got < 1 {
		t.Fatalf("reactive desired=%d, want ≥1", got)
	}
	_ = math.MaxInt // keep math import meaningful if constants change
}
