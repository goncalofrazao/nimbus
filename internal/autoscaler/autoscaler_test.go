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
