package dpengine

import (
	"errors"
	"math"
	"testing"
)

func newEngine(t *testing.T, seed uint64, dataset string, budget float64) *Engine {
	t.Helper()
	l := NewLedger()
	if err := l.AddDataset(dataset, budget); err != nil {
		t.Fatal(err)
	}
	return NewEngine(NewLaplaceMechanism(seed), l)
}

func TestMechanismReproducibleWithFixedSeed(t *testing.T) {
	q := Query{Dataset: "ds", RequestID: "r1", Epsilon: 0.5,
		Composition: SequentialComposition,
		Values:      []float64{1, 2, 3}, Lower: 0, Upper: 5, BoundMode: BoundClamp}

	e1 := newEngine(t, 42, "ds", 10)
	e2 := newEngine(t, 42, "ds", 10)
	r1, err := e1.Execute(q)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := e2.Execute(q)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input: seed=42 values=[1 2 3] bounds=[0,5] eps=0.5 -> noisy=%.10f (both runs)", r1.NoisySum)
	if r1.NoisySum != r2.NoisySum {
		t.Fatalf("same seed must reproduce: %v vs %v", r1.NoisySum, r2.NoisySum)
	}
	// True sum is 6; noise must be finite.
	if math.IsNaN(r1.NoisySum) || math.IsInf(r1.NoisySum, 0) {
		t.Fatalf("non-finite result: %v", r1.NoisySum)
	}
}

func TestMechanismNoiseMagnitude(t *testing.T) {
	// With scale b = sensitivity/epsilon, most samples lie within a few b.
	m := NewLaplaceMechanism(7)
	const b = 2.0 // sensitivity 2, epsilon 1
	maxAbs := 0.0
	for i := 0; i < 1000; i++ {
		if x := math.Abs(m.Noise(2.0, 1.0)); x > maxAbs {
			maxAbs = x
		}
	}
	t.Logf("input: 1000 Laplace(0, b=%.1f) samples -> max|noise|=%.2f", b, maxAbs)
	if maxAbs > 30*b {
		t.Fatalf("noise magnitude implausible: max|noise|=%v", maxAbs)
	}
}

func TestExecuteClampPath(t *testing.T) {
	e := newEngine(t, 1, "ds", 10)
	q := Query{Dataset: "ds", RequestID: "r1", Epsilon: 1,
		Composition: SequentialComposition,
		Values:      []float64{-100, 3, 100}, Lower: 0, Upper: 5, BoundMode: BoundClamp}
	r, err := e.Execute(q)
	if err != nil {
		t.Fatal(err)
	}
	// Clamped sum is 0 + 3 + 5 = 8; noise scale is (5-0)/1 = 5.
	t.Logf("input: values=[-100 3 100] bounds=[0,5] mode=clamp -> clampedRows=%d noisy=%.4f",
		r.ClampedRows, r.NoisySum)
	if r.ClampedRows != 2 {
		t.Fatalf("want 2 clamped rows, got %d", r.ClampedRows)
	}
	if math.Abs(r.NoisySum-8) > 50 {
		t.Fatalf("noisy sum %v implausibly far from clamped sum 8", r.NoisySum)
	}
}

func TestExecuteRejectPath(t *testing.T) {
	e := newEngine(t, 1, "ds", 10)
	q := Query{Dataset: "ds", RequestID: "r1", Epsilon: 1,
		Composition: SequentialComposition,
		Values:      []float64{1, 100}, Lower: 0, Upper: 5, BoundMode: BoundReject}
	_, err := e.Execute(q)
	var dpe *Error
	errors.As(err, &dpe)
	t.Logf("input: values=[1 100] bounds=[0,5] mode=reject -> reason=%q", dpe.Reason)
	if dpe == nil || dpe.Reason != ReasonUnboundedInput {
		t.Fatalf("want ReasonUnboundedInput, got %v", err)
	}
	// Rejected query must not consume budget.
	spent, _, _ := e.Ledger().Spent("ds")
	if spent != 0 {
		t.Fatalf("rejected query consumed budget: spent=%v", spent)
	}
}

func TestExecuteUnboundedInputs(t *testing.T) {
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		e := newEngine(t, 1, "ds", 10)
		q := Query{Dataset: "ds", RequestID: "r1", Epsilon: 1,
			Composition: SequentialComposition,
			Values:      []float64{v}, Lower: 0, Upper: 5, BoundMode: BoundClamp}
		_, err := e.Execute(q)
		var dpe *Error
		errors.As(err, &dpe)
		t.Logf("input: value=%v -> reason=%q (never silently accepted)", v, dpe.Reason)
		if dpe == nil || dpe.Reason != ReasonUnboundedInput {
			t.Fatalf("value %v: want ReasonUnboundedInput, got %v", v, err)
		}
	}
}

func TestExecuteInvalidQueryParameters(t *testing.T) {
	e := newEngine(t, 1, "ds", 10)
	cases := []struct {
		name string
		q    Query
	}{
		{"empty values", Query{Dataset: "ds", RequestID: "r", Epsilon: 1, Lower: 0, Upper: 1}},
		{"inverted bounds", Query{Dataset: "ds", RequestID: "r", Epsilon: 1, Values: []float64{1}, Lower: 5, Upper: 0}},
		{"equal bounds", Query{Dataset: "ds", RequestID: "r", Epsilon: 1, Values: []float64{1}, Lower: 2, Upper: 2}},
		{"bad bound mode", Query{Dataset: "ds", RequestID: "r", Epsilon: 1, Values: []float64{1}, Lower: 0, Upper: 1, BoundMode: BoundMode(9)}},
	}
	for _, tc := range cases {
		_, err := e.Execute(tc.q)
		var dpe *Error
		errors.As(err, &dpe)
		t.Logf("input: %s -> reason=%q", tc.name, dpe.Reason)
		if dpe == nil || dpe.Reason != ReasonInvalidParameter {
			t.Errorf("%s: want ReasonInvalidParameter, got %v", tc.name, err)
		}
	}
}

func TestExecuteBudgetIntegration(t *testing.T) {
	e := newEngine(t, 1, "ds", 1.0)
	mk := func(id string, eps float64) Query {
		return Query{Dataset: "ds", RequestID: id, Epsilon: eps,
			Composition: SequentialComposition,
			Values:      []float64{1, 2}, Lower: 0, Upper: 5, BoundMode: BoundClamp}
	}
	if _, err := e.Execute(mk("r1", 0.6)); err != nil {
		t.Fatal(err)
	}
	// Replay of r1 must succeed without extra charge.
	if _, err := e.Execute(mk("r1", 0.6)); err != nil {
		t.Fatalf("replay rejected: %v", err)
	}
	_, err := e.Execute(mk("r2", 0.5))
	var dpe *Error
	errors.As(err, &dpe)
	t.Logf("input: eps=0.5 after spent=0.6 of 1.0 -> reason=%q", dpe.Reason)
	if dpe == nil || dpe.Reason != ReasonBudgetExhausted {
		t.Fatalf("want ReasonBudgetExhausted, got %v", err)
	}
	spent, _, _ := e.Ledger().Spent("ds")
	if spent != 0.6 {
		t.Fatalf("spent=%v, want 0.6 (replay not double-charged, rejection not charged)", spent)
	}
}
