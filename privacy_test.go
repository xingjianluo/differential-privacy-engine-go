package dpengine

import (
	"math"
	"testing"
)

func TestPrivacyParamsValidate(t *testing.T) {
	cases := []struct {
		name   string
		p      PrivacyParams
		budget bool
		want   ErrorCode // "" 表示合法
	}{
		{"valid pure eps", PrivacyParams{Epsilon: 1.0, Delta: 0}, false, ""},
		{"valid eps delta", PrivacyParams{Epsilon: 0.5, Delta: 1e-6}, false, ""},
		{"zero epsilon", PrivacyParams{Epsilon: 0, Delta: 0}, false, ErrInvalidParams},
		{"negative epsilon", PrivacyParams{Epsilon: -1, Delta: 0}, false, ErrInvalidParams},
		{"nan epsilon", PrivacyParams{Epsilon: math.NaN(), Delta: 0}, false, ErrInvalidParams},
		{"inf epsilon", PrivacyParams{Epsilon: math.Inf(1), Delta: 0}, false, ErrInvalidParams},
		{"negative delta", PrivacyParams{Epsilon: 1, Delta: -0.1}, false, ErrInvalidParams},
		{"delta one", PrivacyParams{Epsilon: 1, Delta: 1}, false, ErrInvalidParams},
		{"delta above one", PrivacyParams{Epsilon: 1, Delta: 1.5}, false, ErrInvalidParams},
		{"nan delta", PrivacyParams{Epsilon: 1, Delta: math.NaN()}, false, ErrInvalidParams},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Logf("input: epsilon=%v delta=%v budget=%v; rule: eps must be finite >0, delta in [0,1)",
				c.p.Epsilon, c.p.Delta, c.budget)
			var err error
			if c.budget {
				err = c.p.ValidateBudget()
			} else {
				err = c.p.Validate()
			}
			if c.want == "" {
				if err != nil {
					t.Fatalf("expected valid, got %v", err)
				}
			} else if !HasCode(err, c.want) {
				t.Fatalf("expected code %s, got %v", c.want, err)
			}
		})
	}
}

func TestSequentialComposeSums(t *testing.T) {
	parts := []PrivacyParams{{Epsilon: 0.3, Delta: 1e-7}, {Epsilon: 0.7, Delta: 2e-7}}
	got := sequentialCompose(parts)
	t.Logf("input parts=%+v; rule sequential: eps=sum, delta=sum; got eps=%v delta=%v",
		parts, got.Epsilon, got.Delta)
	if got.Epsilon != 1.0 || got.Delta != 3e-7 {
		t.Fatalf("unexpected composition: %+v", got)
	}
}

func TestParallelComposeMax(t *testing.T) {
	parts := []PrivacyParams{{Epsilon: 0.3, Delta: 1e-7}, {Epsilon: 0.7, Delta: 2e-8}}
	got := parallelCompose(parts)
	t.Logf("input parts=%+v; rule parallel (disjoint partitions): eps=max, delta=max; got eps=%v delta=%v",
		parts, got.Epsilon, got.Delta)
	if got.Epsilon != 0.7 || got.Delta != 1e-7 {
		t.Fatalf("unexpected composition: %+v", got)
	}
}

func TestFitsBudgetBoundary(t *testing.T) {
	cases := []struct {
		name               string
		usedEps, usedDelta float64
		addEps, addDelta   float64
		budEps, budDelta   float64
		want               bool
	}{
		{"exact fit", 0.6, 0, 0.4, 0, 1.0, 0, true},
		{"below budget", 0.5, 0, 0.4, 0, 1.0, 0, true},
		{"over epsilon", 0.7, 0, 0.4, 0, 1.0, 0, false},
		{"over delta", 0.1, 5e-7, 0.1, 6e-7, 1.0, 1e-6, false},
		{"exact delta fit", 0.1, 5e-7, 0.1, 5e-7, 1.0, 1e-6, true},
		{"within tolerance", 1.0, 0, 1e-13, 0, 1.0, 0, true},
		{"outside tolerance", 1.0, 0, 2e-9, 0, 1.0, 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Logf("input: used=(%v,%v) add=(%v,%v) budget=(%v,%v); rule: used+add <= budget+1e-12",
				c.usedEps, c.usedDelta, c.addEps, c.addDelta, c.budEps, c.budDelta)
			got := fitsBudget(c.usedEps, c.usedDelta, c.addEps, c.addDelta, c.budEps, c.budDelta)
			if got != c.want {
				t.Fatalf("fitsBudget=%v, want %v", got, c.want)
			}
		})
	}
}
