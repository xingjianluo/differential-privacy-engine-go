package dpengine

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func mustLedger(t *testing.T, name string, budget float64) *Ledger {
	t.Helper()
	l := NewLedger()
	if err := l.AddDataset(name, budget); err != nil {
		t.Fatalf("AddDataset: %v", err)
	}
	return l
}

func TestConsumeSequentialMonotonic(t *testing.T) {
	l := mustLedger(t, "ds", 1.0)
	for i, eps := range []float64{0.3, 0.4, 0.3} {
		c, err := l.Consume("ds", fmt.Sprintf("r%d", i), eps, SequentialComposition)
		if err != nil {
			t.Fatalf("consume %d: %v", i, err)
		}
		t.Logf("input: eps=%.2f -> spentAfter=%.4f (sequential composition)", eps, c.SpentAfter)
	}
	spent, total, err := l.Spent("ds")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("final: spent=%.4f total=%.4f", spent, total)
	if spent != 1.0 {
		t.Fatalf("want spent=1.0, got %v", spent)
	}
}

func TestConsumeBudgetExhaustedBoundary(t *testing.T) {
	l := mustLedger(t, "ds", 1.0)
	if _, err := l.Consume("ds", "r1", 0.999999, SequentialComposition); err != nil {
		t.Fatal(err)
	}
	_, err := l.Consume("ds", "r2", 0.000002, SequentialComposition)
	t.Logf("input: eps=0.000002 with remaining=0.000001 -> err=%v", err)
	var dpe *Error
	if !errors.As(err, &dpe) || dpe.Reason != ReasonBudgetExhausted {
		t.Fatalf("want ReasonBudgetExhausted, got %v", err)
	}
	// Exact-fit request must still pass.
	if _, err := l.Consume("ds", "r3", 0.000001, SequentialComposition); err != nil {
		t.Fatalf("exact-fit consume rejected: %v", err)
	}
	// Anything beyond is rejected and budget stays untouched.
	_, err = l.Consume("ds", "r4", 1e-9, SequentialComposition)
	if !errors.As(err, &dpe) || dpe.Reason != ReasonBudgetExhausted {
		t.Fatalf("want ReasonBudgetExhausted, got %v", err)
	}
	spent, _, _ := l.Spent("ds")
	if spent != 1.0 {
		t.Fatalf("rejected request changed budget: spent=%v", spent)
	}
}

func TestConsumeParallelComposition(t *testing.T) {
	l := mustLedger(t, "ds", 1.0)
	for i, eps := range []float64{0.4, 0.9, 0.9} {
		c, err := l.Consume("ds", fmt.Sprintf("p%d", i), eps, ParallelComposition)
		if err != nil {
			t.Fatalf("parallel consume %d: %v", i, err)
		}
		t.Logf("input: eps=%.2f -> spentAfter=%.4f (parallel composition charges max)", eps, c.SpentAfter)
	}
	spent, _, _ := l.Spent("ds")
	if spent != 0.9 {
		t.Fatalf("parallel composition: want spent=0.9, got %v", spent)
	}
}

func TestConsumeIdempotentReplay(t *testing.T) {
	l := mustLedger(t, "ds", 1.0)
	first, err := l.Consume("ds", "req-1", 0.5, SequentialComposition)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		c, err := l.Consume("ds", "req-1", 0.5, SequentialComposition)
		if err != nil {
			t.Fatal(err)
		}
		if c != first {
			t.Fatalf("replay returned different record: %+v vs %+v", c, first)
		}
	}
	spent, _, _ := l.Spent("ds")
	t.Logf("input: 6 identical requests req-1 eps=0.5 -> spent=%.4f (charged once)", spent)
	if spent != 0.5 {
		t.Fatalf("duplicate request double-charged: spent=%v", spent)
	}
}

func TestConsumeConcurrentNoOverspend(t *testing.T) {
	const budget = 10.0
	const workers = 64
	const perRequest = 0.1
	l := mustLedger(t, "ds", budget)

	var wg sync.WaitGroup
	var mu sync.Mutex
	succeeded := 0
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				id := fmt.Sprintf("w%d-r%d", w, i)
				_, err := l.Consume("ds", id, perRequest, SequentialComposition)
				mu.Lock()
				if err == nil {
					succeeded++
				} else {
					var dpe *Error
					if !errors.As(err, &dpe) || dpe.Reason != ReasonBudgetExhausted {
						t.Errorf("unexpected error for %s: %v", id, err)
					}
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	spent, _, _ := l.Spent("ds")
	t.Logf("input: %d workers x 10 requests of eps=%.2f, budget=%.1f -> succeeded=%d spent=%.4f",
		workers, perRequest, budget, succeeded, spent)
	if spent > budget {
		t.Fatalf("overspend under concurrency: spent=%v budget=%v", spent, budget)
	}
	if succeeded != int(budget/perRequest) {
		t.Fatalf("want exactly %d successful charges, got %d", int(budget/perRequest), succeeded)
	}
}

func TestConsumeConcurrentDuplicateRequests(t *testing.T) {
	l := mustLedger(t, "ds", 1.0)
	var wg sync.WaitGroup
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := l.Consume("ds", "shared-req", 0.5, SequentialComposition); err != nil {
				t.Errorf("duplicate request must replay, got %v", err)
			}
		}()
	}
	wg.Wait()
	spent, _, _ := l.Spent("ds")
	t.Logf("input: 32 concurrent identical requests eps=0.5 -> spent=%.4f", spent)
	if spent != 0.5 {
		t.Fatalf("concurrent duplicates double-charged: spent=%v", spent)
	}
}

func TestErrorClassification(t *testing.T) {
	l := mustLedger(t, "ds", 1.0)
	cases := []struct {
		name string
		call func() error
		want Reason
	}{
		{"unknown dataset", func() error { _, e := l.Consume("nope", "r", 0.1, SequentialComposition); return e }, ReasonDatasetNotFound},
		{"zero epsilon", func() error { _, e := l.Consume("ds", "r", 0, SequentialComposition); return e }, ReasonInvalidParameter},
		{"negative epsilon", func() error { _, e := l.Consume("ds", "r", -1, SequentialComposition); return e }, ReasonInvalidParameter},
		{"NaN epsilon", func() error { _, e := l.Consume("ds", "r", nan(), SequentialComposition); return e }, ReasonInvalidParameter},
		{"empty request id", func() error { _, e := l.Consume("ds", "", 0.1, SequentialComposition); return e }, ReasonInvalidParameter},
		{"bad composition", func() error { _, e := l.Consume("ds", "r", 0.1, Composition(99)); return e }, ReasonInvalidParameter},
		{"spent of unknown dataset", func() error { _, _, e := l.Spent("nope"); return e }, ReasonDatasetNotFound},
		{"duplicate dataset", func() error { return l.AddDataset("ds", 1.0) }, ReasonInvalidParameter},
		{"bad budget", func() error { return l.AddDataset("x", -1) }, ReasonInvalidParameter},
	}
	for _, tc := range cases {
		err := tc.call()
		var dpe *Error
		ok := errors.As(err, &dpe)
		t.Logf("input: %s -> reason=%q msg=%q", tc.name, dpe.Reason, dpe.Message)
		if !ok || dpe.Reason != tc.want {
			t.Errorf("%s: want reason %q, got %v", tc.name, tc.want, err)
		}
	}
}

func nan() float64 {
	var z float64
	return z / z
}
