package dpengine

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func wantReason(t *testing.T, err error, want Reason, context string) {
	t.Helper()
	var dpe *Error
	if !errors.As(err, &dpe) {
		t.Fatalf("%s: want *Error reason=%q, got %v", context, want, err)
	}
	t.Logf("input: %s -> reason=%q msg=%q", context, dpe.Reason, dpe.Message)
	if dpe.Reason != want {
		t.Fatalf("%s: want reason=%q, got %q (%v)", context, want, dpe.Reason, err)
	}
}

func TestReserveConfirmSettlement(t *testing.T) {
	l := mustLedger(t, "ds", 10.0)
	r, err := l.Reserve("ds", "batch-1", 4.0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input: reserve batch-1 eps=4 -> status=%s expiresIn=%v", r.Status, time.Until(r.ExpiresAt).Round(time.Second))

	view, _ := l.Budget("ds")
	if view.Spent != 0 || view.Reserved != 4 || view.Available != 6 ||
		view.Releasable != 0 || view.Expired != 0 {
		t.Fatalf("unexpected view after reserve: %+v", view)
	}
	t.Logf("view: total=%.1f spent=%.1f reserved=%.1f releasable=%.1f expired=%.1f available=%.1f",
		view.Total, view.Spent, view.Reserved, view.Releasable, view.Expired, view.Available)

	// Hold is immediately unavailable to other requests.
	if _, err := l.Consume("ds", "other", 6.000001, SequentialComposition); err == nil {
		t.Fatal("consume must be blocked by open hold")
	} else {
		wantReason(t, err, ReasonBudgetExhausted, "consume eps=6.000001 while 4 reserved of 10")
	}
	if _, err := l.Reserve("ds", "batch-x", 6.000001, time.Minute); err == nil {
		t.Fatal("reserve must be blocked by existing holds")
	} else {
		wantReason(t, err, ReasonBudgetExhausted, "reserve eps=6.000001 while 4 reserved of 10")
	}

	// Settle with actual consumption of 2.5; the unused 1.5 returns.
	got, err := l.Confirm("batch-1", 2.5)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != ReservationConfirmed || got.ConfirmedEps != 2.5 {
		t.Fatalf("confirm mismatch: %+v", got)
	}
	view, _ = l.Budget("ds")
	if view.Spent != 2.5 || view.Reserved != 0 || view.Available != 7.5 ||
		view.Releasable != 0 || view.Expired != 0 {
		t.Fatalf("view after confirm: %+v", view)
	}
	t.Logf("input: confirm batch-1 actual=2.5 -> spent=%.1f reserved=%.1f available=%.1f",
		view.Spent, view.Reserved, view.Available)
}

func TestReserveReleaseReturnsFullHold(t *testing.T) {
	l := mustLedger(t, "ds", 5.0)
	if _, err := l.Reserve("ds", "b", 3.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Consume("ds", "r1", 2.5, SequentialComposition); err == nil {
		t.Fatal("consume past hold must be rejected")
	}
	r, err := l.Release("b")
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != ReservationReleased {
		t.Fatalf("want released, got %s", r.Status)
	}
	view, _ := l.Budget("ds")
	if view.Spent != 0 || view.Reserved != 0 || view.Available != 5 {
		t.Fatalf("view after release: %+v", view)
	}
	// Previously blocked request now fits.
	if _, err := l.Consume("ds", "r1", 2.5, SequentialComposition); err != nil {
		t.Fatalf("consume after release must succeed: %v", err)
	}
	t.Logf("input: release b then consume 2.5 -> spent=%.1f available=%.1f", view.Spent, view.Available)
}

func TestConfirmReleaseOnlyOnce(t *testing.T) {
	l := mustLedger(t, "ds", 5.0)
	if _, err := l.Reserve("ds", "b", 2.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Confirm("b", 1.0); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Confirm("b", 1.0); err == nil {
		t.Fatal("second confirm must be rejected")
	} else {
		wantReason(t, err, ReasonInvalidState, "second confirm of settled batch")
	}
	if _, err := l.Release("b"); err == nil {
		t.Fatal("release after confirm must be rejected")
	} else {
		wantReason(t, err, ReasonInvalidState, "release after confirm")
	}

	l2 := mustLedger(t, "d2", 5.0)
	if _, err := l2.Reserve("d2", "b2", 2.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := l2.Release("b2"); err != nil {
		t.Fatal(err)
	}
	if _, err := l2.Release("b2"); err == nil {
		t.Fatal("second release must be rejected")
	} else {
		wantReason(t, err, ReasonInvalidState, "second release")
	}
	if _, err := l2.Confirm("b2", 0); err == nil {
		t.Fatal("confirm after release must be rejected")
	} else {
		wantReason(t, err, ReasonInvalidState, "confirm after release")
	}
	view, _ := l2.Budget("d2")
	if view.Spent != 0 || view.Reserved != 0 || view.Available != 5 {
		t.Fatalf("settled batch must not move budget twice: %+v", view)
	}
}

func TestReserveReplayIdempotent(t *testing.T) {
	l := mustLedger(t, "ds", 10.0)
	first, err := l.Reserve("ds", "dup", 3.0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		r, err := l.Reserve("ds", "dup", 3.0, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if r != first {
			t.Fatalf("replay mismatch:\n%+v\n%+v", r, first)
		}
	}
	view, _ := l.Budget("ds")
	if view.Reserved != 3 {
		t.Fatalf("replayed reserve held budget multiple times: %+v", view)
	}
	t.Logf("input: 6 identical reserve calls -> reserved=%.1f (held once)", view.Reserved)
}

func TestReserveExactBoundary(t *testing.T) {
	l := mustLedger(t, "ds", 1.0)
	if _, err := l.Consume("ds", "r1", 0.6, SequentialComposition); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Reserve("ds", "b1", 0.4, time.Minute); err != nil {
		t.Fatalf("exact-fit reserve rejected: %v", err)
	}
	_, err := l.Reserve("ds", "b2", 1e-9, time.Minute)
	wantReason(t, err, ReasonBudgetExhausted, "reserve 1e-9 with zero availability")
	if _, err := l.Consume("ds", "r2", 1e-9, SequentialComposition); err == nil {
		t.Fatal("direct consume must also be blocked by the exact-fit hold")
	}
	// Confirming below the hold frees the difference immediately.
	if _, err := l.Confirm("b1", 0.3); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Reserve("ds", "b3", 0.1, time.Minute); err != nil {
		t.Fatalf("reserve of freed remainder must fit: %v", err)
	}
	view, _ := l.Budget("ds")
	if !approx(view.Spent, 0.9) || !approx(view.Reserved, 0.1) || !approx(view.Available, 0) {
		t.Fatalf("partial confirm accounting wrong: %+v", view)
	}
}

func approx(a, b float64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d < 1e-9
}

func TestConfirmAboveHoldRejected(t *testing.T) {
	l := mustLedger(t, "ds", 5.0)
	if _, err := l.Reserve("ds", "b", 2.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	_, err := l.Confirm("b", 2.000001)
	wantReason(t, err, ReasonBudgetExhausted, "confirm 2.000001 against 2.0 hold")
	// Batch must remain open and its hold intact.
	r, err := l.GetReservation("b")
	if err != nil || r.Status != ReservationReserved {
		t.Fatalf("failed confirm must leave batch reserved: %+v err=%v", r, err)
	}
	view, _ := l.Budget("ds")
	if view.Reserved != 2 || view.Spent != 0 {
		t.Fatalf("failed confirm changed budget: %+v", view)
	}
}

func TestReserveErrorClassification(t *testing.T) {
	l := mustLedger(t, "ds", 1.0)
	cases := []struct {
		name string
		call func() error
		want Reason
	}{
		{"empty batch id", func() error { _, e := l.Reserve("ds", "", 0.1, time.Minute); return e }, ReasonInvalidParameter},
		{"zero epsilon", func() error { _, e := l.Reserve("ds", "b", 0, time.Minute); return e }, ReasonInvalidParameter},
		{"negative epsilon", func() error { _, e := l.Reserve("ds", "b", -1, time.Minute); return e }, ReasonInvalidParameter},
		{"NaN epsilon", func() error { _, e := l.Reserve("ds", "b", nan(), time.Minute); return e }, ReasonInvalidParameter},
		{"zero ttl", func() error { _, e := l.Reserve("ds", "b", 0.1, 0); return e }, ReasonInvalidParameter},
		{"unknown dataset", func() error { _, e := l.Reserve("nope", "b", 0.1, time.Minute); return e }, ReasonDatasetNotFound},
		{"confirm unknown batch", func() error { _, e := l.Confirm("nope", 0.1); return e }, ReasonBatchNotFound},
		{"release unknown batch", func() error { _, e := l.Release("nope"); return e }, ReasonBatchNotFound},
		{"get unknown batch", func() error { _, e := l.GetReservation("nope"); return e }, ReasonBatchNotFound},
		{"confirm negative actual", func() error {
			_, _ = l.Reserve("ds", "b1", 0.1, time.Minute)
			_, e := l.Confirm("b1", -0.1)
			return e
		}, ReasonInvalidParameter},
		{"budget of unknown dataset", func() error { _, e := l.Budget("nope"); return e }, ReasonDatasetNotFound},
	}
	for _, tc := range cases {
		err := tc.call()
		wantReason(t, err, tc.want, tc.name)
	}
}

// --- concurrency -----------------------------------------------------------

func TestConcurrentConfirmOnlyOneSettles(t *testing.T) {
	l := mustLedger(t, "ds", 10.0)
	if _, err := l.Reserve("ds", "b", 4.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	const n = 64
	var wg sync.WaitGroup
	var okMu sync.Mutex
	confirmed, released := 0, 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				if _, err := l.Confirm("b", 3.0); err == nil {
					okMu.Lock()
					confirmed++
					okMu.Unlock()
				}
			} else {
				if _, err := l.Release("b"); err == nil {
					okMu.Lock()
					released++
					okMu.Unlock()
				}
			}
		}(i)
	}
	wg.Wait()
	t.Logf("input: %d concurrent confirm/release on one batch -> confirmed=%d released=%d", n, confirmed, released)
	if confirmed+released != 1 {
		t.Fatalf("exactly one settlement must win, got confirmed=%d released=%d", confirmed, released)
	}
	view, _ := l.Budget("ds")
	if confirmed == 1 {
		if view.Spent != 3 || view.Reserved != 0 || view.Available != 7 {
			t.Fatalf("winning confirm settled wrong amounts: %+v", view)
		}
	} else {
		if view.Spent != 0 || view.Reserved != 0 || view.Available != 10 {
			t.Fatalf("winning release freed wrong amounts: %+v", view)
		}
	}
}

func TestConcurrentReserveSameBatchHoldsOnce(t *testing.T) {
	l := mustLedger(t, "ds", 10.0)
	const n = 32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _ = l.Reserve("ds", "shared", 4.0, time.Minute)
		}()
	}
	close(start)
	wg.Wait()
	view, _ := l.Budget("ds")
	t.Logf("input: %d concurrent identical reserve calls -> reserved=%.1f", n, view.Reserved)
	if view.Reserved != 4 || view.Available != 6 {
		t.Fatalf("concurrent duplicate reserve held multiple times: %+v", view)
	}
}

func TestConcurrentReserveNoOverspend(t *testing.T) {
	l := mustLedger(t, "ds", 10.0)
	const workers = 32
	const each = 0.5
	var wg sync.WaitGroup
	var mu sync.Mutex
	held := 0
	var heldIDs []string
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			id := fmt.Sprintf("b-%d", w)
			_, err := l.Reserve("ds", id, each, time.Minute)
			mu.Lock()
			if err == nil {
				held++
				heldIDs = append(heldIDs, id)
			} else {
				var dpe *Error
				if !errors.As(err, &dpe) || dpe.Reason != ReasonBudgetExhausted {
					t.Errorf("unexpected err: %v", err)
				}
			}
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	view, _ := l.Budget("ds")
	t.Logf("input: %d workers each reserve %.1f of 10 -> held=%d reserved=%.1f",
		workers, each, held, view.Reserved)
	if view.Reserved > 10+1e-9 {
		t.Fatalf("over-reservation: %+v", view)
	}
	if held != 20 {
		t.Fatalf("want exactly 20 holds, got %d", held)
	}
	// Settle half of the winning batches at zero consumption; freed
	// capacity becomes reusable.
	for i := 0; i < 10; i++ {
		if _, err := l.Confirm(heldIDs[i], 0); err != nil {
			t.Fatal(err)
		}
	}
	view, _ = l.Budget("ds")
	if view.Spent != 0 || view.Reserved != 5 || view.Available != 5 {
		t.Fatalf("zero-consumption confirms must release remainder: %+v", view)
	}
}

// --- expiry ----------------------------------------------------------------

func TestExpireReservationsSweep(t *testing.T) {
	base := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	l := mustLedger(t, "ds", 10.0)
	l.now = func() time.Time { return base }

	if _, err := l.Reserve("ds", "short", 2.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Reserve("ds", "long", 3.0, 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Consume("ds", "r1", 5.0, SequentialComposition); err != nil {
		t.Fatal(err)
	}

	// Nothing due yet.
	got, err := l.ExpireReservations()
	if err != nil || len(got) != 0 {
		t.Fatalf("premature sweep: %d reclaimed err=%v", len(got), err)
	}

	l.now = func() time.Time { return base.Add(60 * time.Second) }
	got, err = l.ExpireReservations()
	if err != nil || len(got) != 1 || got[0].BatchID != "short" {
		t.Fatalf("want only short expired, got %+v err=%v", got, err)
	}
	if got[0].Status != ReservationExpired {
		t.Fatalf("want expired status, got %s", got[0].Status)
	}
	view, _ := l.Budget("ds")
	if view.Reserved != 3 || view.Available != 2 {
		t.Fatalf("expiry freed wrong amounts: %+v", view)
	}
	t.Logf("input: t+60s sweep -> expired=%s reserved=%.1f available=%.1f",
		got[0].BatchID, view.Reserved, view.Available)

	// Expired batch is terminal: confirm/release rejected as expired,
	// sweep idempotent.
	if _, err := l.Confirm("short", 1.0); err == nil {
		t.Fatal("confirm of expired batch must be rejected")
	} else {
		wantReason(t, err, ReasonReservationExpired, "confirm swept-expired batch")
	}
	if _, err := l.Release("short"); err == nil {
		t.Fatal("release of expired batch must be rejected")
	} else {
		wantReason(t, err, ReasonReservationExpired, "release swept-expired batch")
	}
	got2, _ := l.ExpireReservations()
	if len(got2) != 0 {
		t.Fatalf("repeated sweep must be idempotent, got %d", len(got2))
	}

	// Exactly at the deadline the long hold is due.
	l.now = func() time.Time { return base.Add(10 * time.Minute) }
	got3, _ := l.ExpireReservations()
	if len(got3) != 1 || got3[0].BatchID != "long" {
		t.Fatalf("deadline sweep mismatch: %+v", got3)
	}
	view, _ = l.Budget("ds")
	if view.Reserved != 0 || view.Available != 5 {
		t.Fatalf("after full sweep: %+v", view)
	}
	// Reclaimed budget is usable again.
	if _, err := l.Reserve("ds", "new", 5.0, time.Minute); err != nil {
		t.Fatalf("reclaimed budget must be reusable: %v", err)
	}
}

func TestExpiryIsAuditable(t *testing.T) {
	base := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	l := mustLedger(t, "ds", 10.0)
	l.now = func() time.Time { return base }
	if _, err := l.Reserve("ds", "b", 1.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	l.now = func() time.Time { return base.Add(time.Minute) }
	if _, err := l.ExpireReservations(); err != nil {
		t.Fatal(err)
	}
	hist := l.BatchHistory()
	if len(hist) != 2 || hist[0].Kind != ReservationReserved || hist[1].Kind != ReservationExpired {
		t.Fatalf("audit trail mismatch: %+v", hist)
	}
	if hist[1].Amount != 1 || !hist[1].At.Equal(base.Add(time.Minute)) {
		t.Fatalf("expiry event mismatch: %+v", hist[1])
	}
	t.Logf("input: reserve then sweep -> events=[%s %.1f @%v, %s %.1f @%v]",
		hist[0].Kind, hist[0].Amount, hist[0].At.Format(time.RFC3339),
		hist[1].Kind, hist[1].Amount, hist[1].At.Format(time.RFC3339))
}
