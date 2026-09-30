package dpengine

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newFakeLedger(t *testing.T, name string, budget float64) (*Ledger, *fakeClock) {
	t.Helper()
	clock := &fakeClock{t: time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)}
	l := withClock(clock.now)
	if err := l.AddDataset(name, budget); err != nil {
		t.Fatal(err)
	}
	return l, clock
}

func TestReserveHoldsBudgetImmediately(t *testing.T) {
	l, _ := newFakeLedger(t, "ds", 1.0)
	r, err := l.Reserve("ds", "b1", 0.6, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input: reserve batch=b1 eps=0.6 ttl=1m -> status=%s expires=%s", r.Status, r.ExpiresAt.Format(time.RFC3339))

	b, err := l.Budget("ds")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("budget: total=%.2f spent=%.2f reserved=%.2f releasable=%.2f available=%.2f",
		b.Total, b.Spent, b.Reserved, b.Releasable, b.Available)
	if b.Spent != 0 || b.Reserved != 0.6 || b.Releasable != 0 || b.Available != 0.4 {
		t.Fatalf("unexpected breakdown: %+v", b)
	}

	// The hold blocks other requests immediately: 0.5 would exceed the
	// remaining 0.4, even though spent is still 0.
	if _, err := l.Consume("ds", "r1", 0.5, SequentialComposition); err == nil {
		t.Fatal("consume of 0.5 must be rejected while 0.6 is held")
	} else {
		var dpe *Error
		errors.As(err, &dpe)
		t.Logf("input: direct consume eps=0.5 with hold=0.6 -> reason=%q", dpe.Reason)
		if dpe.Reason != ReasonBudgetExhausted {
			t.Fatalf("want budget_exhausted, got %v", err)
		}
	}
	// Exact-fit consume against the non-held remainder passes.
	if _, err := l.Consume("ds", "r2", 0.4, SequentialComposition); err != nil {
		t.Fatalf("exact-fit consume under hold rejected: %v", err)
	}
	b, _ = l.Budget("ds")
	if b.Available != 0 || b.Spent != 0.4 {
		t.Fatalf("breakdown after exact-fit consume: %+v", b)
	}
}

func TestReserveInsufficientAndReplay(t *testing.T) {
	l, _ := newFakeLedger(t, "ds", 1.0)
	if _, err := l.Reserve("ds", "b1", 0.7, time.Minute); err != nil {
		t.Fatal(err)
	}
	_, err := l.Reserve("ds", "b2", 0.4, time.Minute)
	var dpe *Error
	if !errors.As(err, &dpe) || dpe.Reason != ReasonBudgetExhausted {
		t.Fatalf("want budget_exhausted, got %v", err)
	}
	t.Logf("input: reserve 0.4 with held=0.7 spent=0 budget=1 -> reason=%q", dpe.Reason)

	// Replaying b1 with any parameters returns the original hold, never a
	// second hold and never an error.
	first, _ := l.Reservation("b1")
	for i := 0; i < 3; i++ {
		r, err := l.Reserve("ds", "b1", 9.9, 9*time.Second)
		if err != nil {
			t.Fatalf("reserve replay must succeed: %v", err)
		}
		if !reflect.DeepEqual(r, first) {
			t.Fatalf("replay changed reservation: %+v vs %+v", r, first)
		}
	}
	b, _ := l.Budget("ds")
	if b.Reserved != 0.7 {
		t.Fatalf("replay double-held budget: %+v", b)
	}
}

func TestConfirmChargesActualAndReturnsRemainder(t *testing.T) {
	l, _ := newFakeLedger(t, "ds", 1.0)
	if _, err := l.Reserve("ds", "b1", 0.6, time.Minute); err != nil {
		t.Fatal(err)
	}
	r, err := l.Confirm("b1", 0.35)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input: confirm b1 actual=0.35 of reserved=0.6 -> status=%s", r.Status)
	if r.Status != ReservationConfirmed || r.Actual != 0.35 {
		t.Fatalf("bad confirm result: %+v", r)
	}
	b, _ := l.Budget("ds")
	t.Logf("budget: spent=%.2f reserved=%.2f available=%.2f", b.Spent, b.Reserved, b.Available)
	if b.Spent != 0.35 || b.Reserved != 0 || b.Available != 0.65 {
		t.Fatalf("remainder not returned: %+v", b)
	}

	// Repeat confirm is idempotent: same record, no second charge.
	for i := 0; i < 3; i++ {
		r2, err := l.Confirm("b1", 9.0)
		if err != nil {
			t.Fatalf("repeated confirm must replay, got %v", err)
		}
		if !reflect.DeepEqual(r2, r) {
			t.Fatalf("repeated confirm changed record: %+v vs %+v", r2, r)
		}
	}
	b, _ = l.Budget("ds")
	if b.Spent != 0.35 {
		t.Fatalf("repeated confirm double-charged: spent=%v", b.Spent)
	}
	// Direct Consume replay by the same batch id also never charges twice.
	if _, err := l.Consume("ds", "b1", 0.35, SequentialComposition); err != nil {
		t.Fatal(err)
	}
	b, _ = l.Budget("ds")
	if b.Spent != 0.35 {
		t.Fatalf("consume replay double-charged: spent=%v", b.Spent)
	}

	if len(r.Events) != 2 || r.Events[0].Kind != kindReserved || r.Events[1].Kind != kindConfirmed {
		t.Fatalf("audit trail wrong: %+v", r.Events)
	}
}

func TestConfirmZeroActualReturnsWholeHold(t *testing.T) {
	l, _ := newFakeLedger(t, "ds", 1.0)
	if _, err := l.Reserve("ds", "b1", 0.6, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Confirm("b1", 0); err != nil {
		t.Fatal(err)
	}
	b, _ := l.Budget("ds")
	if b.Spent != 0 || b.Reserved != 0 || b.Available != 1.0 {
		t.Fatalf("zero-actual confirm: %+v", b)
	}
}

func TestReleaseReturnsFullHoldAndIsIdempotent(t *testing.T) {
	l, _ := newFakeLedger(t, "ds", 1.0)
	if _, err := l.Reserve("ds", "b1", 0.6, time.Minute); err != nil {
		t.Fatal(err)
	}
	r, err := l.Release("b1")
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != ReservationReleased {
		t.Fatalf("want released, got %s", r.Status)
	}
	for i := 0; i < 3; i++ {
		r2, err := l.Release("b1")
		if err != nil || !reflect.DeepEqual(r2, r) {
			t.Fatalf("repeated release must replay same record: %+v err=%v", r2, err)
		}
	}
	b, _ := l.Budget("ds")
	t.Logf("input: release b1 hold=0.6 x4 -> spent=%.2f reserved=%.2f available=%.2f",
		b.Spent, b.Reserved, b.Available)
	if b.Spent != 0 || b.Reserved != 0 || b.Available != 1.0 {
		t.Fatalf("release did not return full hold: %+v", b)
	}
}

func TestSettlementStateTransitionsRejected(t *testing.T) {
	l, _ := newFakeLedger(t, "ds", 1.0)

	expectReason := func(name string, want Reason, fn func() error) {
		err := fn()
		var dpe *Error
		if !errors.As(err, &dpe) || dpe.Reason != want {
			t.Errorf("%s: want %q, got %v", name, want, err)
		} else {
			t.Logf("input: %s -> reason=%q", name, dpe.Reason)
		}
	}

	expectReason("confirm unknown batch", ReasonBatchNotFound, func() error {
		_, e := l.Confirm("nope", 0.1)
		return e
	})
	expectReason("release unknown batch", ReasonBatchNotFound, func() error {
		_, e := l.Release("nope")
		return e
	})
	if _, err := l.Reservation("nope"); err == nil {
		t.Fatal("Reservation of unknown batch must fail")
	}

	if _, err := l.Reserve("ds", "b1", 0.6, time.Minute); err != nil {
		t.Fatal(err)
	}
	expectReason("confirm actual > reserved", ReasonInvalidParameter, func() error {
		_, e := l.Confirm("b1", 0.61)
		return e
	})
	expectReason("reserve bad ttl", ReasonInvalidParameter, func() error {
		_, e := l.Reserve("ds", "bX", 0.1, 0)
		return e
	})
	expectReason("reserve on unknown dataset", ReasonDatasetNotFound, func() error {
		_, e := l.Reserve("nope", "bX", 0.1, time.Minute)
		return e
	})

	if _, err := l.Confirm("b1", 0.6); err != nil {
		t.Fatal(err)
	}
	expectReason("release confirmed batch", ReasonInvalidState, func() error {
		_, e := l.Release("b1")
		return e
	})

	if _, err := l.Reserve("ds", "b2", 0.2, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Release("b2"); err != nil {
		t.Fatal(err)
	}
	expectReason("confirm released batch", ReasonInvalidState, func() error {
		_, e := l.Confirm("b2", 0.1)
		return e
	})
}

func TestConcurrentRepeatedConfirmChargesOnce(t *testing.T) {
	l, _ := newFakeLedger(t, "ds", 1.0)
	if _, err := l.Reserve("ds", "b1", 0.5, time.Minute); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := l.Confirm("b1", 0.5)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent confirm must all replay, got %v", err)
		}
	}
	b, _ := l.Budget("ds")
	t.Logf("input: 32 concurrent confirms of b1 actual=0.5 -> spent=%.4f reserved=%.4f",
		b.Spent, b.Reserved)
	if b.Spent != 0.5 || b.Reserved != 0 {
		t.Fatalf("concurrent confirm charged more than once: %+v", b)
	}
}

func TestConcurrentRepeatedReleaseReturnsOnce(t *testing.T) {
	l, _ := newFakeLedger(t, "ds", 1.0)
	if _, err := l.Reserve("ds", "b1", 0.5, time.Minute); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := l.Release("b1"); err != nil {
				t.Errorf("concurrent release: %v", err)
			}
		}()
	}
	wg.Wait()
	b, _ := l.Budget("ds")
	t.Logf("input: 32 concurrent releases of b1 hold=0.5 -> spent=%.4f reserved=%.4f available=%.4f",
		b.Spent, b.Reserved, b.Available)
	if b.Spent != 0 || b.Reserved != 0 || b.Available != 1.0 {
		t.Fatalf("concurrent release settled wrong: %+v", b)
	}
}

func TestConcurrentConfirmOrReleaseExactlyOneOutcome(t *testing.T) {
	l, _ := newFakeLedger(t, "ds", 1.0)
	if _, err := l.Reserve("ds", "b1", 0.5, time.Minute); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	confirmed, released := 0, 0
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				_, err := l.Confirm("b1", 0.5)
				mu.Lock()
				if err == nil {
					confirmed++
				}
				mu.Unlock()
			} else {
				_, err := l.Release("b1")
				mu.Lock()
				if err == nil {
					released++
				}
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	t.Logf("input: 32 confirms vs 32 releases racing on b1 -> confirm-ok=%d release-ok=%d",
		confirmed, released)
	if !((confirmed > 0 && released == 0) || (released > 0 && confirmed == 0)) {
		t.Fatalf("exactly one settlement kind may win, got confirm=%d release=%d", confirmed, released)
	}
	r, _ := l.Reservation("b1")
	b, _ := l.Budget("ds")
	switch r.Status {
	case ReservationConfirmed:
		if b.Spent != 0.5 {
			t.Fatalf("confirmed race: want spent=0.5, got %v", b.Spent)
		}
	case ReservationReleased:
		if b.Spent != 0 || b.Available != 1.0 {
			t.Fatalf("released race: want spent=0 available=1, got %+v", b)
		}
	default:
		t.Fatalf("batch not settled: %s", r.Status)
	}
}

func TestExpiryReclaimDeterministic(t *testing.T) {
	l, clock := newFakeLedger(t, "ds", 1.0)
	r, err := l.Reserve("ds", "b1", 0.6, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	deadline := r.ExpiresAt

	// One second before the deadline: nothing due.
	clock.advance(59 * time.Second)
	if got := l.ReclaimExpired(); len(got) != 0 {
		t.Fatalf("nothing must be reclaimed before deadline, got %+v", got)
	}

	// At/after deadline the read-only Budget view flags the hold as
	// releasable but it is still held (reclaim is an explicit settlement).
	clock.advance(time.Second) // now == ExpiresAt
	b, _ := l.Budget("ds")
	t.Logf("input: now=%s deadline=%s -> reserved=%.2f releasable=%.2f available=%.2f",
		clock.now().Format(time.RFC3339), deadline.Format(time.RFC3339),
		b.Reserved, b.Releasable, b.Available)
	if b.Reserved != 0.6 || b.Releasable != 0.6 || b.Available != 0.4 {
		t.Fatalf("overdue hold must stay held until reclaimed: %+v", b)
	}

	reclaimed := l.ReclaimExpired()
	if len(reclaimed) != 1 || reclaimed[0].BatchID != "b1" ||
		reclaimed[0].Status != ReservationExpired {
		t.Fatalf("bad reclaim result: %+v", reclaimed)
	}
	// A second sweep settles nothing (reclaim is itself idempotent).
	if got := l.ReclaimExpired(); len(got) != 0 {
		t.Fatalf("repeat sweep reclaimed again: %+v", got)
	}
	b, _ = l.Budget("ds")
	t.Logf("input: ReclaimExpired -> reserved=%.2f releasable=%.2f available=%.2f",
		b.Reserved, b.Releasable, b.Available)
	if b.Spent != 0 || b.Reserved != 0 || b.Releasable != 0 || b.Available != 1.0 {
		t.Fatalf("budget not restored after reclaim: %+v", b)
	}
	got, _ := l.Reservation("b1")
	if len(got.Events) != 2 || got.Events[1].Kind != kindExpired {
		t.Fatalf("missing expiry audit event: %+v", got.Events)
	}
}

func TestLateSettlementAfterExpiryRejectedAndReclaimed(t *testing.T) {
	l, clock := newFakeLedger(t, "ds", 1.0)
	if _, err := l.Reserve("ds", "b1", 0.6, time.Minute); err != nil {
		t.Fatal(err)
	}
	clock.advance(61 * time.Second)

	if _, err := l.Confirm("b1", 0.1); !isReason(err, ReasonInvalidState) {
		t.Fatalf("late confirm: want invalid_state, got %v", err)
	}
	r, _ := l.Reservation("b1")
	if r.Status != ReservationExpired {
		t.Fatalf("late confirm must settle batch as expired, got %s", r.Status)
	}
	b, _ := l.Budget("ds")
	if b.Reserved != 0 || b.Spent != 0 || b.Available != 1.0 {
		t.Fatalf("late confirm must not charge and must free hold: %+v", b)
	}
}

func TestReserveLazilyReclaimsExpiredHold(t *testing.T) {
	l, clock := newFakeLedger(t, "ds", 1.0)
	if _, err := l.Reserve("ds", "old", 0.6, time.Minute); err != nil {
		t.Fatal(err)
	}
	clock.advance(61 * time.Second)
	// The old hold is overdue; a new 0.6 reservation must succeed once the
	// deterministic reclaim frees it (without the reclaim it would fail).
	r, err := l.Reserve("ds", "new", 0.6, time.Minute)
	if err != nil {
		t.Fatalf("new reservation after expiry rejected: %v", err)
	}
	if r.Status != ReservationPending {
		t.Fatalf("new reservation: %+v", r)
	}
	old, _ := l.Reservation("old")
	if old.Status != ReservationExpired {
		t.Fatalf("old batch must be reclaimed, got %s", old.Status)
	}
	b, _ := l.Budget("ds")
	if b.Reserved != 0.6 || b.Spent != 0 || b.Available != 0.4 {
		t.Fatalf("breakdown after lazy reclaim: %+v", b)
	}
}

func TestReserveExactFitBoundary(t *testing.T) {
	l, _ := newFakeLedger(t, "ds", 1.0)
	if _, err := l.Reserve("ds", "b1", 0.5, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Reserve("ds", "b2", 0.5, time.Minute); err != nil {
		t.Fatalf("exact-fit reservation rejected: %v", err)
	}
	_, err := l.Reserve("ds", "b3", 1e-9, time.Minute)
	if !isReason(err, ReasonBudgetExhausted) {
		t.Fatalf("over-fit reservation: want budget_exhausted, got %v", err)
	}
	b, _ := l.Budget("ds")
	if b.Reserved != 1.0 || b.Available != 0 {
		t.Fatalf("rejected reservation changed holds: %+v", b)
	}
}

func TestConcurrentReservesAndConfirmsNeverOverspend(t *testing.T) {
	const budget = 10.0
	l, _ := newFakeLedger(t, "ds", budget)
	const workers = 32
	var wg sync.WaitGroup
	var mu sync.Mutex
	confirmed := 0
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				id := fmt.Sprintf("w%d-b%d", w, i)
				r, err := l.Reserve("ds", id, 0.1, time.Minute)
				if err != nil {
					var dpe *Error
					if !errors.As(err, &dpe) || dpe.Reason != ReasonBudgetExhausted {
						t.Errorf("unexpected reserve error: %v", err)
					}
					continue
				}
				if _, err := l.Confirm(id, r.Amount); err != nil {
					t.Errorf("confirm %s: %v", id, err)
					continue
				}
				mu.Lock()
				confirmed++
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	b, _ := l.Budget("ds")
	t.Logf("input: %d workers reserve+confirm eps=0.1 budget=%.0f -> confirmed=%d spent=%.12f reserved=%.4f",
		workers, budget, confirmed, b.Spent, b.Reserved)
	if b.Spent+b.Reserved > budget+1e-9 {
		t.Fatalf("invariant broken: spent+reserved=%.6f > budget=%.6f", b.Spent+b.Reserved, budget)
	}
	if math.Abs(b.Spent-budget) > 1e-9 || confirmed != 100 {
		t.Fatalf("want exactly 100 settlements and spent=10, got confirmed=%d spent=%v", confirmed, b.Spent)
	}
}

func TestRestartRestoresPendingHold(t *testing.T) {
	l, clock := newFakeLedger(t, "ds", 1.0)
	if _, err := l.Consume("ds", "r0", 0.2, SequentialComposition); err != nil {
		t.Fatal(err)
	}
	pre, err := l.Reserve("ds", "b1", 0.6, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := SaveSnapshot(&buf, l); err != nil {
		t.Fatal(err)
	}
	restored, err := LoadSnapshot(&buf)
	if err != nil {
		t.Fatal(err)
	}
	restored.now = clock.now // continue the same timeline

	b, _ := restored.Budget("ds")
	t.Logf("input: restart with pending hold -> spent=%.2f reserved=%.2f available=%.2f",
		b.Spent, b.Reserved, b.Available)
	if b.Spent != 0.2 || b.Reserved != 0.6 || math.Abs(b.Available-0.2) > 1e-9 {
		t.Fatalf("pending hold lost or altered across restart: %+v", b)
	}
	r, err := restored.Reservation("b1")
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != ReservationPending || !r.ExpiresAt.Equal(pre.ExpiresAt) || r.Amount != 0.6 {
		t.Fatalf("restored hold mismatch: %+v vs %+v", r, pre)
	}
	// It still blocks other requests after restart.
	if _, err := restored.Consume("ds", "r1", 0.3, SequentialComposition); !isReason(err, ReasonBudgetExhausted) {
		t.Fatalf("restored hold must still block budget: got %v", err)
	}
	// Replaying Reserve with the same batch id does not hold a second time.
	rp, err := restored.Reserve("ds", "b1", 0.6, time.Minute)
	if err != nil || rp.Amount != 0.6 {
		t.Fatalf("reserve replay after restart: %+v err=%v", rp, err)
	}
	b, _ = restored.Budget("ds")
	if b.Reserved != 0.6 {
		t.Fatalf("reserve replay after restart double-held: %+v", b)
	}
	// The original deadline still governs reclaim after restart.
	clock.advance(61 * time.Second)
	reclaimed := restored.ReclaimExpired()
	if len(reclaimed) != 1 || reclaimed[0].Status != ReservationExpired {
		t.Fatalf("restored hold not reclaimed by its original deadline: %+v", reclaimed)
	}
	b, _ = restored.Budget("ds")
	if b.Reserved != 0 || b.Available != 0.8 {
		t.Fatalf("budget after post-restart reclaim wrong: %+v", b)
	}
}

func TestRestartConfirmedBatchReplaysOnce(t *testing.T) {
	l, _ := newFakeLedger(t, "ds", 1.0)
	if _, err := l.Reserve("ds", "b1", 0.6, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Confirm("b1", 0.4); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := SaveSnapshot(&buf, l); err != nil {
		t.Fatal(err)
	}
	restored, err := LoadSnapshot(&buf)
	if err != nil {
		t.Fatal(err)
	}
	// Repeat Confirm / Consume replay after restart: no second charge.
	for i := 0; i < 2; i++ {
		r, err := restored.Confirm("b1", 9.0)
		if err != nil || r.Status != ReservationConfirmed || r.Actual != 0.4 {
			t.Fatalf("confirm replay after restart: %+v err=%v", r, err)
		}
	}
	if _, err := restored.Consume("ds", "b1", 0.4, SequentialComposition); err != nil {
		t.Fatal(err)
	}
	b, _ := restored.Budget("ds")
	t.Logf("input: restart after confirm, replay confirm+consume x2 -> spent=%.4f", b.Spent)
	if b.Spent != 0.4 || b.Reserved != 0 || b.Available != 0.6 {
		t.Fatalf("settlement changed across restart: %+v", b)
	}
}

func TestRestartZeroActualConfirmReplays(t *testing.T) {
	l, _ := newFakeLedger(t, "ds", 1.0)
	if _, err := l.Reserve("ds", "b1", 0.3, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Confirm("b1", 0); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := SaveSnapshot(&buf, l); err != nil {
		t.Fatal(err)
	}
	restored, err := LoadSnapshot(&buf)
	if err != nil {
		t.Fatal(err)
	}
	r, err := restored.Confirm("b1", 0)
	if err != nil || r.Status != ReservationConfirmed {
		t.Fatalf("zero-actual replay after restart: %+v err=%v", r, err)
	}
	// Repeated replay still settles at most once.
	r2, err := restored.Confirm("b1", 0)
	if err != nil || !reflect.DeepEqual(r2, r) {
		t.Fatalf("zero-actual replay not stable: %+v err=%v", r2, err)
	}
	b, _ := restored.Budget("ds")
	if b.Spent != 0 || b.Reserved != 0 || b.Available != 1.0 {
		t.Fatalf("zero-actual settlement altered across restart: %+v", b)
	}
}

func TestLoadSnapshotLegacyHasNoReservations(t *testing.T) {
	// Both v1 (pre-feature) and v0 (missing version) snapshots load with
	// safe defaults: spent preserved, reservation table empty.
	for _, data := range []string{
		`{"version":1,"datasets":[{"Name":"ds","TotalEpsilon":2.0,"SpentEpsilon":0.75,
			"ConsumedUnits":[{"RequestID":"a","Epsilon":0.75,"Composition":0,"SpentAfter":0.75}]}]}`,
		`{"datasets":[{"Name":"ds","TotalEpsilon":2.0,
			"ConsumedUnits":[{"RequestID":"a","Epsilon":0.75,"Composition":0,"SpentAfter":0.75}]}]}`,
	} {
		l, err := LoadSnapshot(strings.NewReader(data))
		if err != nil {
			t.Fatalf("legacy snapshot rejected: %v", err)
		}
		b, err := l.Budget("ds")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("input: legacy snapshot -> spent=%.2f reserved=%.2f available=%.2f",
			b.Spent, b.Reserved, b.Available)
		if b.Spent != 0.75 || b.Reserved != 0 || b.Available != 1.25 {
			t.Fatalf("legacy defaults wrong: %+v", b)
		}
		// Ledger remains fully usable: reservation works from a clean slate.
		if _, err := l.Reserve("ds", "b1", 0.5, time.Minute); err != nil {
			t.Fatalf("reserve after legacy load failed: %v", err)
		}
	}
}

func TestLoadSnapshotV2CorruptRejected(t *testing.T) {
	base := func(res string) string {
		return `{"version":2,"datasets":[{"Name":"ds","TotalEpsilon":1.0}],
			"reservations":[` + res + `]}`
	}
	cases := []struct {
		name string
		data string
		want Reason
	}{
		{"empty batch id", base(`{"BatchID":"","Dataset":"ds","Amount":0.1,"Status":"pending"}`), ReasonCorruptSnapshot},
		{"unknown dataset", base(`{"BatchID":"b","Dataset":"x","Amount":0.1,"Status":"pending"}`), ReasonCorruptSnapshot},
		{"bad status", base(`{"BatchID":"b","Dataset":"ds","Amount":0.1,"Status":"weird"}`), ReasonCorruptSnapshot},
		{"negative amount", base(`{"BatchID":"b","Dataset":"ds","Amount":-1,"Status":"pending"}`), ReasonCorruptSnapshot},
		{"pending over budget", base(`{"BatchID":"b","Dataset":"ds","Amount":1.5,"Status":"pending"}`), ReasonCorruptSnapshot},
		{"confirmed missing consumption", base(`{"BatchID":"b","Dataset":"ds","Amount":0.5,"Actual":0.5,"Status":"confirmed"}`), ReasonCorruptSnapshot},
		{"future version", `{"version":3,"datasets":[]}`, ReasonVersionMismatch},
	}
	for _, tc := range cases {
		_, err := LoadSnapshot(strings.NewReader(tc.data))
		var dpe *Error
		errors.As(err, &dpe)
		t.Logf("input: %s -> reason=%q", tc.name, dpe.Reason)
		if dpe == nil || dpe.Reason != tc.want {
			t.Errorf("%s: want %q, got %v", tc.name, tc.want, err)
		}
	}
}

func TestLoadSnapshotV2DuplicateBatchRejected(t *testing.T) {
	data := `{"version":2,"datasets":[{"Name":"ds","TotalEpsilon":1.0}],
		"reservations":[
			{"BatchID":"b","Dataset":"ds","Amount":0.2,"Status":"pending"},
			{"BatchID":"b","Dataset":"ds","Amount":0.2,"Status":"pending"}
		]}`
	_, err := LoadSnapshot(strings.NewReader(data))
	if !isReason(err, ReasonCorruptSnapshot) {
		t.Fatalf("want corrupt_snapshot, got %v", err)
	}
}

func isReason(err error, want Reason) bool {
	var dpe *Error
	return errors.As(err, &dpe) && dpe.Reason == want
}
