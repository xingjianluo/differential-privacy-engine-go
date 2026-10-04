package dpengine

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Scenario: the deadline is the sole expiry criterion. A batch past its
// ExpiresAt must be unconfirmable/unreleasable even when no sweep ever ran;
// the outcome must be identical to the case where a sweep ran first.
func TestDeadlineAloneBlocksSettlementWithoutSweep(t *testing.T) {
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	// Branch A: nobody calls ExpireReservations at all.
	la := mustLedger(t, "ds", 10.0)
	la.now = func() time.Time { return base }
	if _, err := la.Reserve("ds", "b", 4.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	la.now = func() time.Time { return base.Add(time.Minute + time.Second) }

	_, errConfirm := la.Confirm("b", 3.0)
	wantReason(t, errConfirm, ReasonReservationExpired, "no sweep: confirm overdue batch")
	_, errRelease := la.Release("b")
	wantReason(t, errRelease, ReasonReservationExpired, "no sweep: release overdue batch")

	// Failure must not charge anything nor leave the hold usable for
	// settlement: spent stays zero and the batch reads as expired.
	ra, _ := la.GetReservation("b")
	if ra.Status != ReservationExpired {
		t.Fatalf("no sweep: overdue batch should read expired, got %s", ra.Status)
	}
	va, _ := la.Budget("ds")
	if va.Spent != 0 || va.Reserved != 0 {
		t.Fatalf("no sweep: failed late settlement moved budget: %+v", va)
	}
	t.Logf("no-sweep branch: overdue confirm/release rejected, status=%s spent=%.1f reserved=%.1f releasable=%.1f",
		ra.Status, va.Spent, va.Reserved, va.Releasable)

	// Branch B: the sweep runs before the late settlement attempts.
	lb := mustLedger(t, "ds", 10.0)
	lb.now = func() time.Time { return base }
	if _, err := lb.Reserve("ds", "b", 4.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	lb.now = func() time.Time { return base.Add(time.Minute + time.Second) }
	if got, err := lb.ExpireReservations(); err != nil || len(got) != 1 {
		t.Fatalf("sweep: want 1 reclaimed, got %d err=%v", len(got), err)
	}
	_, errConfirm = lb.Confirm("b", 3.0)
	wantReason(t, errConfirm, ReasonReservationExpired, "swept: confirm expired batch")
	_, errRelease = lb.Release("b")
	wantReason(t, errRelease, ReasonReservationExpired, "swept: release expired batch")
	rb, _ := lb.GetReservation("b")
	vb, _ := lb.Budget("ds")
	t.Logf("swept branch: status=%s spent=%.1f reserved=%.1f releasable=%.1f",
		rb.Status, vb.Spent, vb.Reserved, vb.Releasable)

	// Both branches must end in the same externally observable state.
	if ra.Status != rb.Status || va.Spent != vb.Spent ||
		va.Reserved != vb.Reserved || va.Available != vb.Available {
		t.Fatalf("sweep/no-sweep outcomes diverge:\nA=%+v %+v\nB=%+v %+v", ra, va, rb, vb)
	}
}

// Confirm/release exactly at or past the deadline fail, one second before
// the deadline they still succeed; reasons are distinct per failure class.
func TestDeadlineBoundaryAndReasonClasses(t *testing.T) {
	base := time.Date(2026, 10, 2, 8, 0, 0, 0, time.UTC)

	// One second before deadline: confirm still works.
	live := mustLedger(t, "ds", 10.0)
	live.now = func() time.Time { return base }
	if _, err := live.Reserve("ds", "ok", 2.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	live.now = func() time.Time { return base.Add(time.Minute - time.Second) }
	if _, err := live.Confirm("ok", 1.0); err != nil {
		t.Fatalf("confirm before deadline must succeed: %v", err)
	}
	v, _ := live.Budget("ds")
	t.Logf("alive confirm t+59s -> spent=%.1f reserved=%.1f", v.Spent, v.Reserved)

	// Exactly at the deadline: expired, distinct from invalid state and
	// from not-found.
	at := mustLedger(t, "ds2", 10.0)
	at.now = func() time.Time { return base }
	if _, err := at.Reserve("ds2", "on", 2.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	at.now = func() time.Time { return base.Add(time.Minute) }
	_, err := at.Release("on")
	wantReason(t, err, ReasonReservationExpired, "release exactly at deadline")

	// Settled (but not expired) batches keep reporting invalid_state, and
	// unknown batches keep reporting batch_not_found: the three failure
	// classes never blur together.
	wantReason(t, func() error { _, e := live.Release("ok"); return e }(),
		ReasonInvalidState, "release already confirmed (not expired)")
	_, err = live.Confirm("missing", 0)
	wantReason(t, err, ReasonBatchNotFound, "confirm unknown batch")
	_, err = live.Release("missing")
	wantReason(t, err, ReasonBatchNotFound, "release unknown batch")
}

// Without anyone triggering a sweep, an overdue hold is reclaimed lazily
// when a new reserve needs the capacity, and that capacity is reusable.
func TestOverdueHoldReusedByReserveWithoutSweep(t *testing.T) {
	base := time.Date(2026, 10, 3, 9, 0, 0, 0, time.UTC)
	l := mustLedger(t, "ds", 10.0)
	l.now = func() time.Time { return base }
	if _, err := l.Reserve("ds", "stale", 6.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Consume("ds", "base-load", 4.0, SequentialComposition); err != nil {
		t.Fatal(err)
	}

	// Budget is full while the hold is alive: 6 would not fit.
	l.now = func() time.Time { return base.Add(59 * time.Second) }
	if _, err := l.Reserve("ds", "early", 6.0, time.Minute); err == nil {
		t.Fatal("6.0 reserve must not fit while a live 6.0 hold pins budget")
	} else {
		wantReason(t, err, ReasonBudgetExhausted, "reserve 6 against live 6 hold + 4 spent")
	}

	// Past the deadline, still with NO explicit sweep: the same 6.0
	// reservation now fits because the overdue hold is reclaimed first.
	l.now = func() time.Time { return base.Add(time.Minute) }
	if _, err := l.Reserve("ds", "fresh", 6.0, time.Minute); err != nil {
		t.Fatalf("overdue hold must be reclaimed lazily on reserve: %v", err)
	}
	view, _ := l.Budget("ds")
	if view.Spent != 4 || view.Reserved != 6 || view.Available != 0 {
		t.Fatalf("lazy reclaim on reserve wrong view: %+v", view)
	}
	stale, _ := l.GetReservation("stale")
	if stale.Status != ReservationExpired {
		t.Fatalf("stale batch must be materialized expired, got %s", stale.Status)
	}
	t.Logf("no sweep: stale expired lazily, fresh holds 6 -> spent=%.1f reserved=%.1f available=%.1f",
		view.Spent, view.Reserved, view.Available)

	// The overdue batch was never charged and can never be settled now.
	wantReason(t, func() error { _, e := l.Confirm("stale", 6.0); return e }(),
		ReasonReservationExpired, "confirm lazily-reclaimed batch")
}

// Without a sweep, a direct Consume reuses an overdue hold's capacity.
func TestOverdueHoldReusedByConsumeWithoutSweep(t *testing.T) {
	base := time.Date(2026, 10, 3, 11, 0, 0, 0, time.UTC)
	l := mustLedger(t, "ds", 10.0)
	l.now = func() time.Time { return base }
	if _, err := l.Reserve("ds", "stale", 7.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	// While alive, a 4.0 direct charge does not fit (only 3 available).
	if _, err := l.Consume("ds", "q1", 4.0, SequentialComposition); err == nil {
		t.Fatal("direct consume must be blocked by live hold")
	}
	l.now = func() time.Time { return base.Add(time.Minute) }
	if _, err := l.Consume("ds", "q1", 4.0, SequentialComposition); err != nil {
		t.Fatalf("direct consume must reclaim overdue hold without a sweep: %v", err)
	}
	view, _ := l.Budget("ds")
	if view.Spent != 4 || view.Reserved != 0 || view.Available != 6 {
		t.Fatalf("lazy reclaim on consume wrong view: %+v", view)
	}
	t.Logf("no sweep: consume 4 after deadline reclaimed 7 -> spent=%.1f reserved=%.1f available=%.1f",
		view.Spent, view.Reserved, view.Available)
}

// The overdue share is reported separately from live holds in Budget.
func TestBudgetSplitsLiveAndOverdueHolds(t *testing.T) {
	base := time.Date(2026, 10, 4, 9, 0, 0, 0, time.UTC)
	l := mustLedger(t, "ds", 10.0)
	l.now = func() time.Time { return base }
	if _, err := l.Consume("ds", "c", 1.0, SequentialComposition); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Reserve("ds", "live", 3.0, 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Reserve("ds", "due", 2.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	l.now = func() time.Time { return base.Add(2 * time.Minute) }

	view, err := l.Budget("ds")
	if err != nil {
		t.Fatal(err)
	}
	if view.Total != 10 || view.Spent != 1 ||
		view.Reserved != 3 || view.Releasable != 2 || view.Expired != 2 ||
		view.Available != 6 {
		t.Fatalf("live/overdue split wrong: %+v", view)
	}
	// sp+live = 4 <= 10 always; available already counts the overdue 2.
	if view.Spent+view.Reserved > view.Total+1e-9 {
		t.Fatalf("invariant spent+reserved<=total broken: %+v", view)
	}
	t.Logf("view: total=%.1f spent=%.1f reserved(live)=%.1f releasable(expired)=%.1f available=%.1f",
		view.Total, view.Spent, view.Reserved, view.Releasable, view.Available)

	// Reading the view is non-mutating: the due batch is still held on
	// record until something actually reclaims it.
	due, _ := l.GetReservation("due")
	if due.Status != ReservationExpired {
		t.Fatalf("GetReservation should materialize overdue state, got %s", due.Status)
	}
}

// Concurrent confirm vs expiry at the deadline: exactly one wins. If the
// confirm wins (runs before the deadline instant) the actual amount is
// charged once and nothing is refunded; otherwise the batch expires once
// with zero charge. No double movement is possible.
func TestConcurrentConfirmVsExpireOnlyOneEffect(t *testing.T) {
	base := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	const (
		nsPerMinute          = int64(time.Minute / time.Nanosecond)
		beforeDeadlineOffset = nsPerMinute - 1
		atDeadlineOffset     = nsPerMinute
	)

	for trial := 0; trial < 8; trial++ {
		l := mustLedger(t, "ds", 10.0)
		var offNanos atomic.Int64
		l.now = func() time.Time {
			return base.Add(time.Duration(offNanos.Load()))
		}
		offNanos.Store(0)
		if _, err := l.Reserve("ds", "b", 4.0, time.Minute); err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		var mu sync.Mutex
		confirmed, expiredWins := 0, 0
		// Confirmers race with the clock one nanosecond before the
		// deadline; sweepers race with the clock exactly at it. Each
		// worker publishes its view through the atomic clock, so the
		// outcome depends purely on lock ordering — yet exactly one
		// terminal transition may ever happen.
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if i%2 == 0 {
					offNanos.Store(atDeadlineOffset)
					if got, _ := l.ExpireReservations(); len(got) == 1 {
						mu.Lock()
						expiredWins++
						mu.Unlock()
					}
				} else {
					offNanos.Store(beforeDeadlineOffset)
					if _, err := l.Confirm("b", 3.0); err == nil {
						mu.Lock()
						confirmed++
						mu.Unlock()
					}
				}
			}(i)
		}
		wg.Wait()

		if confirmed+expiredWins != 1 {
			t.Fatalf("trial %d: want exactly one terminal effect, got confirmed=%d expired=%d",
				trial, confirmed, expiredWins)
		}
		view, _ := l.Budget("ds")
		r, _ := l.GetReservation("b")
		if confirmed == 1 {
			if r.Status != ReservationConfirmed || view.Spent != 3 ||
				view.Reserved != 0 || view.Available != 7 {
				t.Fatalf("trial %d: confirm-winner accounting wrong: r=%+v view=%+v",
					trial, r, view)
			}
		} else {
			if r.Status != ReservationExpired || view.Spent != 0 ||
				view.Reserved != 0 || view.Available != 10 {
				t.Fatalf("trial %d: expire-winner accounting wrong: r=%+v view=%+v",
					trial, r, view)
			}
		}
		t.Logf("trial %d: confirmed=%d expired=%d -> status=%s spent=%.1f reserved=%.1f available=%.1f",
			trial, confirmed, expiredWins, r.Status, view.Spent, view.Reserved, view.Available)
	}
}

// Concurrent repeated settlement attempts on one live batch plus a late
// sweep: the batch settles or expires exactly once; budget never moves
// twice even under the race detector.
func TestConcurrentDuplicateSettlementMovesBudgetOnce(t *testing.T) {
	base := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	l := mustLedger(t, "ds", 10.0)
	var offNanos atomic.Int64
	l.now = func() time.Time { return base.Add(time.Duration(offNanos.Load())) }
	offNanos.Store(0)
	if _, err := l.Reserve("ds", "b", 4.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	// All settlement attempts race while the hold is still alive (t+30s).
	offNanos.Store(int64(30 * time.Second))

	const n = 64
	var wg sync.WaitGroup
	var mu sync.Mutex
	confirmed, released := 0, 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			switch i % 3 {
			case 0:
				if _, err := l.Confirm("b", 3.0); err == nil {
					mu.Lock()
					confirmed++
					mu.Unlock()
				}
			case 1:
				if _, err := l.Release("b"); err == nil {
					mu.Lock()
					released++
					mu.Unlock()
				}
			default:
				// Mixed worker: at most one of its two calls can win;
				// count whichever settles the batch.
				if _, err := l.Confirm("b", 3.0); err == nil {
					mu.Lock()
					confirmed++
					mu.Unlock()
				} else if _, err := l.Release("b"); err == nil {
					mu.Lock()
					released++
					mu.Unlock()
				}
			}
		}(i)
	}
	wg.Wait()
	if confirmed+released != 1 {
		t.Fatalf("exactly one settlement must win, got confirmed=%d released=%d",
			confirmed, released)
	}
	// After all settlement attempts the deadline passes: a sweep must
	// find nothing left to reclaim (the settled batch is terminal).
	offNanos.Store(int64(time.Hour))
	if got, _ := l.ExpireReservations(); len(got) != 0 {
		t.Fatalf("settled batch must never be reclaimed, got %+v", got)
	}
	view, _ := l.Budget("ds")
	if confirmed == 1 {
		if view.Spent != 3 || view.Reserved != 0 {
			t.Fatalf("duplicate confirms charged more than once: %+v", view)
		}
	} else {
		if view.Spent != 0 || view.Reserved != 0 {
			t.Fatalf("duplicate releases freed more than once: %+v", view)
		}
	}
	t.Logf("%d racing settlers -> confirmed=%d released=%d, late sweep reclaims=0, spent=%.1f reserved=%.1f",
		n, confirmed, released, view.Spent, view.Reserved)
}

// Restart after the deadline: a batch that was open when saved but is
// already overdue when restored is judged by its ExpiresAt alone — no
// sweep before the crash, no sweep after it.
func TestRestartAfterDeadlineJudgedByDeadline(t *testing.T) {
	base := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	l := mustLedger(t, "ds", 10.0)
	l.now = func() time.Time { return base }
	if _, err := l.Consume("ds", "c", 2.0, SequentialComposition); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Reserve("ds", "late", 4.0, time.Minute); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := SaveSnapshot(&buf, l); err != nil {
		t.Fatal(err)
	}
	r, err := LoadSnapshot(&buf)
	if err != nil {
		t.Fatal(err)
	}
	// Process comes back up 10 minutes after the deadline; it never runs
	// a sweep. The deadline must decide everything.
	r.now = func() time.Time { return base.Add(10 * time.Minute) }

	_, err = r.Confirm("late", 4.0)
	wantReason(t, err, ReasonReservationExpired, "post-restart confirm of overdue batch")
	_, err = r.Release("late")
	wantReason(t, err, ReasonReservationExpired, "post-restart release of overdue batch")

	// The 4.0 hold is reusable right away, still without an explicit
	// sweep: 4 spent + 4 live reserved == 8 <= 10 would not have fit
	// while the stale 4.0 hold counted (2+4+4 = 10 actually fits; use a
	// 5.0 reserve which only fits after reclaim: 2+5=7 vs 2+4+5=11).
	if _, err := r.Reserve("ds", "new", 5.0, time.Minute); err != nil {
		t.Fatalf("post-restart reserve must reclaim overdue hold lazily: %v", err)
	}
	view, _ := r.Budget("ds")
	if view.Spent != 2 || view.Reserved != 5 || view.Available != 3 {
		t.Fatalf("post-restart lazy reclaim wrong view: %+v", view)
	}
	t.Logf("restart 10m after deadline (no sweep): late rejected as expired; new 5.0 reserve fits -> spent=%.1f reserved=%.1f available=%.1f",
		view.Spent, view.Reserved, view.Available)

	// Audit: the expiry event exists exactly once (materialized lazily).
	kinds := map[ReservationStatus]int{}
	for _, ev := range r.BatchHistory() {
		if ev.BatchID == "late" {
			kinds[ev.Kind]++
		}
	}
	if kinds[ReservationReserved] != 1 || kinds[ReservationExpired] != 1 {
		t.Fatalf("late batch audit trail wrong: %+v", kinds)
	}

	// Restarting again keeps the expired state; nothing is re-reclaimed.
	var buf2 bytes.Buffer
	if err := SaveSnapshot(&buf2, r); err != nil {
		t.Fatal(err)
	}
	r2, err := LoadSnapshot(&buf2)
	if err != nil {
		t.Fatal(err)
	}
	// Keep the recovered clock at the restart instant: the "new" hold is
	// still alive, only the previously expired batch must be terminal.
	r2.now = func() time.Time { return base.Add(10 * time.Minute) }
	if got, _ := r2.ExpireReservations(); len(got) != 0 {
		t.Fatalf("second restart must not re-expire: %+v", got)
	}
	late2, _ := r2.GetReservation("late")
	if late2.Status != ReservationExpired {
		t.Fatalf("expired batch must stay expired across restart, got %s", late2.Status)
	}
	new2, _ := r2.GetReservation("new")
	if new2.Status != ReservationReserved {
		t.Fatalf("live batch must stay reserved across restart, got %s", new2.Status)
	}
	t.Logf("second restart: late stays %s, new stays %s, sweep reclaims=0",
		late2.Status, new2.Status)
}

// Sanity: over-hold while alive is budget_exhausted, while any confirm
// after the deadline is the expiry reason regardless of the amount.
func TestExpiryReasonsAreDistinguishable(t *testing.T) {
	base := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	l := mustLedger(t, "ds", 10.0)
	l.now = func() time.Time { return base }
	if _, err := l.Reserve("ds", "b", 1.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Confirm("b", 2.0); err == nil {
		t.Fatal("over-hold confirm should fail")
	} else {
		wantReason(t, err, ReasonBudgetExhausted, "over-hold while alive is budget_exhausted, not expiry")
	}
	l.now = func() time.Time { return base.Add(time.Minute) }
	// After deadline even an in-hold amount is an expiry error, not
	// budget_exhausted.
	wantReason(t, func() error { _, e := l.Confirm("b", 0.5); return e }(),
		ReasonReservationExpired, "in-hold confirm after deadline is expiry")
	var dpe *Error
	_, err := l.Release("b")
	if !errors.As(err, &dpe) {
		t.Fatalf("expected *Error, got %T", err)
	}
}
