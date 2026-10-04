package dpengine

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// fixedClock returns a controllable, concurrency-safe time source for
// deterministic expiry tests.
type fixedClock struct {
	mu sync.RWMutex
	t  time.Time
}

func newFixedClock(t time.Time) *fixedClock { return &fixedClock{t: t} }
func (c *fixedClock) now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.t
}
func (c *fixedClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// A batch past its deadline can no longer be settled, regardless of whether
// an expiry sweep ran. The deadline is the sole expiry authority.
func TestDeadlineRejectsConfirmWithoutSweep(t *testing.T) {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	clk := newFixedClock(base)
	l := mustLedger(t, "ds", 10.0)
	l.now = clk.now

	if _, err := l.Reserve("ds", "late", 4.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	clk.advance(61 * time.Second)

	// No ExpireReservations() call anywhere: the stored status is still
	// "reserved", yet the deadline decides.
	r, err := l.GetReservation("late")
	if err != nil || r.Status != ReservationReserved {
		t.Fatalf("precondition: batch still stored as reserved, got %+v", r)
	}

	if _, err := l.Confirm("late", 2.0); err == nil {
		t.Fatal("confirm past deadline without sweep must be rejected")
	} else {
		wantReason(t, err, ReasonReservationExpired, "confirm 61s after a 60s deadline, no sweep")
	}
	if _, err := l.Release("late"); err == nil {
		t.Fatal("release past deadline without sweep must be rejected")
	} else {
		wantReason(t, err, ReasonReservationExpired, "release 61s after a 60s deadline, no sweep")
	}

	// Expiry never becomes spent: the budget is intact.
	view, _ := l.Budget("ds")
	if view.Spent != 0 {
		t.Fatalf("expired batch must not charge consumption, spent=%.2f", view.Spent)
	}
	t.Logf("input: confirm/release at t+61s (deadline t+60s), no sweep -> both reservation_expired; spent=%.1f", view.Spent)
}

// A long-unsettled hold must not pin budget: once its deadline has passed,
// a later reserve (or direct consume) reclaims it on demand — even if no
// explicit ExpireReservations sweep ever ran.
func TestExpiredHoldReusedWithoutSweepByReserve(t *testing.T) {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	clk := newFixedClock(base)
	l := mustLedger(t, "ds", 10.0)
	l.now = clk.now

	if _, err := l.Reserve("ds", "sleeping", 8.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	// Before the deadline, a competing 8-epsilon hold cannot fit.
	if _, err := l.Reserve("ds", "rival", 8.0, time.Minute); err == nil {
		t.Fatal("rival hold must be blocked while sleeping hold is live")
	}
	clk.advance(61 * time.Second)

	// View separates expired from still-held before any reclaim.
	view, _ := l.Budget("ds")
	if view.Reserved != 0 || view.Expired != 8 || view.Spent != 0 {
		t.Fatalf("want reserved=0 expired=8 spent=0, got %+v", view)
	}
	t.Logf("input: sleeping=8 past deadline, no sweep -> reserved=%.1f expired=%.1f releasable=%.1f",
		view.Reserved, view.Expired, view.Releasable)

	// A new reservation that needs the room triggers the reclaim implicitly.
	if _, err := l.Reserve("ds", "rival", 8.0, 5*time.Minute); err != nil {
		t.Fatalf("expired hold must be reclaimable on demand: %v", err)
	}
	view, _ = l.Budget("ds")
	if view.Reserved != 8 || view.Expired != 0 || view.Spent != 0 || view.Available != 2 {
		t.Fatalf("after on-demand reclaim by reserve: %+v", view)
	}
	sleeping, _ := l.GetReservation("sleeping")
	if sleeping.Status != ReservationExpired {
		t.Fatalf("sleeping batch must be terminal expired, got %s", sleeping.Status)
	}
	t.Logf("input: rival reserves 8 with no prior sweep -> sleeping expired, reserved=%.1f expired=%.1f available=%.1f",
		view.Reserved, view.Expired, view.Available)
}

func TestExpiredHoldReusedWithoutSweepByConsume(t *testing.T) {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	clk := newFixedClock(base)
	l := mustLedger(t, "ds", 10.0)
	l.now = clk.now

	if _, err := l.Reserve("ds", "sleeping", 8.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	// Direct consume that needs exactly the held amount is blocked live.
	if _, err := l.Consume("ds", "q1", 8.0, SequentialComposition); err == nil {
		t.Fatal("direct consume must be blocked while the hold is live")
	}
	clk.advance(61 * time.Second)

	// Same consume, no sweep in between: the expired hold is reclaimed and
	// the charge goes through.
	c, err := l.Consume("ds", "q1", 8.0, SequentialComposition)
	if err != nil {
		t.Fatalf("direct consume must reclaim and reuse the expired hold: %v", err)
	}
	view, _ := l.Budget("ds")
	if c.SpentAfter != 8 || view.Spent != 8 || view.Reserved != 0 || view.Expired != 0 || view.Available != 2 {
		t.Fatalf("after on-demand reclaim by consume: c=%+v view=%+v", c, view)
	}
	t.Logf("input: consume 8 past deadline without sweep -> spent=%.1f reserved=%.1f expired=%.1f available=%.1f",
		view.Spent, view.Reserved, view.Expired, view.Available)

	// Reclaim happened exactly once: the sleeping batch is expired and an
	// explicit sweep afterwards returns nothing.
	if got, _ := l.ExpireReservations(); len(got) != 0 {
		t.Fatalf("already-reclaimed hold must not be reclaimed twice: %+v", got)
	}
}

// Under a deadline race, settlement and reclaim are mutually exclusive:
// across many goroutines either exactly one confirm charges the hold (it
// ran strictly before the deadline) or the hold is reclaimed once — never
// both, and never twice. We also hammer confirm/release/sweep together past
// the deadline and assert the pool is never both charged and refilled.
func TestConcurrentConfirmVsExpireExactlyOneEffect(t *testing.T) {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

	for iter := 0; iter < 50; iter++ {
		clk := newFixedClock(base)
		l := mustLedger(t, "ds", 4.0)
		l.now = clk.now
		if _, err := l.Reserve("ds", "b", 4.0, time.Minute); err != nil {
			t.Fatal(err)
		}

		const n = 16
		var wg sync.WaitGroup
		var mu sync.Mutex
		confirmed, reclaimed, expiredReject, otherReject := 0, 0, 0, 0
		start := make(chan struct{})
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				switch i % 4 {
				case 0, 1:
					// Half try to settle the full hold; the deadline may
					// already have passed.
					if _, err := l.Confirm("b", 4.0); err == nil {
						mu.Lock()
						confirmed++
						mu.Unlock()
					} else {
						mu.Lock()
						var dpe *Error
						if errors.As(err, &dpe) && dpe.Reason == ReasonReservationExpired {
							expiredReject++
						} else {
							otherReject++
						}
						mu.Unlock()
					}
				case 2:
					if _, err := l.Release("b"); err == nil {
						mu.Lock()
						reclaimed++ // a successful release also frees the hold once
						mu.Unlock()
					}
				case 3:
					if got, _ := l.ExpireReservations(); len(got) == 1 {
						mu.Lock()
						reclaimed++
						mu.Unlock()
					}
				}
			}(i)
		}
		// Flip the deadline right as workers start, so settlement and
		// reclaim genuinely collide.
		close(start)
		clk.advance(time.Minute)
		wg.Wait()

		view, _ := l.Budget("ds")
		// Exactly one terminal effect across confirm/release/expire.
		if confirmed+reclaimed != 1 {
			t.Fatalf("iter %d: want exactly one terminal effect, got confirmed=%d freed=%d rejects(expired=%d other=%d) view=%+v",
				iter, confirmed, reclaimed, expiredReject, otherReject, view)
		}
		// Never both charged and returned: if the confirm won, spent=4 and
		// nothing sits free; otherwise spent=0 and the full 4 is available.
		if confirmed == 1 {
			if view.Spent != 4 || view.Reserved != 0 || view.Expired != 0 || view.Available != 0 {
				t.Fatalf("iter %d: winning confirm left inconsistent view: %+v", iter, view)
			}
		} else {
			if view.Spent != 0 || view.Reserved != 0 || view.Expired != 0 || view.Available != 4 {
				t.Fatalf("iter %d: winning reclaim left inconsistent view: %+v", iter, view)
			}
		}
	}
	t.Logf("input: 50 iterations x 16 goroutines racing confirm/release/expire across the deadline -> exactly one effect each")
}

// A batch already past its deadline before a restart must still be judged
// by that deadline afterwards: it cannot be settled, and its hold is
// reclaimable after restart exactly as before — the restart neither revives
// nor double-frees it.
func TestDeadlineSurvivesRestart(t *testing.T) {
	base := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	clk := newFixedClock(base)
	l := mustLedger(t, "ds", 10.0)
	l.now = clk.now
	if _, err := l.Reserve("ds", "open", 6.0, time.Minute); err != nil {
		t.Fatal(err)
	}

	// The process only restarts later, well after the deadline. The
	// restored ledger is handed the post-deadline clock.
	r := snapshotRoundTrip(t, l)
	r.now = clk.now // base + 0 so far; now move past it
	clk.advance(2 * time.Minute)

	// Still stored as "reserved" in the snapshot, but the deadline rules.
	got, err := r.GetReservation("open")
	if err != nil || got.Status != ReservationReserved {
		t.Fatalf("restored batch should be stored reserved, got %+v", got)
	}
	if _, err := r.Confirm("open", 3.0); err == nil {
		t.Fatal("confirm after restart past deadline must be rejected")
	} else {
		wantReason(t, err, ReasonReservationExpired, "confirm restored batch 1m past its deadline")
	}
	if _, err := r.Release("open"); err == nil {
		t.Fatal("release after restart past deadline must be rejected")
	} else {
		wantReason(t, err, ReasonReservationExpired, "release restored batch 1m past its deadline")
	}

	// The view splits the dead hold out without any sweep.
	view, _ := r.Budget("ds")
	if view.Reserved != 0 || view.Expired != 6 || view.Spent != 0 {
		t.Fatalf("restored expired hold view wrong: %+v", view)
	}
	t.Logf("input: restart 2m after a 1m deadline, no sweep -> reserved=%.1f expired=%.1f spent=%.1f",
		view.Reserved, view.Expired, view.Spent)

	// On-demand reclaim after restart frees it exactly once and the room
	// is reusable.
	if _, err := r.Reserve("ds", "after", 6.0, time.Minute); err != nil {
		t.Fatalf("expired hold must be reclaimable after restart: %v", err)
	}
	if swept, _ := r.ExpireReservations(); len(swept) != 0 {
		t.Fatalf("reclaimed twice after restart: %+v", swept)
	}
	view, _ = r.Budget("ds")
	if view.Reserved != 6 || view.Expired != 0 || view.Available != 4 {
		t.Fatalf("post-restart reclaim view wrong: %+v", view)
	}
	t.Logf("input: new reserve 6 after restart -> reserved=%.1f expired=%.1f available=%.1f",
		view.Reserved, view.Expired, view.Available)
}

// At the exact deadline the hold is due: the last live instant is strictly
// before ExpiresAt, and now == ExpiresAt is already expired.
func TestDeadlineBoundaryIsExclusive(t *testing.T) {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	clk := newFixedClock(base)
	l := mustLedger(t, "ds", 10.0)
	l.now = clk.now
	if _, err := l.Reserve("ds", "b", 2.0, time.Minute); err != nil {
		t.Fatal(err)
	}

	clk.advance(time.Minute - time.Nanosecond)
	if _, err := l.Confirm("b", 1.0); err != nil {
		t.Fatalf("one nanosecond before the deadline must still settle: %v", err)
	}
	view, _ := l.Budget("ds")
	if view.Spent != 1 || view.Reserved != 0 {
		t.Fatalf("last-instant confirm wrong view: %+v", view)
	}
	t.Logf("input: confirm at deadline-1ns -> settled; spent=%.1f reserved=%.1f", view.Spent, view.Reserved)

	// A second batch probed exactly at its deadline is expired, no sweep.
	l2 := mustLedger(t, "d2", 10.0)
	l2.now = clk.now
	clk2 := newFixedClock(base)
	l2.now = clk2.now
	if _, err := l2.Reserve("d2", "b2", 2.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	clk2.advance(time.Minute) // now == ExpiresAt
	if _, err := l2.Release("b2"); err == nil {
		t.Fatal("release exactly at the deadline must be rejected")
	} else {
		wantReason(t, err, ReasonReservationExpired, "release at now == expiresAt")
	}
}

// The three settlement failures stay clearly distinguishable: unknown
// batch, already terminal batch, and deadline-expired batch.
func TestExpiryErrorDistinguishedFromNotFoundAndState(t *testing.T) {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	clk := newFixedClock(base)
	l := mustLedger(t, "ds", 10.0)
	l.now = clk.now
	if _, err := l.Reserve("ds", "b", 2.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Reserve("ds", "done", 2.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Confirm("done", 1.0); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Minute)

	// 1) unknown -> batch_not_found
	_, err := l.Confirm("ghost", 1.0)
	wantReason(t, err, ReasonBatchNotFound, "confirm unknown batch past clock")
	// 2) already settled -> invalid_state (deadline irrelevant)
	_, err = l.Confirm("done", 1.0)
	wantReason(t, err, ReasonInvalidState, "confirm already-confirmed batch")
	_, err = l.Release("done")
	wantReason(t, err, ReasonInvalidState, "release already-confirmed batch")
	// 3) open but past deadline -> reservation_expired
	_, err = l.Confirm("b", 1.0)
	wantReason(t, err, ReasonReservationExpired, "confirm open past-deadline batch")
	_, err = l.Release("b")
	wantReason(t, err, ReasonReservationExpired, "release open past-deadline batch")
	t.Logf("input: ghost/done(confirmed)/b(past deadline) -> not_found, invalid_state, reservation_expired all distinct")
}

// Under a mixed live/expired workload the budget partition always satisfies
// Spent + Reserved (+ Expired pre-reclaim) <= Total, and expiry never
// converts into spent.
func TestBudgetInvariantWithMixedHolds(t *testing.T) {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	clk := newFixedClock(base)
	l := mustLedger(t, "ds", 10.0)
	l.now = clk.now

	if _, err := l.Consume("ds", "q0", 1.0, SequentialComposition); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Reserve("ds", "live", 3.0, 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Reserve("ds", "dead", 4.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Minute)

	view, _ := l.Budget("ds")
	if view.Spent != 1 || view.Reserved != 3 || view.Expired != 4 ||
		view.Releasable != 7 || view.Available != 2 {
		t.Fatalf("mixed view wrong: %+v", view)
	}
	if view.Spent+view.Reserved > view.Total ||
		view.Spent+view.Reserved+view.Expired > view.Total+epsilonTolerance {
		t.Fatalf("budget invariant violated: %+v", view)
	}
	t.Logf("input: spent=1 live=3 dead=4 -> spent=%.1f reserved=%.1f expired=%.1f releasable=%.1f available=%.1f (invariant holds)",
		view.Spent, view.Reserved, view.Expired, view.Releasable, view.Available)

	// New reserve that exactly fits into spent+live+new = 10 must succeed
	// without reclaiming the dead hold (it already fits in available=2)?
	// It needs 2, which fits, so the dead hold stays unreclaimed until
	// something actually needs it; but a 6-epsilon request forces it.
	if _, err := l.Reserve("ds", "needsroom", 6.0, 10*time.Minute); err != nil {
		t.Fatalf("request needing the expired hold must reclaim it: %v", err)
	}
	view, _ = l.Budget("ds")
	if view.Spent != 1 || view.Reserved != 9 || view.Expired != 0 || view.Available != 0 {
		t.Fatalf("after forced reclaim view wrong: %+v", view)
	}
}

// Once the deadline has passed, a storm of concurrent confirm/release calls
// must all fail with reservation_expired and charge nothing — even if an
// on-demand reclaim triggered by a concurrent reserve interleaves with them.
func TestConcurrentSettleAfterDeadlineChargesNothing(t *testing.T) {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	clk := newFixedClock(base)
	l := mustLedger(t, "ds", 10.0)
	l.now = clk.now
	if _, err := l.Reserve("ds", "b", 5.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	clk.advance(time.Minute) // deadline reached, no sweep

	const n = 32
	var wg sync.WaitGroup
	start := make(chan struct{})
	var mu sync.Mutex
	expiredErrs, stateErrs, successes := 0, 0, 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			var err error
			if i%2 == 0 {
				_, err = l.Confirm("b", 5.0)
			} else {
				_, err = l.Release("b")
			}
			mu.Lock()
			switch {
			case err == nil:
				successes++
			default:
				var dpe *Error
				if errors.As(err, &dpe) {
					switch dpe.Reason {
					case ReasonReservationExpired:
						expiredErrs++
					case ReasonInvalidState:
						stateErrs++
					}
				}
			}
			mu.Unlock()
		}(i)
	}
	// Interleave a reserve that triggers the on-demand reclaim while
	// settlements are in flight.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		_, _ = l.Reserve("ds", "new", 5.0, time.Minute)
	}()
	close(start)
	wg.Wait()

	if successes != 0 {
		t.Fatalf("no past-deadline settlement may succeed, got %d", successes)
	}
	if expiredErrs+stateErrs != n {
		t.Fatalf("all %d settlements must be expired/invalid_state rejections, got expired=%d invalid=%d",
			n, expiredErrs, stateErrs)
	}
	view, _ := l.Budget("ds")
	if view.Spent != 0 || view.Reserved != 5 || view.Expired != 0 {
		t.Fatalf("dead batch must never charge; new hold replaces it once: %+v", view)
	}
	t.Logf("input: %d concurrent past-deadline settle calls + reclaiming reserve -> successes=%d expired=%d invalid=%d; spent=%.1f reserved=%.1f",
		n, successes, expiredErrs, stateErrs, view.Spent, view.Reserved)
}
