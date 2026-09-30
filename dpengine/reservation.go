package dpengine

import (
	"fmt"
	"math"
	"time"
)

// ReservationStatus is the lifecycle state of a reserved budget batch.
type ReservationStatus string

const (
	// ReservationPending: budget is held and unavailable to other requests.
	ReservationPending ReservationStatus = "pending"
	// ReservationConfirmed: the batch settled; actual consumption is recorded.
	ReservationConfirmed ReservationStatus = "confirmed"
	// ReservationReleased: the batch was cancelled/failed; the held budget
	// is returned in full.
	ReservationReleased ReservationStatus = "released"
	// ReservationExpired: the batch was not settled before its deadline and
	// was reclaimed by the deterministic expiry rule.
	ReservationExpired ReservationStatus = "expired"
)

// ReservationKind classifies an entry in a batch's audit trail.
type ReservationKind string

const (
	kindReserved  ReservationKind = "reserved"
	kindConfirmed ReservationKind = "confirmed"
	kindReleased  ReservationKind = "released"
	kindExpired   ReservationKind = "expired"
)

// Reservation is one batch's budget hold: reserve first, settle later.
//
// Lifecycle (each transition happens at most once, even under concurrent
// or replayed calls):
//
//	pending ──Confirm──▶ confirmed
//	        ──Release──▶ released
//	        ──deadline──▶ expired   (deterministic reclaim)
//
// A confirmed batch charges Actual (<= Amount) epsilon; the unused part
// (Amount - Actual) becomes available again. Released and expired batches
// return the full held amount without charging anything.
type Reservation struct {
	BatchID   string
	Dataset   string
	Amount    float64
	CreatedAt time.Time
	ExpiresAt time.Time
	Status    ReservationStatus
	// Actual is the epsilon charged at confirm time; 0 otherwise.
	Actual    float64
	SettledAt time.Time
	// Events is the immutable, append-only lifecycle audit trail.
	Events []ReservationEvent
}

// ReservationEvent is one immutable lifecycle record of a reservation.
type ReservationEvent struct {
	Kind       ReservationKind
	At         time.Time
	Amount     float64
	SpentAfter float64
}

// BudgetBreakdown reports the externally distinguishable budget parts of
// one dataset:
//
//	Total      = configured total epsilon
//	Spent      = permanently confirmed consumption
//	Reserved   = epsilon held by pending batches (unavailable now)
//	Releasable = subset of Reserved held by overdue pending batches; it is
//	             still unavailable until a reclaim settles it
//	Available  = Total - Spent - Reserved
type BudgetBreakdown struct {
	Total      float64
	Spent      float64
	Reserved   float64
	Releasable float64
	Available  float64
}

// Reserve holds epsilon for the batch identified by batchID. The held
// amount becomes unavailable to every other request immediately. ttl is
// the settlement deadline: a pending batch not confirmed/released before
// now+ttl is reclaimable by the deterministic expiry rule.
//
// Reserve is idempotent per batchID: a repeated call (including retries
// after a crash) returns the original reservation and never holds budget
// twice.
func (l *Ledger) Reserve(dataset, batchID string, epsilon float64, ttl time.Duration) (Reservation, error) {
	if batchID == "" {
		return Reservation{}, newError(ReasonInvalidParameter, "batch id must not be empty")
	}
	if epsilon <= 0 || math.IsNaN(epsilon) || math.IsInf(epsilon, 0) {
		return Reservation{}, newError(ReasonInvalidParameter,
			fmt.Sprintf("reserved epsilon must be a finite positive number, got %v", epsilon))
	}
	if ttl <= 0 {
		return Reservation{}, newError(ReasonInvalidParameter,
			fmt.Sprintf("ttl must be positive, got %v", ttl))
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Replay: same batchID returns the original hold, never a double hold.
	if r, ok := l.reservations[batchID]; ok {
		return *r, nil
	}

	ds, ok := l.datasets[dataset]
	if !ok {
		return Reservation{}, newError(ReasonDatasetNotFound, "no such dataset: "+dataset)
	}

	now := l.now()
	// Deterministic reclaim: an overdue hold never blocks new reservations
	// forever — reclaim this dataset's expired batches before checking.
	l.reclaimDatasetLocked(ds.Name, now)

	if ds.SpentEpsilon+l.reservedOf(ds)+epsilon > ds.TotalEpsilon {
		return Reservation{}, newError(ReasonBudgetExhausted,
			fmt.Sprintf("dataset %q: cannot reserve %.6f (spent=%.6f reserved=%.6f budget=%.6f)",
				dataset, epsilon, ds.SpentEpsilon, l.reservedOf(ds), ds.TotalEpsilon))
	}

	r := &Reservation{
		BatchID:   batchID,
		Dataset:   dataset,
		Amount:    epsilon,
		CreatedAt: now,
		ExpiresAt: now.Add(ttl),
		Status:    ReservationPending,
		Events: []ReservationEvent{{
			Kind:       kindReserved,
			At:         now,
			Amount:     epsilon,
			SpentAfter: ds.SpentEpsilon,
		}},
	}
	l.reservations[batchID] = r
	return *r, nil
}

// Confirm settles a pending batch: actualEpsilon (<= reserved amount) is
// permanently charged as sequential-composition consumption and the
// unused remainder of the hold becomes available. Replaying a confirmed
// batch returns the original settlement; confirming a released/expired
// batch is rejected with ReasonInvalidState. actualEpsilon may be 0.
func (l *Ledger) Confirm(batchID string, actualEpsilon float64) (Reservation, error) {
	if batchID == "" {
		return Reservation{}, newError(ReasonInvalidParameter, "batch id must not be empty")
	}
	if actualEpsilon < 0 || math.IsNaN(actualEpsilon) || math.IsInf(actualEpsilon, 0) {
		return Reservation{}, newError(ReasonInvalidParameter,
			fmt.Sprintf("actual epsilon must be a finite non-negative number, got %v", actualEpsilon))
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	r, ok := l.reservations[batchID]
	if !ok {
		return Reservation{}, newError(ReasonBatchNotFound, "no such batch: "+batchID)
	}
	switch r.Status {
	case ReservationConfirmed:
		// Idempotent replay: settlement happens at most once.
		return *r, nil
	case ReservationReleased:
		return *r, newError(ReasonInvalidState, "batch already released: "+batchID)
	case ReservationExpired:
		return *r, newError(ReasonInvalidState, "batch already reclaimed as expired: "+batchID)
	}

	now := l.now()
	if !now.Before(r.ExpiresAt) {
		// Past the deadline the only legal transition is the deterministic
		// reclaim; a late confirmation would make settlement depend on
		// scheduler timing.
		l.expireLocked(r, now)
		return *r, newError(ReasonInvalidState, "batch expired before settlement: "+batchID)
	}
	if actualEpsilon > r.Amount {
		return *r, newError(ReasonInvalidParameter,
			fmt.Sprintf("actual epsilon %.6f exceeds reserved %.6f", actualEpsilon, r.Amount))
	}

	ds := l.datasets[r.Dataset]
	newSpent := ds.SpentEpsilon + actualEpsilon
	// Invariant: newSpent + other-pending <= total, because actual <= Amount
	// and this batch's hold covered Amount. Guard anyway, never overspend.
	otherReserved := l.reservedOf(ds) - r.Amount
	if newSpent+otherReserved > ds.TotalEpsilon {
		return *r, newError(ReasonBudgetExhausted,
			fmt.Sprintf("dataset %q: confirming %.6f would exceed budget", r.Dataset, actualEpsilon))
	}

	c := Consumption{
		RequestID:   batchID,
		Epsilon:     actualEpsilon,
		Composition: SequentialComposition,
		SpentAfter:  newSpent,
	}
	ds.SpentEpsilon = newSpent
	// Append even for actualEpsilon == 0 so the audit trail and the replay
	// index survive a snapshot round-trip (prevents replay double-charge).
	ds.ConsumedUnits = append(ds.ConsumedUnits, c)
	l.processed[batchID] = c

	r.Status = ReservationConfirmed
	r.Actual = actualEpsilon
	r.SettledAt = now
	r.Events = append(r.Events, ReservationEvent{
		Kind:       kindConfirmed,
		At:         now,
		Amount:     actualEpsilon,
		SpentAfter: newSpent,
	})
	return *r, nil
}

// Release cancels a pending batch and returns its full held amount
// without charging. Replaying a released batch returns the original
// record; releasing an already confirmed/expired batch is rejected with
// ReasonInvalidState.
func (l *Ledger) Release(batchID string) (Reservation, error) {
	if batchID == "" {
		return Reservation{}, newError(ReasonInvalidParameter, "batch id must not be empty")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	r, ok := l.reservations[batchID]
	if !ok {
		return Reservation{}, newError(ReasonBatchNotFound, "no such batch: "+batchID)
	}
	switch r.Status {
	case ReservationReleased:
		return *r, nil
	case ReservationConfirmed:
		return *r, newError(ReasonInvalidState, "batch already confirmed: "+batchID)
	case ReservationExpired:
		return *r, newError(ReasonInvalidState, "batch already reclaimed as expired: "+batchID)
	}

	now := l.now()
	if !now.Before(r.ExpiresAt) {
		l.expireLocked(r, now)
		return *r, newError(ReasonInvalidState, "batch expired before release: "+batchID)
	}

	ds := l.datasets[r.Dataset]
	r.Status = ReservationReleased
	r.SettledAt = now
	r.Events = append(r.Events, ReservationEvent{
		Kind:       kindReleased,
		At:         now,
		Amount:     r.Amount,
		SpentAfter: ds.SpentEpsilon,
	})
	return *r, nil
}

// ReclaimExpired applies the deterministic expiry rule to every pending
// batch whose deadline has passed (now >= ExpiresAt): the batch becomes
// ReservationExpired and its full hold returns to the available pool
// without charging. Already-settled batches are untouched. The reclaimed
// batches are returned (empty slice when nothing was due) so callers can
// audit the sweep.
func (l *Ledger) ReclaimExpired() []Reservation {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	var reclaimed []Reservation
	for _, r := range l.reservations {
		if r.Status == ReservationPending && !now.Before(r.ExpiresAt) {
			l.expireLocked(r, now)
			reclaimed = append(reclaimed, *r)
		}
	}
	return reclaimed
}

// expireLocked marks one pending batch expired and appends the audit
// event. Caller must hold l.mu.
func (l *Ledger) expireLocked(r *Reservation, now time.Time) {
	ds := l.datasets[r.Dataset]
	r.Status = ReservationExpired
	r.SettledAt = now
	r.Events = append(r.Events, ReservationEvent{
		Kind:       kindExpired,
		At:         now,
		Amount:     r.Amount,
		SpentAfter: ds.SpentEpsilon,
	})
}

// reclaimDatasetLocked reclaims overdue pending batches of one dataset.
// Caller must hold l.mu.
func (l *Ledger) reclaimDatasetLocked(dataset string, now time.Time) {
	for _, r := range l.reservations {
		if r.Dataset == dataset && r.Status == ReservationPending && !now.Before(r.ExpiresAt) {
			l.expireLocked(r, now)
		}
	}
}

// Reservation returns a copy of a batch by ID.
func (l *Ledger) Reservation(batchID string) (Reservation, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.reservations[batchID]
	if !ok {
		return Reservation{}, newError(ReasonBatchNotFound, "no such batch: "+batchID)
	}
	return *r, nil
}

// Budget reports the spent / reserved / releasable / available parts of a
// dataset. The read never mutates state: overdue batches remain counted
// under Reserved (and Releasable) until ReclaimExpired settles them.
func (l *Ledger) Budget(dataset string) (BudgetBreakdown, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ds, ok := l.datasets[dataset]
	if !ok {
		return BudgetBreakdown{}, newError(ReasonDatasetNotFound, "no such dataset: "+dataset)
	}
	now := l.now()
	var reserved, releasable float64
	for _, r := range l.reservations {
		if r.Dataset != dataset || r.Status != ReservationPending {
			continue
		}
		reserved += r.Amount
		if !now.Before(r.ExpiresAt) {
			releasable += r.Amount
		}
	}
	return BudgetBreakdown{
		Total:      ds.TotalEpsilon,
		Spent:      ds.SpentEpsilon,
		Reserved:   reserved,
		Releasable: releasable,
		Available:  ds.TotalEpsilon - ds.SpentEpsilon - reserved,
	}, nil
}
