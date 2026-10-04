package dpengine

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// ReservationStatus is the lifecycle state of a budget reservation.
type ReservationStatus string

const (
	// ReservationReserved: budget is held and unavailable to others.
	ReservationReserved ReservationStatus = "reserved"
	// ReservationConfirmed: actual consumption was settled, remaining
	// hold released. Terminal.
	ReservationConfirmed ReservationStatus = "confirmed"
	// ReservationReleased: the whole hold was released without
	// consumption. Terminal.
	ReservationReleased ReservationStatus = "released"
	// ReservationExpired: the hold reached its ExpiresAt deadline. It is
	// materialized either by an explicit ExpireReservations sweep or
	// lazily by the next operation that touches the budget. Terminal.
	ReservationExpired ReservationStatus = "expired"
)

// epsilonTolerance covers float rounding when checking that a settled
// amount does not exceed its hold. It is far smaller than any realistic
// epsilon, so it can only absorb IEEE-754 accumulation noise — never an
// observable 1e-9-scale overspend.
const epsilonTolerance = 1e-12

// Reservation is one batch's budget hold.
type Reservation struct {
	BatchID      string
	Dataset      string
	ReservedEps  float64
	ConfirmedEps float64
	Status       ReservationStatus
	CreatedAt    time.Time
	ExpiresAt    time.Time
	ResolvedAt   time.Time
}

// BatchEvent is one immutable, auditable reservation lifecycle record.
// Kind is the state the batch transitioned into with this event.
type BatchEvent struct {
	BatchID string
	Dataset string
	Kind    ReservationStatus
	Amount  float64
	At      time.Time
}

// BudgetView breaks down the budget of one dataset:
//
//	Total      = configured total epsilon
//	Spent      = settled consumption (monotonic, never decreases)
//	Reserved   = epsilon held by open reservations whose deadline has not
//	             passed yet; genuinely unavailable to other requests
//	Releasable = epsilon held by reservations that are already past their
//	             deadline but have not been materialized by a sweep yet;
//	             this amount is part of Available (it is reclaimed on
//	             demand the moment capacity is needed)
//	Available  = epsilon grantable to a new request right now
//	           = Total - Spent - Reserved (overdue holds are excluded —
//	             they are reclaimed rather than allowed to block others)
//
//	Expired    = alias of Releasable: the overdue, awaiting-reclaim share,
//	             presented under its business name.
//
// The three mutually exclusive budget categories partition Total:
//
//	Total = Spent + Reserved + Available, with Releasable ⊆ Available.
//
// Invariant: Spent + Reserved <= Total always holds.
type BudgetView struct {
	Dataset    string
	Total      float64
	Spent      float64
	Reserved   float64
	Releasable float64
	Expired    float64
	Available  float64
}

// Reserve holds epsilon for a batch. The hold is immediately unavailable
// to other requests. It must later be settled exactly once via Confirm or
// Release before the hold deadline (ExpiresAt = now + ttl); a batch whose
// deadline has passed is expired regardless of whether a sweep ran, and
// replaying a batchID that already exists returns its current state
// unchanged (after materializing any due expiry).
func (l *Ledger) Reserve(dataset, batchID string, epsilon float64, ttl time.Duration) (Reservation, error) {
	if batchID == "" {
		return Reservation{}, newError(ReasonInvalidParameter, "batch id must not be empty")
	}
	if epsilon <= 0 || math.IsNaN(epsilon) || math.IsInf(epsilon, 0) {
		return Reservation{}, newError(ReasonInvalidParameter,
			fmt.Sprintf("epsilon must be a finite positive number, got %v", epsilon))
	}
	if ttl <= 0 {
		return Reservation{}, newError(ReasonInvalidParameter,
			fmt.Sprintf("ttl must be positive, got %v", ttl))
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Idempotent replay of the whole lifecycle: an existing batch is
	// never re-charged nor reset. First honor the deadline: if the hold
	// is due it is expired whether or not a sweep happened to run.
	if r, ok := l.reservations[batchID]; ok {
		l.reclaimExpiredLocked(r.Dataset, l.now())
		return *r, nil
	}

	ds, ok := l.datasets[dataset]
	if !ok {
		return Reservation{}, newError(ReasonDatasetNotFound, "no such dataset: "+dataset)
	}
	// Overdue holds never block new requests: reclaim them before
	// checking capacity, even if no sweep ever ran.
	now := l.now()
	l.reclaimExpiredLocked(dataset, now)
	if ds.SpentEpsilon+ds.ReservedEpsilon+epsilon > ds.TotalEpsilon+epsilonTolerance {
		return Reservation{}, newError(ReasonBudgetExhausted,
			fmt.Sprintf("dataset %q: reserve %.6f but only %.6f available (spent %.6f, reserved %.6f, budget %.6f)",
				dataset, epsilon, ds.TotalEpsilon-ds.SpentEpsilon-ds.ReservedEpsilon,
				ds.SpentEpsilon, ds.ReservedEpsilon, ds.TotalEpsilon))
	}

	r := &Reservation{
		BatchID:     batchID,
		Dataset:     dataset,
		ReservedEps: epsilon,
		Status:      ReservationReserved,
		CreatedAt:   now,
		ExpiresAt:   now.Add(ttl),
	}
	ds.ReservedEpsilon += epsilon
	l.reservations[batchID] = r
	l.batchEvents = append(l.batchEvents, BatchEvent{
		BatchID: batchID, Dataset: dataset,
		Kind: ReservationReserved, Amount: epsilon, At: now,
	})
	return *r, nil
}

// Confirm settles a reservation: actualEps becomes settled Spent (must not
// exceed the hold), and the unused remainder of the hold is freed. Confirm
// is effective exactly once per batch. A batch whose deadline has passed
// can never be confirmed — it fails with ReasonReservationExpired even if
// no expiry sweep ever ran (the deadline is the sole criterion). A confirm
// of an otherwise settled batch fails with ReasonInvalidState.
func (l *Ledger) Confirm(batchID string, actualEps float64) (Reservation, error) {
	if actualEps < 0 || math.IsNaN(actualEps) || math.IsInf(actualEps, 0) {
		return Reservation{}, newError(ReasonInvalidParameter,
			fmt.Sprintf("actual epsilon must be a finite non-negative number, got %v", actualEps))
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	r, ok := l.reservations[batchID]
	if !ok {
		return Reservation{}, newError(ReasonBatchNotFound, "no such batch: "+batchID)
	}
	// Deadline first, state second: an overdue open reservation is
	// expired independently of any sweep, and a swept batch is reported
	// with the same reason — both are distinct from a non-expired
	// invalid state and from a missing batch.
	now := l.now()
	if r.Status == ReservationReserved && !r.ExpiresAt.After(now) {
		l.reclaimExpiredLocked(r.Dataset, now)
	}
	if r.Status == ReservationExpired {
		return Reservation{}, newError(ReasonReservationExpired,
			fmt.Sprintf("batch %q expired at %s and cannot be confirmed", batchID, r.ExpiresAt.Format(time.RFC3339)))
	}
	if r.Status != ReservationReserved {
		return Reservation{}, newError(ReasonInvalidState,
			fmt.Sprintf("batch %q is %s, only a reserved batch can be confirmed", batchID, r.Status))
	}
	if actualEps > r.ReservedEps+epsilonTolerance {
		return Reservation{}, newError(ReasonBudgetExhausted,
			fmt.Sprintf("batch %q: confirmed %.6f exceeds reserved %.6f", batchID, actualEps, r.ReservedEps))
	}

	ds := l.datasets[r.Dataset]
	// Hold is removed from Reserved; the actual amount moves into Spent.
	// Spent never exceeds Total because actualEps <= ReservedEps and the
	// reserve-time check guaranteed Spent + Reserved <= Total.
	ds.ReservedEpsilon -= r.ReservedEps
	ds.SpentEpsilon += actualEps
	if ds.ReservedEpsilon < epsilonTolerance {
		ds.ReservedEpsilon = 0
	}

	r.ConfirmedEps = actualEps
	r.Status = ReservationConfirmed
	r.ResolvedAt = now
	l.batchEvents = append(l.batchEvents, BatchEvent{
		BatchID: batchID, Dataset: r.Dataset,
		Kind: ReservationConfirmed, Amount: actualEps, At: now,
	})
	return *r, nil
}

// Release cancels a reservation and frees the entire hold without charging
// any consumption. It is effective exactly once per batch and only while
// the hold is still alive: a batch past its deadline fails with
// ReasonReservationExpired (whether or not a sweep ran), and releasing any
// other settled batch fails with ReasonInvalidState.
func (l *Ledger) Release(batchID string) (Reservation, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	r, ok := l.reservations[batchID]
	if !ok {
		return Reservation{}, newError(ReasonBatchNotFound, "no such batch: "+batchID)
	}
	now := l.now()
	if r.Status == ReservationReserved && !r.ExpiresAt.After(now) {
		l.reclaimExpiredLocked(r.Dataset, now)
	}
	if r.Status == ReservationExpired {
		return Reservation{}, newError(ReasonReservationExpired,
			fmt.Sprintf("batch %q expired at %s and cannot be released", batchID, r.ExpiresAt.Format(time.RFC3339)))
	}
	if r.Status != ReservationReserved {
		return Reservation{}, newError(ReasonInvalidState,
			fmt.Sprintf("batch %q is %s, only a reserved batch can be released", batchID, r.Status))
	}

	ds := l.datasets[r.Dataset]
	ds.ReservedEpsilon -= r.ReservedEps
	if ds.ReservedEpsilon < epsilonTolerance {
		ds.ReservedEpsilon = 0
	}
	r.Status = ReservationReleased
	r.ResolvedAt = now
	l.batchEvents = append(l.batchEvents, BatchEvent{
		BatchID: batchID, Dataset: r.Dataset,
		Kind: ReservationReleased, Amount: r.ReservedEps, At: now,
	})
	return *r, nil
}

// reclaimExpiredLocked materializes every due reservation of one dataset:
// each overdue hold moves out of the dataset Reserved total exactly once
// and becomes the terminal ReservationExpired state with one audit event.
// It is the single code path shared by the explicit sweep and by lazy
// reclamation, so a hold can never be both settled and returned to the
// pool. Caller must hold l.mu.
func (l *Ledger) reclaimExpiredLocked(dataset string, now time.Time) {
	var ids []string
	for id, r := range l.reservations {
		if r.Dataset == dataset && r.Status == ReservationReserved &&
			!r.ExpiresAt.After(now) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		l.reclaimOneLocked(l.reservations[id], now)
	}
}

// reclaimOneLocked performs the one-time state transition of a single due
// reservation. It is only ever reached for a reservation that is still
// ReservationReserved, so the hold is removed from Reserved exactly once.
func (l *Ledger) reclaimOneLocked(r *Reservation, now time.Time) {
	ds := l.datasets[r.Dataset]
	ds.ReservedEpsilon -= r.ReservedEps
	if ds.ReservedEpsilon < epsilonTolerance {
		ds.ReservedEpsilon = 0
	}
	r.Status = ReservationExpired
	r.ResolvedAt = now
	l.batchEvents = append(l.batchEvents, BatchEvent{
		BatchID: r.BatchID, Dataset: r.Dataset,
		Kind: ReservationExpired, Amount: r.ReservedEps, At: now,
	})
}

// ExpireReservations reclaims every open reservation whose ExpiresAt is due
// according to the ledger clock: the full hold is removed from the held
// total and the batch becomes ReservationExpired (terminal). Recovery is
// deterministic and auditable — batches are reclaimed in BatchID order and
// each reclaim appends exactly one expiry event. Already-settled or
// already-reclaimed batches are never touched, so the sweep is idempotent
// and a batch is never both charged and refunded. The reclaimed
// reservations are returned; an empty slice means nothing was due.
//
// Calling the sweep is optional: the same reclamation happens lazily when
// capacity is needed or when an overdue batch is touched — the deadline,
// not this call, decides expiry.
func (l *Ledger) ExpireReservations() ([]Reservation, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	var ids []string
	for id, r := range l.reservations {
		if r.Status == ReservationReserved && !r.ExpiresAt.After(now) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)

	reclaimed := make([]Reservation, 0, len(ids))
	for _, id := range ids {
		r := l.reservations[id]
		l.reclaimOneLocked(r, now)
		reclaimed = append(reclaimed, *r)
	}
	return reclaimed, nil
}

// GetReservation returns a copy of one reservation. A reservation past its
// deadline is reported as ReservationExpired even when no sweep ran, so the
// observed state always agrees with the deadline-based judgment.
func (l *Ledger) GetReservation(batchID string) (Reservation, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.reservations[batchID]
	if !ok {
		return Reservation{}, newError(ReasonBatchNotFound, "no such batch: "+batchID)
	}
	now := l.now()
	if r.Status == ReservationReserved && !r.ExpiresAt.After(now) {
		l.reclaimExpiredLocked(r.Dataset, now)
	}
	return *r, nil
}

// Budget returns the spent / reserved / releasable / available breakdown.
// Reporting it does not mutate state: overdue but not yet materialized
// holds are classified as Releasable (a.k.a. Expired) and are excluded
// from both Reserved and the unavailable share, so Available already
// reflects the capacity that reclaiming them would free.
func (l *Ledger) Budget(dataset string) (BudgetView, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ds, ok := l.datasets[dataset]
	if !ok {
		return BudgetView{}, newError(ReasonDatasetNotFound, "no such dataset: "+dataset)
	}
	now := l.now()
	var overdue float64
	for _, r := range l.reservations {
		if r.Dataset == dataset && r.Status == ReservationReserved &&
			!r.ExpiresAt.After(now) {
			overdue += r.ReservedEps
		}
	}
	active := ds.ReservedEpsilon - overdue
	if active < epsilonTolerance {
		active = 0
	}
	available := ds.TotalEpsilon - ds.SpentEpsilon - active
	if available < 0 && available > -epsilonTolerance {
		available = 0
	}
	return BudgetView{
		Dataset:    dataset,
		Total:      ds.TotalEpsilon,
		Spent:      ds.SpentEpsilon,
		Reserved:   active,
		Releasable: overdue,
		Expired:    overdue,
		Available:  available,
	}, nil
}

// BatchHistory returns a copy of the append-only batch audit trail.
func (l *Ledger) BatchHistory() []BatchEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]BatchEvent, len(l.batchEvents))
	copy(out, l.batchEvents)
	return out
}
