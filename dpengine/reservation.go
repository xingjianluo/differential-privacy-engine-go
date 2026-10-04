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
	// ReservationExpired: the hold was reclaimed by an expiry sweep.
	// Terminal.
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
//	             yet passed; unavailable to other requests
//	Expired    = epsilon held by reservations whose deadline has passed but
//	             which have not been reclaimed yet; logically dead and
//	             reclaimable on demand (explicit sweep or the next
//	             reserve/consume that needs room)
//	Releasable = Reserved + Expired: every epsilon currently parked in a
//	             hold that can come back via release, deadline expiry or a
//	             reclaim sweep. It is a breakdown annotation, not an
//	             additional partition.
//	Available  = Total - Spent - Reserved - Expired as of the last
//	             reclaim: the amount grantable without triggering a
//	             reclaim. The Expired portion becomes usable immediately
//	             with the next reserve/consume that needs it (or an
//	             explicit ExpireReservations call).
//
// Invariants at all times:
//
//	Spent + Reserved + Expired <= Total
//	Spent + Reserved             <= Total   (a batch past its deadline can
//	                                          never be settled any more, so
//	                                          expired holds never turn into
//	                                          spent)
type BudgetView struct {
	Dataset    string
	Total      float64
	Spent      float64
	Reserved   float64
	Expired    float64
	Releasable float64
	Available  float64
}

// Reserve holds epsilon for a batch. The hold is immediately unavailable
// to other requests. It must later be settled exactly once via Confirm or
// Release before its deadline; after the deadline it can no longer be
// settled and is reclaimed (explicitly via ExpireReservations, or on demand
// when a later reserve/consume needs the room). Replaying a batchID that
// already exists returns its current state unchanged and never charges the
// hold twice.
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

	now := l.now()
	// Deadline is the sole expiry authority: before granting anything,
	// reclaim every hold whose deadline has passed — whether or not an
	// explicit sweep ever ran — so dead holds cannot pin the budget. The
	// reclaim is serialized here, so it can never race a settlement into a
	// double effect.
	l.reclaimDue(now)

	// Idempotent replay of the whole lifecycle: an existing batch is
	// never re-charged nor reset.
	if r, ok := l.reservations[batchID]; ok {
		return *r, nil
	}

	ds, ok := l.datasets[dataset]
	if !ok {
		return Reservation{}, newError(ReasonDatasetNotFound, "no such dataset: "+dataset)
	}
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
// is effective exactly once per batch.
//
// The deadline is the sole expiry authority: a batch whose ExpiresAt has
// passed can no longer be confirmed, regardless of whether an expiry sweep
// has run, and fails with ReasonReservationExpired. A second
// confirm/release after settlement fails with ReasonInvalidState; an
// unknown batch fails with ReasonBatchNotFound.
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
	if r.Status == ReservationReserved && !r.ExpiresAt.After(l.now()) {
		return Reservation{}, newError(ReasonReservationExpired,
			fmt.Sprintf("batch %q passed its deadline at %v and can no longer be confirmed",
				batchID, r.ExpiresAt))
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
	now := l.now()
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
// any consumption. Effective exactly once per batch. Releasing a batch
// whose deadline has passed fails with ReasonReservationExpired (it can
// only be reclaimed); releasing an already settled batch fails with
// ReasonInvalidState; an unknown batch fails with ReasonBatchNotFound.
func (l *Ledger) Release(batchID string) (Reservation, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	r, ok := l.reservations[batchID]
	if !ok {
		return Reservation{}, newError(ReasonBatchNotFound, "no such batch: "+batchID)
	}
	if r.Status == ReservationReserved && !r.ExpiresAt.After(l.now()) {
		return Reservation{}, newError(ReasonReservationExpired,
			fmt.Sprintf("batch %q passed its deadline at %v and can no longer be released",
				batchID, r.ExpiresAt))
	}
	if r.Status != ReservationReserved {
		return Reservation{}, newError(ReasonInvalidState,
			fmt.Sprintf("batch %q is %s, only a reserved batch can be released", batchID, r.Status))
	}

	ds := l.datasets[r.Dataset]
	now := l.now()
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

// ExpireReservations reclaims every open reservation whose ExpiresAt is due
// according to the ledger clock: the full hold is returned to the available
// pool and the batch becomes ReservationExpired (terminal). Recovery is
// deterministic and auditable — batches are reclaimed in BatchID order and
// each reclaim appends an expiry event. Already-settled or already-reclaimed
// batches are never touched, so repeated sweeps are idempotent and a hold is
// returned to the pool at most once. The reclaimed reservations are
// returned; an empty slice means nothing was due.
//
// Calling this method is optional: deadline-due holds are also reclaimed on
// demand by the next Reserve/Consume that could use the room, and can never
// be confirmed or released past their deadline even if no sweep ever runs.
func (l *Ledger) ExpireReservations() ([]Reservation, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.reclaimDue(l.now()), nil
}

// reclaimDue transitions every still-open reservation whose deadline is not
// after now into the terminal expired state, returns its full hold to the
// dataset pool and appends one expiry event per batch, in BatchID order.
// Callers must hold l.mu. Because reclaimDue runs under the same mutex as
// Confirm/Release, a batch can be settled OR reclaimed, never both — the
// deadline check in Confirm/Release guarantees an expired hold never turns
// into spent, and the single status flip guarantees the hold is freed once.
func (l *Ledger) reclaimDue(now time.Time) []Reservation {
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
		ds := l.datasets[r.Dataset]
		ds.ReservedEpsilon -= r.ReservedEps
		if ds.ReservedEpsilon < epsilonTolerance {
			ds.ReservedEpsilon = 0
		}
		r.Status = ReservationExpired
		r.ResolvedAt = now
		l.batchEvents = append(l.batchEvents, BatchEvent{
			BatchID: id, Dataset: r.Dataset,
			Kind: ReservationExpired, Amount: r.ReservedEps, At: now,
		})
		reclaimed = append(reclaimed, *r)
	}
	return reclaimed
}

// GetReservation returns a copy of one reservation.
func (l *Ledger) GetReservation(batchID string) (Reservation, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.reservations[batchID]
	if !ok {
		return Reservation{}, newError(ReasonBatchNotFound, "no such batch: "+batchID)
	}
	return *r, nil
}

// Budget returns the spent / reserved / expired / releasable / available
// breakdown at one consistent instant. Holds whose deadline has passed but
// which have not been reclaimed yet are reported as Expired (not Reserved),
// even when no explicit sweep has happened.
func (l *Ledger) Budget(dataset string) (BudgetView, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ds, ok := l.datasets[dataset]
	if !ok {
		return BudgetView{}, newError(ReasonDatasetNotFound, "no such dataset: "+dataset)
	}
	now := l.now()
	var reserved, expired float64
	for _, r := range l.reservations {
		if r.Dataset != dataset || r.Status != ReservationReserved {
			continue
		}
		if r.ExpiresAt.After(now) {
			reserved += r.ReservedEps
		} else {
			expired += r.ReservedEps
		}
	}
	available := ds.TotalEpsilon - ds.SpentEpsilon - reserved - expired
	if available < 0 && available > -epsilonTolerance {
		available = 0
	}
	return BudgetView{
		Dataset:    dataset,
		Total:      ds.TotalEpsilon,
		Spent:      ds.SpentEpsilon,
		Reserved:   reserved,
		Expired:    expired,
		Releasable: reserved + expired,
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
