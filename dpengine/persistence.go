package dpengine

import (
	"encoding/json"
	"fmt"
	"io"
)

// SnapshotVersion is the current on-disk schema version. Snapshots with a
// higher version are rejected explicitly; snapshots with a lower (or
// missing) version are loaded with deterministic default semantics.
//
// v2 adds the top-level Reservations array so unsettled budget holds
// survive a restart. v0/v1 snapshots simply load with no reservations:
// previously consumed budget is preserved untouched.
const SnapshotVersion = 2

// Snapshot is the serializable state of a Ledger.
type Snapshot struct {
	Version      int           `json:"version"`
	Datasets     []Dataset     `json:"datasets"`
	Reservations []Reservation `json:"reservations,omitempty"`
}

// SaveSnapshot writes the ledger state to w as versioned JSON. The
// per-request consumption records and the reservation audit trails are
// included so idempotent replay and unsettled holds survive restarts.
func SaveSnapshot(w io.Writer, l *Ledger) error {
	l.mu.Lock()
	snap := Snapshot{Version: SnapshotVersion}
	for _, ds := range l.datasets {
		snap.Datasets = append(snap.Datasets, *ds)
	}
	for _, r := range l.reservations {
		snap.Reservations = append(snap.Reservations, *r)
	}
	l.mu.Unlock()
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(snap)
}

// LoadSnapshot restores a ledger from r.
//
// Compatibility rules:
//   - Version > SnapshotVersion: rejected with ReasonVersionMismatch
//     (never silently downgraded).
//   - Version 0/1 (no reservation info): loaded with stable defaults —
//     existing spent consumption is preserved/rederived and the
//     reservation table starts empty. Missing pre-existing holds never
//     zeroes out recorded consumption.
//   - Version 2: pending reservations are restored exactly as they were,
//     still holding budget and still bound to their original deadline;
//     they are never auto-reclaimed at load (the expiry rule is applied
//     deterministically via ReclaimExpired / the next Reserve).
func LoadSnapshot(r io.Reader) (*Ledger, error) {
	var snap Snapshot
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return nil, newError(ReasonCorruptSnapshot, "cannot decode snapshot: "+err.Error())
	}
	if snap.Version > SnapshotVersion {
		return nil, newError(ReasonVersionMismatch,
			fmt.Sprintf("snapshot version %d not supported (max %d)", snap.Version, SnapshotVersion))
	}

	l := NewLedger()
	for _, ds := range snap.Datasets {
		if ds.Name == "" {
			return nil, newError(ReasonCorruptSnapshot, "dataset with empty name in snapshot")
		}
		if _, dup := l.datasets[ds.Name]; dup {
			return nil, newError(ReasonCorruptSnapshot, "duplicate dataset in snapshot: "+ds.Name)
		}
		d := ds // copy
		// Default semantics for legacy data missing SpentEpsilon: derive
		// it deterministically from the recorded consumption units.
		if d.SpentEpsilon == 0 && len(d.ConsumedUnits) > 0 {
			d.SpentEpsilon = d.ConsumedUnits[len(d.ConsumedUnits)-1].SpentAfter
		}
		l.datasets[d.Name] = &d
		// Rebuild the idempotent-replay index from the audit trail.
		for _, c := range d.ConsumedUnits {
			l.processed[c.RequestID] = c
		}
	}

	// Per-dataset total of restored pending holds, for the aggregate
	// invariant spent + pending <= total.
	pendingTotal := make(map[string]float64)
	for i := range snap.Reservations {
		r := snap.Reservations[i]
		if err := validateRestoredReservation(&r, l); err != nil {
			return nil, err
		}
		if _, dup := l.reservations[r.BatchID]; dup {
			return nil, newError(ReasonCorruptSnapshot, "duplicate batch in snapshot: "+r.BatchID)
		}
		if r.Status == ReservationPending {
			pendingTotal[r.Dataset] += r.Amount
		}
		rr := r
		l.reservations[rr.BatchID] = &rr
	}
	for name, held := range pendingTotal {
		ds := l.datasets[name]
		if ds.SpentEpsilon+held > ds.TotalEpsilon {
			return nil, newError(ReasonCorruptSnapshot, fmt.Sprintf(
				"snapshot implies spent+pending %.6f > budget %.6f for dataset %q",
				ds.SpentEpsilon+held, ds.TotalEpsilon, name))
		}
	}
	return l, nil
}

// validateRestoredReservation checks a persisted reservation for internal
// consistency and restores derived guarantees. Pending holds are trusted
// against the recorded spent amount (no spending data exists to contradict
// them) and keep holding budget until their original deadline or an
// explicit reclaim.
func validateRestoredReservation(r *Reservation, l *Ledger) error {
	if r.BatchID == "" {
		return newError(ReasonCorruptSnapshot, "reservation with empty batch id in snapshot")
	}
	if r.Status == "" {
		// Deterministic safe default for partially written legacy records.
		r.Status = ReservationPending
	}
	switch r.Status {
	case ReservationPending, ReservationConfirmed, ReservationReleased, ReservationExpired:
	default:
		return newError(ReasonCorruptSnapshot, "unknown reservation status in snapshot: "+string(r.Status))
	}
	if _, ok := l.datasets[r.Dataset]; !ok {
		return newError(ReasonCorruptSnapshot,
			"reservation "+r.BatchID+" references unknown dataset: "+r.Dataset)
	}
	if r.Amount < 0 {
		return newError(ReasonCorruptSnapshot, "negative reservation amount in snapshot: "+r.BatchID)
	}
	if r.Status == ReservationConfirmed {
		if r.Actual < 0 || r.Actual > r.Amount {
			return newError(ReasonCorruptSnapshot,
				"confirmed reservation with actual outside [0,amount] in snapshot: "+r.BatchID)
		}
		// The confirmed consumption (including a zero-epsilon settlement)
		// must be part of the dataset audit trail, otherwise a replay after
		// restart could double-charge.
		if _, seen := l.processed[r.BatchID]; !seen {
			return newError(ReasonCorruptSnapshot,
				"confirmed reservation missing consumption record in snapshot: "+r.BatchID)
		}
	}
	return nil
}
