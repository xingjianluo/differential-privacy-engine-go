package dpengine

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
)

// SnapshotVersion is the current on-disk schema version. Snapshots with a
// higher version are rejected explicitly; snapshots with a lower (or
// missing) version are loaded with deterministic default semantics.
//
// Version history:
//
//	1: datasets with spent budget and consumption audit trail.
//	2: additionally persists open/settled reservations and the batch
//	   lifecycle audit trail (reserve/confirm/release/expire).
const SnapshotVersion = 2

// Snapshot is the serializable state of a Ledger.
type Snapshot struct {
	Version      int           `json:"version"`
	Datasets     []Dataset     `json:"datasets"`
	Reservations []Reservation `json:"reservations,omitempty"`
	BatchEvents  []BatchEvent  `json:"batch_events,omitempty"`
}

// SaveSnapshot writes the ledger state to w as versioned JSON. The
// per-request consumption records, batch reservations and batch lifecycle
// events are included so idempotent replay and unsettled holds survive
// restarts.
func SaveSnapshot(w io.Writer, l *Ledger) error {
	l.mu.Lock()
	snap := Snapshot{Version: SnapshotVersion}
	for _, ds := range l.datasets {
		snap.Datasets = append(snap.Datasets, *ds)
	}
	batchIDs := make([]string, 0, len(l.reservations))
	for id := range l.reservations {
		batchIDs = append(batchIDs, id)
	}
	sort.Strings(batchIDs)
	for _, id := range batchIDs {
		snap.Reservations = append(snap.Reservations, *l.reservations[id])
	}
	snap.BatchEvents = append(snap.BatchEvents, l.batchEvents...)
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
//   - Version 0/1 (legacy, no reservation data): loaded with stable
//     defaults — reservations default to the empty set, ReservedEpsilon
//     defaults to zero, a missing SpentEpsilon is recomputed
//     deterministically from the recorded consumption units, and the
//     replay index is rebuilt from them. Existing consumption is never
//     cleared.
//   - Version 2: reservations and the batch audit trail are restored and
//     cross-checked against the dataset totals; internal inconsistency is
//     rejected with ReasonCorruptSnapshot.
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
		if d.ReservedEpsilon < 0 {
			return nil, newError(ReasonCorruptSnapshot,
				fmt.Sprintf("dataset %q has negative reserved epsilon", d.Name))
		}
		if d.SpentEpsilon < 0 {
			return nil, newError(ReasonCorruptSnapshot,
				fmt.Sprintf("dataset %q has negative spent epsilon", d.Name))
		}
		l.datasets[d.Name] = &d
		// Rebuild the idempotent-replay index from the audit trail.
		for _, c := range d.ConsumedUnits {
			if _, dup := l.processed[c.RequestID]; dup {
				return nil, newError(ReasonCorruptSnapshot,
					"duplicate request id in consumption trail: "+c.RequestID)
			}
			l.processed[c.RequestID] = c
		}
	}

	// Legacy snapshots (version 0/1) carry no reservation data: the empty
	// reservation set and zero holds are the deterministic safe default.
	if snap.Version < 2 {
		for _, ds := range l.datasets {
			if ds.ReservedEpsilon != 0 {
				return nil, newError(ReasonCorruptSnapshot,
					fmt.Sprintf("legacy snapshot of dataset %q carries reservation totals", ds.Name))
			}
		}
		return l, nil
	}

	// Restore reservations and rebuild reserved totals. Open reservations
	// stay reserved: their holds are not lost, cannot be double-held and
	// remain subject to the same expiry sweep after restart.
	openReserved := make(map[string]float64)
	for _, rv := range snap.Reservations {
		if rv.BatchID == "" {
			return nil, newError(ReasonCorruptSnapshot, "reservation with empty batch id")
		}
		if _, dup := l.reservations[rv.BatchID]; dup {
			return nil, newError(ReasonCorruptSnapshot, "duplicate batch in snapshot: "+rv.BatchID)
		}
		ds, ok := l.datasets[rv.Dataset]
		if !ok {
			return nil, newError(ReasonCorruptSnapshot,
				"reservation "+rv.BatchID+" references unknown dataset: "+rv.Dataset)
		}
		switch rv.Status {
		case ReservationReserved:
			if rv.ReservedEps <= 0 {
				return nil, newError(ReasonCorruptSnapshot,
					"open reservation "+rv.BatchID+" must hold positive epsilon")
			}
			if rv.ConfirmedEps != 0 {
				return nil, newError(ReasonCorruptSnapshot,
					"open reservation "+rv.BatchID+" must not carry confirmed epsilon")
			}
			openReserved[rv.Dataset] += rv.ReservedEps
		case ReservationConfirmed:
			if rv.ConfirmedEps < 0 || rv.ConfirmedEps > rv.ReservedEps+epsilonTolerance {
				return nil, newError(ReasonCorruptSnapshot,
					"confirmed reservation "+rv.BatchID+" has inconsistent amounts")
			}
		case ReservationReleased, ReservationExpired:
			// Terminal non-consuming states: nothing must stay held.
		default:
			return nil, newError(ReasonCorruptSnapshot,
				"reservation "+rv.BatchID+" has unknown status: "+string(rv.Status))
		}
		// The dataset budget itself is cross-checked in the aggregate
		// loop below; ds is intentionally not mutated per reservation.
		_ = ds
		r := rv
		l.reservations[rv.BatchID] = &r
	}

	for name, ds := range l.datasets {
		held := openReserved[name]
		if math.Abs(ds.ReservedEpsilon-held) > epsilonTolerance {
			return nil, newError(ReasonCorruptSnapshot,
				fmt.Sprintf("dataset %q reserved total %.12f disagrees with open reservations %.12f",
					name, ds.ReservedEpsilon, held))
		}
		if ds.SpentEpsilon+ds.ReservedEpsilon > ds.TotalEpsilon+epsilonTolerance {
			return nil, newError(ReasonCorruptSnapshot,
				fmt.Sprintf("dataset %q exceeds its budget: spent %.6f + reserved %.6f > total %.6f",
					name, ds.SpentEpsilon, ds.ReservedEpsilon, ds.TotalEpsilon))
		}
	}

	l.batchEvents = append(l.batchEvents, snap.BatchEvents...)
	if err := validateBatchTrail(l); err != nil {
		return nil, err
	}
	return l, nil
}

// validateBatchTrail cross-checks the restored event log against the
// reservation records: every batch starts with one reserve event, ends in
// the state recorded on its reservation, and reserved/confirmed amounts
// never disagree.
func validateBatchTrail(l *Ledger) error {
	seen := make(map[string]BatchEvent)
	for _, ev := range l.batchEvents {
		rv, ok := l.reservations[ev.BatchID]
		if !ok {
			return newError(ReasonCorruptSnapshot,
				"batch event references unknown batch: "+ev.BatchID)
		}
		first, exists := seen[ev.BatchID]
		if !exists {
			if ev.Kind != ReservationReserved {
				return newError(ReasonCorruptSnapshot,
					"batch "+ev.BatchID+" does not start with a reserve event")
			}
			if math.Abs(ev.Amount-rv.ReservedEps) > epsilonTolerance {
				return newError(ReasonCorruptSnapshot,
					"batch "+ev.BatchID+" reserve amount disagrees with reservation record")
			}
			seen[ev.BatchID] = ev
			continue
		}
		// After the reserve event only a single terminal event is legal,
		// and a batch may never carry two terminal events.
		if first.Kind != ReservationReserved {
			return newError(ReasonCorruptSnapshot,
				"batch "+ev.BatchID+" has more than one terminal event")
		}
		switch ev.Kind {
		case ReservationConfirmed, ReservationReleased, ReservationExpired:
			if rv.Status != ev.Kind {
				return newError(ReasonCorruptSnapshot,
					"batch "+ev.BatchID+" terminal event disagrees with reservation status")
			}
			if ev.Kind == ReservationConfirmed &&
				math.Abs(ev.Amount-rv.ConfirmedEps) > epsilonTolerance {
				return newError(ReasonCorruptSnapshot,
					"batch "+ev.BatchID+" confirmed amount disagrees with reservation record")
			}
			seen[ev.BatchID] = ev
		default:
			return newError(ReasonCorruptSnapshot,
				"batch "+ev.BatchID+" has non-terminal event after reserve")
		}
	}
	// Every reservation must have at least its reserve event.
	for id := range l.reservations {
		if _, ok := seen[id]; !ok {
			return newError(ReasonCorruptSnapshot, "batch without reserve event: "+id)
		}
	}
	return nil
}
