package dpengine

import (
	"encoding/json"
	"fmt"
	"io"
)

// SnapshotVersion is the current on-disk schema version. Snapshots with a
// higher version are rejected explicitly; snapshots with a lower (or
// missing) version are loaded with deterministic default semantics.
const SnapshotVersion = 1

// Snapshot is the serializable state of a Ledger.
type Snapshot struct {
	Version  int       `json:"version"`
	Datasets []Dataset `json:"datasets"`
}

// SaveSnapshot writes the ledger state to w as versioned JSON. The
// per-request consumption records are included so idempotent replay
// survives restarts.
func SaveSnapshot(w io.Writer, l *Ledger) error {
	l.mu.Lock()
	snap := Snapshot{Version: SnapshotVersion}
	for _, ds := range l.datasets {
		snap.Datasets = append(snap.Datasets, *ds)
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
//   - Version 0 / missing fields: loaded with stable defaults — a missing
//     SpentEpsilon is recomputed deterministically from the recorded
//     consumption units, and the replay index is rebuilt from them.
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
	return l, nil
}
