package dpengine

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

func snapshotRoundTrip(t *testing.T, l *Ledger) *Ledger {
	t.Helper()
	var buf bytes.Buffer
	if err := SaveSnapshot(&buf, l); err != nil {
		t.Fatal(err)
	}
	r, err := LoadSnapshot(&buf)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestSnapshotRestoresOpenReservation(t *testing.T) {
	base := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	l := mustLedger(t, "ds", 10.0)
	l.now = func() time.Time { return base }
	if _, err := l.Consume("ds", "r1", 2.0, SequentialComposition); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Reserve("ds", "open", 3.0, 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	// A settled batch must also round-trip as-is.
	if _, err := l.Reserve("ds", "done", 1.0, 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Confirm("done", 0.5); err != nil {
		t.Fatal(err)
	}

	r := snapshotRoundTrip(t, l)
	// Restored ledgers default to wall-clock time; pin the clock back to
	// the crash instant so the open hold (deadline base+5m) is alive.
	r.now = func() time.Time { return base }

	view, _ := r.Budget("ds")
	t.Logf("input: restart with spent=2 open=3 confirmed(done=.5) -> spent=%.1f reserved=%.1f available=%.1f",
		view.Spent, view.Reserved, view.Available)
	if !approx(view.Spent, 2.5) || !approx(view.Reserved, 3) || !approx(view.Available, 4.5) {
		t.Fatalf("budget view after restart wrong: %+v", view)
	}

	// The open hold is not lost: status, amounts and deadline survive.
	open, err := r.GetReservation("open")
	if err != nil {
		t.Fatal(err)
	}
	if open.Status != ReservationReserved || open.ReservedEps != 3 ||
		!open.ExpiresAt.Equal(base.Add(5*time.Minute)) || !open.CreatedAt.Equal(base) {
		t.Fatalf("open reservation not restored faithfully: %+v", open)
	}
	done, err := r.GetReservation("done")
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != ReservationConfirmed || done.ConfirmedEps != 0.5 {
		t.Fatalf("settled reservation not restored: %+v", done)
	}

	// The hold still blocks other budget after restart.
	if _, err := r.Reserve("ds", "intruder", 4.6, time.Minute); err == nil {
		t.Fatal("restored hold must still block competing requests")
	}
	if _, err := r.Consume("ds", "r2", 4.6, SequentialComposition); err == nil {
		t.Fatal("restored hold must still block direct consume")
	}

	// Replaying the reserve after restart holds nothing twice.
	replayed, err := r.Reserve("ds", "open", 3.0, time.Minute)
	if err != nil || replayed.Status != ReservationReserved {
		t.Fatalf("reserve replay after restart: %+v %v", replayed, err)
	}
	view, _ = r.Budget("ds")
	if !approx(view.Reserved, 3) {
		t.Fatalf("reserve replay after restart double-held: %+v", view)
	}

	// Settle the recovered hold exactly once, just as before the crash.
	settled, err := r.Confirm("open", 3.0)
	if err != nil {
		t.Fatalf("confirm recovered reservation: %v", err)
	}
	if settled.ConfirmedEps != 3 {
		t.Fatalf("confirm amount wrong: %+v", settled)
	}
	if _, err := r.Release("open"); err == nil {
		t.Fatal("second settlement after restart must be rejected")
	}
	view, _ = r.Budget("ds")
	if !approx(view.Spent, 5.5) || !approx(view.Reserved, 0) || !approx(view.Available, 4.5) {
		t.Fatalf("post-restart settle accounting wrong: %+v", view)
	}
	t.Logf("input: settle recovered open=3 -> spent=%.1f reserved=%.1f available=%.1f",
		view.Spent, view.Reserved, view.Available)

	// Audit trail survived the restart intact.
	hist := r.BatchHistory()
	kinds := []ReservationStatus{}
	for _, ev := range hist {
		kinds = append(kinds, ev.Kind)
	}
	wantKinds := []ReservationStatus{
		ReservationReserved, ReservationReserved, ReservationConfirmed,
		ReservationConfirmed,
	}
	if len(kinds) != len(wantKinds) {
		t.Fatalf("audit trail lost across restart: %+v", kinds)
	}
	for i := range kinds {
		if kinds[i] != wantKinds[i] {
			t.Fatalf("audit order mismatch at %d: got %v want %v", i, kinds, wantKinds)
		}
	}
}

func TestSnapshotOpenReservationStillExpiresAfterRestart(t *testing.T) {
	base := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	l := mustLedger(t, "ds", 10.0)
	l.now = func() time.Time { return base }
	if _, err := l.Reserve("ds", "late", 4.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	r := snapshotRoundTrip(t, l)
	// Process restarts 30s later; the 1m deadline is not due yet.
	r.now = func() time.Time { return base.Add(30 * time.Second) }
	if got, _ := r.ExpireReservations(); len(got) != 0 {
		t.Fatalf("hold reclaimed before its deadline: %+v", got)
	}
	// At the exact deadline the hold is due and is reclaimed.
	r.now = func() time.Time { return base.Add(time.Minute) }
	got, err := r.ExpireReservations()
	if err != nil || len(got) != 1 || got[0].Status != ReservationExpired {
		t.Fatalf("recovered hold must expire at its deadline: %+v err=%v", got, err)
	}
	view, _ := r.Budget("ds")
	if !approx(view.Reserved, 0) || !approx(view.Available, 10) {
		t.Fatalf("expired recovered hold wrong view: %+v", view)
	}
	t.Logf("input: restart at t+1m, hold deadline t+1m -> expired=%s available=%.1f",
		got[0].BatchID, view.Available)
}

func TestSnapshotPreservesConsumptionTrail(t *testing.T) {
	// Existing consumption and its replay index must not regress when
	// reservation data is also present.
	l := mustLedger(t, "ds", 3.0)
	if _, err := l.Consume("ds", "a", 0.5, SequentialComposition); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Reserve("ds", "b", 1.0, time.Minute); err != nil {
		t.Fatal(err)
	}
	r := snapshotRoundTrip(t, l)
	c, err := r.Consume("ds", "a", 0.5, SequentialComposition)
	if err != nil {
		t.Fatalf("pre-crash replay after restart: %v", err)
	}
	if c.SpentAfter != 0.5 {
		t.Fatalf("replay record mismatch: %+v", c)
	}
	view, _ := r.Budget("ds")
	if !approx(view.Spent, 0.5) || !approx(view.Reserved, 1) {
		t.Fatalf("consumption trail regressed: %+v", view)
	}
}

func TestLoadLegacySnapshotWithoutReservations(t *testing.T) {
	// Version 1 snapshot: no reservations fields at all. It must load
	// with zero holds and never clear existing consumption.
	data := `{"version":1,"datasets":[{"Name":"ds","TotalEpsilon":2.0,"SpentEpsilon":0.75,
		"ConsumedUnits":[
			{"RequestID":"a","Epsilon":0.5,"Composition":0,"SpentAfter":0.5},
			{"RequestID":"b","Epsilon":0.25,"Composition":0,"SpentAfter":0.75}
		]}]}`
	l, err := LoadSnapshot(strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	view, err := l.Budget("ds")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input: v1 snapshot -> spent=%.2f reserved=%.1f available=%.2f",
		view.Spent, view.Reserved, view.Available)
	if !approx(view.Spent, 0.75) || view.Reserved != 0 || !approx(view.Available, 1.25) {
		t.Fatalf("legacy default view wrong: %+v", view)
	}
	if hist := l.BatchHistory(); len(hist) != 0 {
		t.Fatalf("legacy snapshot must produce empty batch history: %+v", hist)
	}
	// Existing consumption is intact and replayable.
	if _, err := l.Consume("ds", "a", 0.5, SequentialComposition); err != nil {
		t.Fatalf("legacy replay rejected: %v", err)
	}
	// Reservation workflow works on top of migrated ledger.
	if _, err := l.Reserve("ds", "new", 1.0, time.Minute); err != nil {
		t.Fatalf("reserve after legacy load: %v", err)
	}
}

func TestLoadV0SnapshotStillWorks(t *testing.T) {
	// Unversioned legacy JSON (as produced before explicit versioning):
	// derived spent, zero holds, no error.
	data := `{"datasets": [{"Name": "ds", "TotalEpsilon": 2.0,
		"ConsumedUnits": [
			{"RequestID": "a", "Epsilon": 0.5, "Composition": 0, "SpentAfter": 0.5}
		]}]}`
	l, err := LoadSnapshot(strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	view, _ := l.Budget("ds")
	if !approx(view.Spent, 0.5) || view.Reserved != 0 {
		t.Fatalf("v0 default wrong: %+v", view)
	}
}

func TestLoadCorruptV2SnapshotsRejected(t *testing.T) {
	cases := []struct {
		name string
		data string
		want Reason
	}{
		{
			"reserved total disagrees with open holds",
			`{"version":2,"datasets":[{"Name":"ds","TotalEpsilon":10,"SpentEpsilon":0,"ReservedEpsilon":4}],
			  "reservations":[{"BatchID":"b","Dataset":"ds","ReservedEps":3,"Status":"reserved"}],
			  "batch_events":[{"BatchID":"b","Dataset":"ds","Kind":"reserved","Amount":3}]}`,
			ReasonCorruptSnapshot,
		},
		{
			"open hold exceeds budget",
			`{"version":2,"datasets":[{"Name":"ds","TotalEpsilon":2,"SpentEpsilon":1,"ReservedEpsilon":2}],
			  "reservations":[{"BatchID":"b","Dataset":"ds","ReservedEps":2,"Status":"reserved"}],
			  "batch_events":[{"BatchID":"b","Dataset":"ds","Kind":"reserved","Amount":2}]}`,
			ReasonCorruptSnapshot,
		},
		{
			"reservation references unknown dataset",
			`{"version":2,"datasets":[],
			  "reservations":[{"BatchID":"b","Dataset":"ghost","ReservedEps":1,"Status":"reserved"}],
			  "batch_events":[{"BatchID":"b","Dataset":"ghost","Kind":"reserved","Amount":1}]}`,
			ReasonCorruptSnapshot,
		},
		{
			"duplicate batch",
			`{"version":2,"datasets":[{"Name":"ds","TotalEpsilon":10,"ReservedEpsilon":2}],
			  "reservations":[
			    {"BatchID":"b","Dataset":"ds","ReservedEps":1,"Status":"reserved"},
			    {"BatchID":"b","Dataset":"ds","ReservedEps":1,"Status":"reserved"}],
			  "batch_events":[{"BatchID":"b","Dataset":"ds","Kind":"reserved","Amount":1}]}`,
			ReasonCorruptSnapshot,
		},
		{
			"unknown reservation status",
			`{"version":2,"datasets":[{"Name":"ds","TotalEpsilon":10,"ReservedEpsilon":0}],
			  "reservations":[{"BatchID":"b","Dataset":"ds","ReservedEps":1,"Status":"mystery"}],
			  "batch_events":[{"BatchID":"b","Dataset":"ds","Kind":"reserved","Amount":1}]}`,
			ReasonCorruptSnapshot,
		},
		{
			"reserve event amount disagrees",
			`{"version":2,"datasets":[{"Name":"ds","TotalEpsilon":10,"ReservedEpsilon":1}],
			  "reservations":[{"BatchID":"b","Dataset":"ds","ReservedEps":1,"Status":"reserved"}],
			  "batch_events":[{"BatchID":"b","Dataset":"ds","Kind":"reserved","Amount":2}]}`,
			ReasonCorruptSnapshot,
		},
		{
			"two terminal events for one batch",
			`{"version":2,"datasets":[{"Name":"ds","TotalEpsilon":10,"ReservedEpsilon":0}],
			  "reservations":[{"BatchID":"b","Dataset":"ds","ReservedEps":1,"ConfirmedEps":0,"Status":"released"}],
			  "batch_events":[
			    {"BatchID":"b","Dataset":"ds","Kind":"reserved","Amount":1},
			    {"BatchID":"b","Dataset":"ds","Kind":"released","Amount":1},
			    {"BatchID":"b","Dataset":"ds","Kind":"expired","Amount":1}]}`,
			ReasonCorruptSnapshot,
		},
		{
			"batch without reserve event",
			`{"version":2,"datasets":[{"Name":"ds","TotalEpsilon":10,"ReservedEpsilon":1}],
			  "reservations":[{"BatchID":"b","Dataset":"ds","ReservedEps":1,"Status":"reserved"}],
			  "batch_events":[]}`,
			ReasonCorruptSnapshot,
		},
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

func TestSnapshotRestartMultipleTimes(t *testing.T) {
	// Open reservation survives repeated save/load cycles and can still
	// only be settled once.
	l := mustLedger(t, "ds", 10.0)
	base := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	l.now = func() time.Time { return base }
	if _, err := l.Reserve("ds", "b", 2.0, time.Hour); err != nil {
		t.Fatal(err)
	}
	cur := l
	for i := 0; i < 3; i++ {
		cur = snapshotRoundTrip(t, cur)
		cur.now = func() time.Time { return base }
	}
	view, _ := cur.Budget("ds")
	if !approx(view.Reserved, 2) || !approx(view.Available, 8) {
		t.Fatalf("hold drifted across repeated restarts: %+v", view)
	}
	if _, err := cur.Confirm("b", 2.0); err != nil {
		t.Fatal(err)
	}
	if _, err := cur.Confirm("b", 2.0); err == nil {
		t.Fatal("double confirm across restarts succeeded")
	}
}
